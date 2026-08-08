defmodule DMEx.Handler do
  @moduledoc """
  Declares what a subscription consumes, from whom, and what it does with it.

      defmodule MyApp.OrderProjection do
        use DMEx.Handler, logs: [:orders], name: :order_projection

        @impl true
        def handle_message(%MyApp.Messages.OrderPlaced{} = message, _metadata) do
          # side effects go here; raise (or let something raise) to signal failure
          :ok
        end

        def handle_message(%MyApp.Messages.OrderCancelled{} = message, _metadata), do: :ok
      end

  The module is the complete declaration: `DMEx.Supervisor` binds `:orders` to a
  URL and starts it, and nothing at that call site repeats what is written here.
  Each entry in `logs:` is its own subscription — its own connection, cursor and
  dead messages — all running the same clauses. 
  ## The message types are read from the callback

  A subscription's message set is derived at compile time from the structs
  `c:handle_message/2` matches on. There is no option for it: a separate list
  could only *disagree* with the clauses, silently, in either direction.

  That set is also the **server-side selection**, encoded as a compile-time
  `DMEx.Handler.Fingerprint` blob in the request path so the log reads only matching
  rows. Two consequences:

    * **Inclusion is heuristic in one direction only.** No false negatives, so
      every type you match on is delivered. A false positive admits one extra
      type, which arrives and is skipped like any unknown type.
    * **A filter matching none of the log's types is a `404`**, which the
      subscription retries rather than treating as fatal: a handler deployed
      ahead of the log that will emit its types is legitimately in that state.

  Derivation covers **types, not values**: a clause narrowed to
  `%OrderPlaced{status: "paid"}` still declares `OrderPlaced`, so an unpaid one
  arrives — and is skipped like any other message no clause matches, advancing
  the cursor and emitting `[:dmex, :handler, :skip]`. Narrowing is a filter, not
  an assertion. The heads, guards and all, are compiled into a `__dmex_cast__/3`
  that decodes a row and returns it only if a clause accepts it — `nil` covering
  both a type nothing declares and a value nothing wants, since they are the
  same answer. The pipeline asks it before it opens a transaction, so a row no
  clause wants costs a struct match.

  ## A clause that names no type asks for the whole log

  A first argument that names no type declares nothing, and a handler with one
  gets **no fingerprint**: it requests the whole log and filters locally. There
  is no option for that either — the clauses say it, the same way they say
  everything else about the message set:

      def handle_message(%MyApp.Messages.OrderPlaced{} = message, _metadata), do: :ok
      def handle_message(%module{} = message, _metadata), do: :ok

  A plain variable says the same thing and is exactly as wide: every message is
  a struct, so `%module{}` admits precisely what `_message` does, while binding
  the module a catch-all usually wants to report.

  It is the escape hatch for telling a log or encoding mismatch apart from a log
  that has genuinely appended nothing, and it costs every row the log holds
  rather than the ones this handler wants.

  It also widens what can be **decoded**. A type no clause named still resolves
  to a module, through every message module the node knows
  (`DMEx.Message.for_type/1`), so a catch-all receives everything this node can
  turn back into a struct. A type *no* module declares is still skipped —
  there is nothing to decode it into — which is what makes such a handler a
  usable diagnostic: every row arrives, and `[:dmex, :handler, :skip]` names the
  types nothing here understands.

  This is the one place a subscription's set is not exactly its clauses. It is
  sound because the selection is already the whole log: what the application
  happens to know changes what is *handled*, never what is delivered, and a
  module it does not have was being skipped anyway.

  Rejected at compile time: a first argument matching something no message can
  be; a struct that is not a `DMEx.Message`; and no `c:handle_message/2` at
  all.

  ## Options

    * `:logs` — required; the logical names of the logs to read, as atoms or
      strings, resolved against `logs:` on `DMEx.Supervisor`. A bare name is
      accepted for the single-log case.
    * `:name` — required; this handler's own name. The same for every log, since
      the log name is the other half of each subscription's identity.
    * any `DMEx.Config` handler setting — validated here at compile time;
      `DMEx.Supervisor.subscribe/2` may override one per subscription.

  ## The subscription id is the durable identity

  `"\#{log}/\#{id}"` keys `dmex_cursors` and half of `dmex_dead_messages`. Both
  halves must match `~r/\\A[a-z][a-z0-9_]*\\z/`, which makes `/` an unambiguous
  separator needing no escaping — so an id cannot be a UUID, or contain a dot or
  a dash. Atoms and strings render identically.

  **Renaming this module is free**, since identity is the declared pair.
  Changing `name:` or the log's key in `DMEx.Supervisor` does reset a cursor;
  `DMEx.Handler.Cursors.rename!/3` migrates those. Adding a clause does not: the
  new type is handled from the tip onward and its history is *not* replayed, so
  ask for it deliberately with `DMEx.Handler.Cursors.move!/3`.

  ## Succeeding is what advances the cursor

  DMEx runs the callback and the `last_seen` update in **one transaction**.
  Returning normally commits both; raising rolls both back and the message is
  redelivered. The return value is ignored: raising is how failure is signalled.

  Side effects through the handler's **own repo are exactly-once for free**,
  because they roll back with the transaction that would have recorded them.
  Effects that leave it must be idempotent on something in the message or its
  metadata, since a crash between the effect landing and the commit is
  indistinguishable from never having processed the message.
  """

  alias DMEx.Config
  alias DMEx.Handler.Fingerprint

  @handler_opts [:logs, :name]

  @typedoc """
  A handler's compile-time declaration, as returned by its `__dmex__/0`.

  `:messages` is the module set the clauses named, and the only copy of it: the
  type *strings* are read off those modules — at compile time to build the
  fingerprint and the `__dmex_cast__/3` clauses, and live wherever else they are
  wanted. Caching them here as well would be a snapshot to keep in step with the
  modules for no reader's benefit.

  `:fingerprint` is the path segment this subscription requests — those types
  encoded — or `nil` when a clause named no type, which asks for the whole log.
  It is the one thing that *is* cached, because the request path wants a
  compile-time constant and an unencodable set has to be a compile error rather
  than a boot failure. `compile_edges/1` is what keeps it in step with the
  modules it was built from. A `nil` fingerprint is also the only statement of
  "whole log" anywhere: a second field could only disagree with it.

  `:config` is only what this module declared, not a resolved config: resolving
  at compile time would freeze DMEx's defaults into the handler's BEAM file, and
  `DMEx.Supervisor.subscribe/2` is where a call site's overrides merge in.
  """
  @type declaration :: %{
          handler: module(),
          logs: [String.t()],
          name: String.t(),
          messages: MapSet.t(module()),
          fingerprint: String.t() | nil,
          config: keyword()
        }

  @doc """
  Handles one message.

  Receives the decoded struct and the publisher's `metadata` map, which DMEx
  writes nothing into — no id, no `type`, nothing about the row. The
  return value is ignored; raise to signal failure.

  **The first argument declares what this subscription consumes.** A named
  struct adds its type to the set; one that names none — `%module{}` or a plain
  variable — asks for the whole log, and receives every type this application
  has a module for. See the module documentation.
  """
  @callback handle_message(message :: struct(), metadata :: map()) :: any()

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour DMEx.Handler

      # Only what was written at the `use` site. The message set is read off the
      # callback's clauses, which cannot happen until they all exist — hence
      # @before_compile, and hence carrying these across in an attribute.
      @dmex_declared_opts DMEx.Handler.__declared_opts__(__MODULE__, opts)

      # The clause heads *as written*. `Module.get_definition/2` has them too,
      # but expanded and normalized past the point of being reinjectable, and
      # `__dmex_cast__/3`'s match test is built by reinjecting them.
      Module.register_attribute(__MODULE__, :dmex_heads, accumulate: true)
      @on_definition DMEx.Handler

      @before_compile DMEx.Handler
    end
  end

  @doc false
  def __on_definition__(env, :def, :handle_message, [message, metadata], guards, _body) do
    Module.put_attribute(env.module, :dmex_heads, {message, metadata, guards})
  end

  def __on_definition__(_env, _kind, _name, _args, _guards, _body), do: :ok

  @doc false
  defmacro __before_compile__(env) do
    declaration = __declaration__(env)
    match = matcher(env, declaration)

    quote do
      unquote(compile_edges(declaration.messages))

      @doc """
      This handler's resolved declaration.
      """
      @spec __dmex__() :: DMEx.Handler.declaration()
      def __dmex__, do: unquote(Macro.escape(declaration))

      unquote(match)
    end
  end

  # One call per message module, emitted into the handler's own body, which is
  # what makes the handler→message edge a **compile** dependency: recompiling a
  # message recompiles every handler that baked its `type/0` in. Without it the
  # edge is `export` only — and a `type:` is a function body, which an export
  # digest does not cover, so editing one recompiled the message alone and left
  # the fingerprint and the cast clauses naming a type nothing appends.
  #
  # `__declaration__/1` already calls every `type/0`, and cannot do this job: it
  # calls through a variable, from inside this module's compiled code, where no
  # tracer can see it. Only a literal module in the handler's own AST records an
  # edge, and only at the top level — the same call inside a `def` would be a
  # runtime dependency and track nothing.
  #
  # Evaluated at compile time and discarded, so it costs one call per message per
  # compile and leaves nothing in the BEAM. What it does cost is recompilation:
  # a message module's consumers are now rebuilt with it, which is the honest
  # price of them containing a copy of its type.
  defp compile_edges(messages) do
    for module <- messages do
      quote do
        _ = unquote(module).type()
      end
    end
  end

  @doc false
  @spec __declared_opts__(module(), keyword()) :: %{
          logs: [String.t()],
          name: String.t(),
          config: keyword()
        }
  def __declared_opts__(module, opts) do
    DMEx.validate_keys!(
      opts,
      @handler_opts ++ Config.handler_keys(),
      "`use DMEx.Handler` in #{inspect(module)}"
    )

    %{
      logs: logs!(module, opts),
      name: fetch_identifier!(module, opts, :name),
      config: config!(module, opts)
    }
  end

  @doc false
  @spec __declaration__(Macro.Env.t()) :: declaration()
  def __declaration__(env) do
    %{logs: logs, name: name, config: config} = Module.get_attribute(env.module, :dmex_declared_opts)

    {messages, selective?} = messages!(env)

    %{
      handler: env.module,
      logs: logs,
      name: name,
      messages: messages,
      config: config,
      # Whether the clauses named a type for every case they handle, which is the
      # whole of the question — so a handler asking for the whole log also never
      # risks the compile error an unbuildable fingerprint would raise.
      fingerprint: if(selective?, do: fingerprint!(env, types(messages)))
    }
  end

  # Sorted, so the fingerprint is a function of the set and not of clause order —
  # which is the same reason `:messages` is a `MapSet`.
  defp types(messages), do: messages |> Enum.map(& &1.type()) |> Enum.sort()

  @doc """
  Lists every handler compiled into the applications `DMEx.discovery/1` scans.

  What `DMEx.Supervisor` starts, so adding a handler module is the whole of
  adding a subscription. Sorted, so boot order is reproducible.
  """
  @spec discover(atom() | nil) :: [module()]
  def discover(otp_app \\ nil) do
    otp_app
    |> DMEx.discovery()
    |> Enum.filter(&handler?/1)
    |> Enum.sort()
  end

  @doc """
  Returns whether `module` was built with `use DMEx.Handler`.

  `handle_message/2` is half the test because a log module answers `__dmex__/0`
  too, and discovery scans the same applications both live in.
  """
  @spec handler?(module()) :: boolean()
  def handler?(module) when is_atom(module) do
    Code.ensure_loaded?(module) and function_exported?(module, :__dmex__, 0) and
      function_exported?(module, :handle_message, 2)
  end

  @doc """
  Builds `handler`'s durable subscription identifier for one of its logs.

  The one place a declaration becomes the string `dmex_cursors` and
  `dmex_dead_messages` are keyed by.
  """
  @spec subscription_id!(module(), atom() | String.t()) :: String.t()
  def subscription_id!(handler, log) when is_atom(handler) do
    declaration = declaration!(handler)
    log = to_string(log)

    if log not in declaration.logs, do: raise_unknown_log(declaration, log)

    log <> "/" <> declaration.name
  end

  @doc "Every subscription `handler` declares — one per log it reads."
  @spec subscription_ids(module()) :: [String.t()]
  def subscription_ids(handler) when is_atom(handler) do
    handler
    |> declaration!()
    |> Map.fetch!(:logs)
    |> Enum.map(&subscription_id!(handler, &1))
  end

  @doc """
  The subscription a `for:` option addresses, for the operator-facing modules.

  `DMEx.Handler.Cursors` and `DMEx.Handler.DeadMessages` resolve the same option
  the same way, so they cannot drift.
  """
  @spec resolve_subscription_id!(module(), keyword()) :: String.t()
  def resolve_subscription_id!(handler, opts) when is_atom(handler) and is_list(opts) do
    subscription_id!(handler, log!(handler, opts))
  end

  @doc """
  Resolves which of `handler`'s logs a `for:` option means.

  A handler reading one log needs no option; one reading several has to be told,
  since every operator-facing call addresses exactly one cursor.
  """
  @spec log!(module(), keyword()) :: String.t()
  def log!(handler, opts) when is_atom(handler) and is_list(opts) do
    declaration = declaration!(handler)

    case {Keyword.get(opts, :for), declaration.logs} do
      {nil, [only]} ->
        only

      {nil, logs} ->
        raise ArgumentError,
              "#{inspect(handler)} reads #{length(logs)} logs (#{Enum.join(logs, ", ")}), so " <>
                "this call has to say which one it means: pass `for: :#{hd(logs)}`."

      {log, _logs} ->
        log = to_string(log)

        if log in declaration.logs, do: log, else: raise_unknown_log(declaration, log)
    end
  end

  @doc """
  Returns `handler`'s declaration, or raises if it has none.

  The one place a module becomes its declaration, so every caller rejects a
  non-handler with the same message.
  """
  @spec declaration!(module()) :: declaration()
  def declaration!(handler) when is_atom(handler) do
    if !handler?(handler) do
      raise ArgumentError,
            "#{inspect(handler)} is not a DMEx handler. Add `use DMEx.Handler` to it."
    end

    handler.__dmex__()
  end

  defp raise_unknown_log(declaration, log) do
    raise ArgumentError,
          "#{inspect(declaration.handler)} does not read #{inspect(log)}. It declares " <>
            "#{inspect(declaration.logs)}."
  end

  ## Encoding the selection

  # The first point at which this *can* happen — `messages!/1` has just run
  # `Code.ensure_compiled!/1` on every matched module, which is what makes
  # `type/0` callable — and the right one, since the result is then a constant
  # compiled into `__dmex__/0` and an unbuildable fingerprint is a compile error
  # rather than a runtime fallback path.
  defp fingerprint!(env, types) do
    case Fingerprint.build(types) do
      {:ok, fingerprint} ->
        Fingerprint.encode(fingerprint)

      {:error, reason} ->
        raise ArgumentError,
              "#{inspect(env.module)} could not encode its #{length(types)} message " <>
                "type(s) into a selection fingerprint (#{inspect(reason)}). This is a DMEx " <>
                "bug rather than a mistake in this module — please report it with the " <>
                "list: #{inspect(types)}."
    end
  end

  ## Asking whether a clause wants a message

  # Selection is by type, so a clause narrowed below its type —
  # `%OrderPlaced{status: "paid"}` — is sent messages no clause accepts, and the
  # pipeline has to know which *before* it opens the transaction the handler
  # would have run in. Calling and rescuing the `FunctionClauseError` answers a
  # transaction too late; this answers from the same heads, on a function whose
  # bodies are `true`.
  #
  # It is one function because it is one question. A row a consumer has no
  # module for and a row no clause head accepts are the same answer — `nil` —
  # and a caller cannot decode without asking, or ask without decoding.
  defp matcher(env, %{messages: messages} = declaration) do
    heads = Enum.reverse(Module.get_attribute(env.module, :dmex_heads))
    {:v1, :def, _meta, clauses} = Module.get_definition(env.module, {:handle_message, 2})

    if length(heads) != length(clauses), do: raise_unseen_clauses(env, heads, clauses)

    accepts = Enum.map(heads, &matcher_clause/1)
    casts = Enum.map(messages, &cast_clause/1)

    # What turns a narrowed-away value down, including on a whole-log handler:
    # a clause naming no type still narrows if it patterns on the metadata or
    # carries a guard, and without this the test would raise rather than answer.
    # Marked generated because a handler narrowing nothing has heads covering
    # every struct its own cast can return, so this reads as unreachable to
    # anything checking — in a user's dialyzer run, say, where every call site is
    # now visible.
    turn_down =
      quote generated: true do
        defp __dmex_match__(_message, _metadata), do: false
      end

    quote do
      @doc false
      @spec __dmex_cast__(String.t(), term(), map()) :: struct() | nil
      unquote_splicing(casts)
      unquote(last_cast_clause(declaration))

      unquote_splicing(accepts)
      unquote(turn_down)
    end
  end

  # What a type no clause named falls to, and the whole difference between the
  # two kinds of handler.
  #
  # A selective one requested its types and nothing else, so anything else is a
  # filter false positive: `nil`, and the pipeline skips it.
  #
  # A whole-log one asked for every row, so a type it never mentioned still
  # resolves, through the node's index of message modules. Not
  # `String.to_atom/1` on what a log sent: `DMEx.Message.for_type/1` is a lookup in
  # a map built at boot from compiled modules, so a type nothing declares stays a
  # skip rather than an atom.
  #
  # It still asks `__dmex_match__/2`. Naming no type is a property of the *first
  # argument*, where accepting a message is the whole head — a clause naming no
  # type can still narrow on the metadata or carry a guard, and delivering a
  # message no head wants would raise `FunctionClauseError` from the handler:
  # a failure, retried and dead-lettered, for a message nothing asked for.
  defp last_cast_clause(%{fingerprint: nil}) do
    quote do
      def __dmex_cast__(type, data, metadata) do
        case DMEx.Message.for_type(type) do
          nil ->
            nil

          module ->
            message = module.cast(data)

            if __dmex_match__(message, metadata), do: message
        end
      end
    end
  end

  defp last_cast_clause(_declaration) do
    quote do
      def __dmex_cast__(_type, _data, _metadata), do: nil
    end
  end

  # One clause per declared type, compiled from the set rather than looked up in
  # a map built from it at boot — and never `String.to_atom/1` on whatever the
  # log sent. A type with no clause falls to the `nil` above without decoding.
  defp cast_clause(module) do
    quote do
      def __dmex_cast__(unquote(module.type()), data, metadata) do
        message = unquote(module).cast(data)

        if __dmex_match__(message, metadata), do: message
      end
    end
  end

  defp matcher_clause({message, metadata, guards}) do
    head = quote do: __dmex_match__(unquote(message), unquote(metadata))
    head = if guards == [], do: head, else: {:when, [], [head, chain(guards)]}

    quote do
      defp unquote(generated(head)), do: true
    end
  end

  # `def f(x) when a when b` — alternative guard sequences, which the hook hands
  # over flat and a clause head wants nested.
  defp chain([guard]), do: guard
  defp chain([guard | rest]), do: {:when, [], [guard, chain(rest)]}

  # These are the user's own patterns moved to a function they did not write, so
  # a binding this copy has no body to use is not their mistake to be warned
  # about.
  defp generated(ast) do
    Macro.prewalk(ast, fn node -> Macro.update_meta(node, &mark/1) end)
  end

  defp mark(meta), do: Keyword.put(meta, :generated, true)

  defp raise_unseen_clauses(env, heads, clauses) do
    raise ArgumentError, """
    #{inspect(env.module)} defines #{length(clauses)} handle_message/2 clause(s), but \
    `use DMEx.Handler` saw #{length(heads)} of them.

    It has to come first. The `use` is what installs the hook that reads the clause \
    patterns, and a clause defined above it would be delivered nothing at all.
    """
  end

  ## Deriving the message set

  # `Module.get_definition/2` hands back the stored clauses with their aliases
  # already resolved to modules, so this needs no `Macro.expand/2` and no
  # `@on_definition` hook shadowing every definition on its way past.
  defp messages!(env) do
    if !Module.defines?(env.module, {:handle_message, 2}, :def) do
      raise ArgumentError, """
      #{inspect(env.module)} defines no handle_message/2, so there are no message types \
      to derive.

      A handler declares what it consumes by matching on it:

          def handle_message(%MyApp.Messages.OrderPlaced{} = message, _metadata), do: :ok
      """
    end

    {:v1, :def, _meta, clauses} = Module.get_definition(env.module, {:handle_message, 2})

    # Every clause is visited, including the second one matching a struct an
    # earlier clause already named — the set collapses the duplicate, but only
    # after `message!/2` has had its say about it. `:any` is a clause that named
    # no type, and one of those is enough to make the subscription whole-log:
    # there is no fingerprint that could admit what it is willing to accept.
    {messages, selective?} =
      Enum.reduce(clauses, {MapSet.new(), true}, fn clause, {messages, selective?} ->
        case message!(env, clause) do
          {:type, module} -> {MapSet.put(messages, module), selective?}
          :any -> {messages, false}
        end
      end)

    {messages, selective?}
  end

  defp message!(env, {meta, [pattern | _rest], _guards, _body}) do
    with module when module != :any <- struct!(env, meta, pattern) do
      {:type, message_module!(env, meta, module)}
    end
  end

  defp message_module!(env, meta, module) do
    # Not the cheaper `Code.ensure_loaded?/1`: expanding `%module{}` in a clause
    # head resolves the struct through the parallel compiler *without* putting
    # the module in the code server, so `type/0` is not callable yet. Only this
    # blocks until the module is genuinely compiled. It records no dependency of
    # its own — the compile edge is `compile_edges/1`'s doing, so what creates it
    # is one visible line rather than a side effect of a readiness check.
    #
    # The bang matters: `Code.ensure_compiled/1` answers
    # `{:error, :unavailable}` for a module caught in a compiler deadlock, which
    # is not the same fact as "this is not a message".
    Code.ensure_compiled!(module)

    if !DMEx.Message.message?(module), do: raise_not_a_message(env, meta, module)

    module
  end

  # `%Mod{}`, and `%Mod{} = message` either way round — the ways a clause names
  # its type.
  defp struct!(_env, _meta, {:%, _, [module, _fields]}) when is_atom(module), do: module

  defp struct!(env, meta, {:=, _, [left, right]}) do
    case {left, right} do
      {{:%, _, [module, _]}, _} when is_atom(module) -> module
      {_, {:%, _, [module, _]}} when is_atom(module) -> module
      # Both halves, so `%module{} = message` is the whole log while `x = 5` —
      # which no message can match — is still the mistake it was.
      _ -> if unnamed?(left) and unnamed?(right), do: :any, else: raise_bad_pattern(env, meta)
    end
  end

  defp struct!(env, meta, pattern) do
    if unnamed?(pattern), do: :any, else: raise_bad_pattern(env, meta)
  end

  # The patterns that name no type, which is how a handler asks for the whole
  # log. Two spellings, equally wide: every message is a struct, so `%module{}`
  # admits exactly what a plain variable does — while binding the module a
  # catch-all usually wants to report.
  defp unnamed?({:%, _, [{name, _, context}, _fields]}) when is_atom(name) and is_atom(context) do
    true
  end

  defp unnamed?({name, _meta, context}) when is_atom(name) and is_atom(context), do: true
  defp unnamed?(_pattern), do: false

  defp raise_not_a_message(env, meta, module) do
    raise ArgumentError,
          "#{at(env, meta)}: #{inspect(env.module)} matches on #{inspect(module)}, which is " <>
            "not a DMEx message module. A handler's clauses are what declare the types its " <>
            "subscription consumes, so each one has to be a struct built with " <>
            "`use DMEx.Message`."
  end

  defp raise_bad_pattern(env, meta) do
    raise ArgumentError, """
    #{at(env, meta)}: this handle_message/2 clause matches something no message can be.

    A handler's clauses are what declare the message types its subscription
    consumes, so a clause either names its type:

        def handle_message(%MyApp.Messages.OrderPlaced{} = message, _metadata), do: :ok

    or names none, which asks for the whole log — `%module{}` and a plain
    variable being the same width, since every message is a struct:

        def handle_message(%module{} = message, _metadata), do: :ok
        def handle_message(_message, _metadata), do: :ok

    What arrives is always a struct built by `DMEx.Message.cast/1`, so no other
    pattern could match one.
    """
  end

  defp at(env, meta) do
    "#{Path.relative_to_cwd(env.file)}:#{Keyword.get(meta, :line, env.line)}"
  end

  ## Compile-time validation

  # Kept as the declared keyword rather than the resolved map, so DMEx's
  # defaults are not compiled into every handler — but resolved once here and
  # discarded, so a bad value is a compile error rather than a boot failure.
  defp config!(module, opts) do
    declared = Keyword.take(opts, Config.handler_keys())

    try do
      _ = Config.handler!(declared)
    rescue
      error in ArgumentError ->
        reraise ArgumentError,
                [message: "in `use DMEx.Handler` in #{inspect(module)}: #{error.message}"],
                __STACKTRACE__
    end

    declared
  end

  # One name or several, in declaration order.
  defp logs!(module, opts) do
    logs =
      case Keyword.fetch(opts, :logs) do
        {:ok, []} -> raise_no_logs(module)
        {:ok, logs} when is_list(logs) -> Enum.map(logs, &identifier!(module, :logs, &1))
        {:ok, one} -> [identifier!(module, :logs, one)]
        :error -> raise_missing(module, :logs)
      end

    case logs -- Enum.uniq(logs) do
      [] ->
        logs

      [repeated | _] ->
        raise ArgumentError,
              "#{inspect(module)} names #{inspect(repeated)} twice in `logs:`. One log is one " <>
                "subscription, so a repeat could only ever mean one of them."
    end
  end

  defp raise_no_logs(module) do
    raise ArgumentError,
          "`logs:` in #{inspect(module)} names no log, so there is no subscription to start. " <>
            "A handler reads at least one: `use DMEx.Handler, logs: [:orders], name: :projection`"
  end

  # Atoms and binaries must render identically — otherwise a config style change
  # would silently mint a second identity and reset a cursor.
  defp fetch_identifier!(module, opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} -> identifier!(module, key, value)
      :error -> raise_missing(module, key)
    end
  end

  defp identifier!(module, key, value) do
    value =
      case value do
        value when is_atom(value) and not is_nil(value) -> Atom.to_string(value)
        value when is_binary(value) -> value
        other -> raise_identifier(module, key, other)
      end

    if !DMEx.identifier?(value), do: raise_identifier(module, key, value)

    value
  end

  defp raise_missing(module, key) do
    raise ArgumentError,
          "`use DMEx.Handler` requires #{inspect(key)} in #{inspect(module)}, e.g. " <>
            "`use DMEx.Handler, logs: [:orders], name: :order_projection`"
  end

  defp raise_identifier(module, key, value) do
    raise ArgumentError,
          "#{inspect(key)} in #{inspect(module)} must be an atom or a string matching " <>
            "#{inspect(DMEx.identifier_format())}, got: #{inspect(value)}. Both halves of a subscription " <>
            "identifier are restricted to that charset so \"/\" can separate them with no " <>
            "escaping — which rules out UUIDs, dots and dashes."
  end
end
