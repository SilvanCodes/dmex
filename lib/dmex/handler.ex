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
  `DMEx.Handler.Fingerprint` blob in the request path so the log reads only
  matching rows. Inclusion is heuristic in one direction only — every type you
  match on is delivered, and a false positive admits one extra that arrives and
  is skipped — and a filter matching none of the log's types is a `404`, which
  the subscription retries rather than treating as fatal. See
  `DMEx.Handler.Fingerprint` and `DMEx.Log.Plug` for why each holds.

  Derivation covers **types, not values**: a clause narrowed to
  `%OrderPlaced{status: "paid"}` still declares `OrderPlaced`, so an unpaid one
  arrives and is skipped like any other message no clause matches.

  ## A clause that names no type asks for the whole log

  A first argument that names no type declares nothing, and a handler with one
  gets **no fingerprint**: it requests the whole log and filters locally. There
  is no option for that either — the clauses say it, the same way they say
  everything else about the message set:

      def handle_message(%MyApp.Messages.OrderPlaced{} = message, _metadata), do: :ok
      def handle_message(%module{} = message, _metadata), do: :ok

  A plain variable says the same thing and is exactly as wide: every message is
  a struct, so `%module{}` admits precisely what `_message` does, while binding
  the module a catch-all usually wants to report. It is the escape hatch for
  telling a log or encoding mismatch apart from a log that has genuinely
  appended nothing, and it costs every row the log holds.

  Rejected at compile time: a first argument matching something no message can
  be; a struct that is not a `DMEx.Message`; and no `c:handle_message/2` at
  all.

  ## Options

    * `:logs` — required; the logical names of the logs to read, as atoms or
      strings, resolved against `bindings:` on `DMEx.Supervisor`. A bare name is
      accepted for the single-log case.
    * `:name` — required; this handler's own name. The same for every log, since
      the log name is the other half of each subscription's identity.
    * any `DMEx.Config` handler setting — validated here at compile time;
      `DMEx.Supervisor.subscribe/2` may override one per subscription.

  ## The subscription id is the durable identity

  `"\#{log}/\#{id}"` keys `dmex_cursors` and half of `dmex_dead_messages`, so
  **renaming this module is free**: identity is the declared pair. Changing
  `name:` or the log's key in `DMEx.Supervisor` mints a new identity with no
  cursor — the old row survives under the old id, which is what
  `DMEx.Handler.Cursors.rename!/3` carries over and then removes. Adding a
  clause mints nothing: the new type is handled from the tip onward and its
  history is *not* replayed, so ask for it deliberately with
  `DMEx.Handler.Cursors.move!/3`.

  ## Succeeding is what advances the cursor

  The callback and the `last_seen` update run in **one transaction**: returning
  normally commits both, raising rolls both back and the message is
  redelivered. Side effects through the handler's **own repo are therefore
  exactly-once for free**; effects that leave it must be idempotent on
  something in the message or its metadata. `DMEx.Handler.Pipeline` specifies
  the rest, `on_failure` included.
  """

  alias DMEx.Config
  alias DMEx.Handler.Fingerprint

  @handler_opts [:logs, :name]

  # A log name and a handler's `name:` are the same kind of name, so the rule is
  # written once — see `identifier!/3`.
  @identifier ~r/\A[a-z][a-z0-9_]*\z/
  @charset_hint " Both halves of a subscription identifier are restricted to that charset so " <>
                  "\"/\" can separate them with no escaping — which rules out UUIDs, dots and dashes."

  @typedoc """
  A handler's compile-time declaration, as returned by its `__dmex_handler__/0`.

  `:messages` is the module set the clauses named, and the only copy of it: the
  type *strings* are read off those modules rather than snapshotted here.
  `:fingerprint` is those types encoded — or `nil`, the only statement of "whole
  log" anywhere — and is the one thing that *is* cached, because the request
  path wants a compile-time constant and an unencodable set has to be a compile
  error rather than a boot failure. `:config` is only what this module declared:
  resolving at compile time would freeze DMEx's defaults into its BEAM file.
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
  writes nothing into — no id, no `type`, nothing about the row. The return
  value is ignored; raise to signal failure.

  **The first argument declares what this subscription consumes**, and one
  naming no type asks for the whole log — see the module documentation.
  """
  @callback handle_message(message :: struct(), metadata :: map()) :: any()

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour DMEx.Handler

      # Only what was written at the `use` site: the message set cannot be read
      # off the clauses until they all exist, hence @before_compile.
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

    quote do
      unquote(compile_edges(declaration.messages))

      @doc """
      This handler's resolved declaration.
      """
      @spec __dmex_handler__() :: DMEx.Handler.declaration()
      def __dmex_handler__, do: unquote(Macro.escape(declaration))

      unquote(matcher(env, declaration))
    end
  end

  # One call per message module, emitted into the handler's own body, which is
  # what makes the handler→message edge a **compile** dependency: recompiling a
  # message recompiles every handler that baked its `type/0` in. Without it the
  # edge is `export` only — and a `type:` is a function body, which an export
  # digest does not cover, so editing one would recompile the message alone and
  # leave the fingerprint and the cast clauses naming a type nothing appends.
  #
  # Only a literal module in the handler's own AST, at the top level, records
  # that edge: `__declaration__/1` calls every `type/0` too, but through a
  # variable from inside this module, where no tracer can see it, and the same
  # call inside a `def` would track a runtime dependency instead.
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
    Config.keys!(
      opts,
      @handler_opts ++ Config.handler_keys(),
      "`use DMEx.Handler` in #{inspect(module)}",
      " A handler declares both: `use DMEx.Handler, logs: [:orders], name: :order_projection`.",
      @handler_opts
    )

    %{
      logs: logs!(module, opts),
      name: declared_identifier!(module, :name, Keyword.fetch!(opts, :name)),
      config: config!(module, opts)
    }
  end

  @doc false
  @spec __declaration__(Macro.Env.t()) :: declaration()
  def __declaration__(env) do
    declared = Module.get_attribute(env.module, :dmex_declared_opts)
    {messages, selective?} = messages!(env)

    # `selective?` is whether the clauses named a type for every case they
    # handle, so a handler asking for the whole log never risks the compile
    # error an unbuildable fingerprint would raise.
    Map.merge(declared, %{
      handler: env.module,
      messages: messages,
      fingerprint: if(selective?, do: fingerprint!(env, types(messages)))
    })
  end

  # Sorted, so the fingerprint is a function of the set and not of clause order —
  # the same reason `:messages` is a `MapSet`. Retired types are in it: a
  # subscription that cannot *select* the strings its own history was appended
  # under cannot read that history, which is the whole of what a retirement buys.
  defp types(messages), do: messages |> Enum.flat_map(&DMEx.Message.declared(&1)) |> Enum.sort()

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
  """
  @spec handler?(module()) :: boolean()
  def handler?(module) when is_atom(module) do
    Code.ensure_loaded?(module) and function_exported?(module, :__dmex_handler__, 0)
  end

  @doc """
  Builds `handler`'s durable subscription identifier for one of its logs — the
  string `dmex_cursors` and `dmex_dead_messages` are keyed by.
  """
  @spec subscription_id!(module(), atom() | String.t()) :: String.t()
  def subscription_id!(handler, log) when is_atom(handler) do
    declaration = declaration!(handler)
    log = to_string(log)

    if log in declaration.logs,
      do: id(declaration, log),
      else: raise_unknown_log(declaration, log)
  end

  @doc "Every subscription `handler` declares — one per log it reads."
  @spec subscription_ids(module()) :: [String.t()]
  def subscription_ids(handler) when is_atom(handler) do
    declaration = declaration!(handler)

    Enum.map(declaration.logs, &id(declaration, &1))
  end

  @doc """
  The subscription a `for:` option addresses, for the operator-facing modules,
  which resolve the option this one way so they cannot drift.
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
  Returns `handler`'s declaration, raising the same way for every caller if it
  has none.
  """
  @spec declaration!(module()) :: declaration()
  def declaration!(handler) when is_atom(handler) do
    if !handler?(handler) do
      raise ArgumentError,
            "#{inspect(handler)} is not a DMEx handler. Add `use DMEx.Handler` to it."
    end

    handler.__dmex_handler__()
  end

  defp id(declaration, log), do: log <> "/" <> declaration.name

  defp raise_unknown_log(declaration, log) do
    raise ArgumentError,
          "#{inspect(declaration.handler)} does not read #{inspect(log)}. It declares " <>
            "#{inspect(declaration.logs)}."
  end

  ## Encoding the selection

  # The first point at which this *can* happen: `messages!/1` has just run
  # `Code.ensure_compiled!/1` on every matched module, which is what makes
  # `type/0` callable.
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
  # pipeline has to know which *before* it opens the transaction. Calling and
  # rescuing the `FunctionClauseError` would answer a transaction too late.
  defp matcher(env, %{messages: messages} = declaration) do
    heads = Enum.reverse(Module.get_attribute(env.module, :dmex_heads))

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
      unquote_splicing(Enum.flat_map(messages, &cast_clause/1))
      unquote(last_cast_clause(declaration))

      unquote_splicing(Enum.map(heads, &matcher_clause/1))
      unquote(turn_down)
    end
  end

  # What a type no clause named falls to, and the whole difference between the
  # two kinds of handler: a selective one skips it, while a whole-log one
  # resolves it through `DMEx.Message.for_type/1` — a map built at boot from
  # compiled modules, never `String.to_atom/1` on what a log sent.
  #
  # It still asks `__dmex_match__/2`: a clause naming no type can still narrow
  # on the metadata or carry a guard, and delivering a message no head wants
  # would raise `FunctionClauseError` from the handler — a failure, retried and
  # dead-lettered, for a message nothing asked for.
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
  # log sent. A module's retired types get a clause each, to the same `cast/1`:
  # whether an older row's payload needs different handling is answered there.
  defp cast_clause(module) do
    for type <- DMEx.Message.declared(module) do
      quote do
        def __dmex_cast__(unquote(type), data, metadata) do
          message = unquote(module).cast(data)

          if __dmex_match__(message, metadata), do: message
        end
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
    Macro.prewalk(ast, fn node -> Macro.update_meta(node, &Keyword.put(&1, :generated, true)) end)
  end

  defp raise_unseen_clauses(env, seen, defined) do
    raise ArgumentError, """
    #{inspect(env.module)} defines #{defined} handle_message/2 clause(s), but \
    `use DMEx.Handler` saw #{seen} of them.

    It has to come first. The `use` is what installs the hook that reads the clause \
    patterns, and a clause defined above it would be delivered nothing at all.
    """
  end

  ## Deriving the message set

  # `Module.get_definition/2` hands back the stored clauses with their aliases
  # already resolved to modules, so this needs no `Macro.expand/2`.
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

    heads = Module.get_attribute(env.module, :dmex_heads)

    if length(heads) != length(clauses),
      do: raise_unseen_clauses(env, length(heads), length(clauses))

    # Every clause is visited, so a duplicate struct still gets `message!/2`'s
    # say before the set collapses it. `:any` is a clause that named no type,
    # and one of those is enough to make the subscription whole-log.
    Enum.reduce(clauses, {MapSet.new(), true}, fn clause, {messages, selective?} ->
      case message!(env, clause) do
        {:type, module} -> {MapSet.put(messages, module), selective?}
        :any -> {messages, false}
      end
    end)
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
    # blocks until the module is genuinely compiled. The bang matters:
    # `Code.ensure_compiled/1` answers `{:error, :unavailable}` for a module
    # caught in a compiler deadlock, which is not the same fact as "this is not
    # a message".
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

  # The two patterns that name no type: equally wide, since every message is a
  # struct.
  defp unnamed?({:%, _, [{name, _, ctx}, _fields]}) when is_atom(name) and is_atom(ctx), do: true
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

    or names none — `%module{}` or a plain variable, the same width since every
    message is a struct — which asks for the whole log.
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

    _ = Config.handler!(declared, "`use DMEx.Handler` in #{inspect(module)}")

    declared
  end

  # One name or several, in declaration order.
  defp logs!(module, opts) do
    logs =
      case Keyword.fetch!(opts, :logs) do
        [] -> raise_no_logs(module)
        logs when is_list(logs) -> Enum.map(logs, &declared_identifier!(module, :logs, &1))
        one -> [declared_identifier!(module, :logs, one)]
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

  defp declared_identifier!(module, key, value) do
    identifier!(value, "#{inspect(key)} in #{inspect(module)}", @charset_hint)
  end

  # Checks one half of a subscription identifier and returns it as a string;
  # `DMEx.Supervisor.Bindings` checks a log name against this same function, so
  # the two halves cannot be restricted differently.
  #
  # Atoms and binaries must render identically — otherwise a config style change
  # would silently mint a second identity and reset a cursor.
  @doc false
  @spec identifier!(term(), String.t(), String.t()) :: String.t()
  def identifier!(value, source, hint \\ "") do
    name = if is_atom(value) and not is_nil(value), do: Atom.to_string(value), else: value

    if !(is_binary(name) and Regex.match?(@identifier, name)) do
      raise ArgumentError,
            "#{source} must be an atom or a string matching #{inspect(@identifier)}, " <>
              "got: #{inspect(name)}." <> hint
    end

    name
  end
end
