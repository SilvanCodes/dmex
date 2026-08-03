defmodule DMEx.Handler do
  @moduledoc """
  Declares what a subscription consumes, from whom, and what it does with it.

      defmodule MyApp.OrderProjection do
        use DMEx.Handler, logs: [:orders], id: :order_projection

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
  an assertion. The heads, guards and all, are compiled into a `__dmex_match__/2`
  the pipeline asks before it opens a transaction, so a message no clause wants
  costs a struct match.

  Rejected at compile time: a clause whose first argument is not a struct
  pattern (a bare variable declares nothing, and cannot serve as a fallback
  since the pipeline only delivers the declared types); a struct that is not a
  `DMEx.Message`; and no `c:handle_message/2` at all.

  ## Options

    * `:logs` — required; the logical names of the logs to read, as atoms or
      strings, resolved against `logs:` on `DMEx.Supervisor`. A bare name is
      accepted for the single-log case.
    * `:id` — required; this handler's own name. The same for every log, since
      the log name is the other half of each subscription's identity.
    * `:selective` — whether the log narrows the stream to this handler's
      message types. Defaults to `true`. `false` requests the whole log and
      filters locally: an escape hatch for telling a log or encoding mismatch
      apart from a log that has genuinely appended nothing.
    * any `DMEx.Config` handler setting — validated here at compile time;
      `DMEx.Supervisor.subscribe/2` may override one per subscription.

  ## The subscription id is the durable identity

  `"\#{log}/\#{id}"` keys `dmex_cursors` and half of `dmex_dead_messages`. Both
  halves must match `~r/\\A[a-z][a-z0-9_]*\\z/`, which makes `/` an unambiguous
  separator needing no escaping — so an id cannot be a UUID, or contain a dot or
  a dash. Atoms and strings render identically.

  **Renaming this module is free**, since identity is the declared pair.
  Changing `id:` or the log's key in `DMEx.Supervisor` does reset a cursor;
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

  @identifier ~r/\A[a-z][a-z0-9_]*\z/
  @handler_opts [:logs, :id, :selective]

  @typedoc """
  A handler's compile-time declaration, as returned by its `__dmex__/0`.

  `:fingerprint` is the path segment this subscription requests, or `nil` when
  `selective: false` asked for the whole log. There is no separate `:selective`
  field: a `nil` fingerprint is that answer, and carrying both would be two
  values that can disagree.

  `:config` is only what this module declared, not a resolved config: resolving
  at compile time would freeze DMEx's defaults into the handler's BEAM file, and
  `DMEx.Supervisor.subscribe/2` is where a call site's overrides merge in.
  """
  @type declaration :: %{
          handler: module(),
          logs: [String.t()],
          id: String.t(),
          messages: MapSet.t(module()),
          fingerprint: String.t() | nil,
          config: keyword()
        }

  @doc """
  Handles one message.

  Receives the decoded struct and the `metadata` map — the publisher's, plus
  whichever of its own columns the *log* lifts into it with `metadata:`, which is
  the only way an id or an append time reaches a handler. See
  `DMEx.Log.merge_metadata/2`. The return value is ignored; raise to signal failure.

  **The first argument must be a struct pattern.** That is what declares which
  message types this subscription consumes — see the module documentation.
  """
  @callback handle_message(message :: struct(), metadata :: map()) :: any()

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour DMEx.Handler

      # Only what was written at the `use` site. The message set is read off the
      # callback's clauses, which cannot happen until they all exist — hence
      # @before_compile, and hence carrying these across in an attribute.
      @dmex_declared DMEx.Handler.__declared__(__MODULE__, opts)

      # The clause heads *as written*. `Module.get_definition/2` has them too,
      # but expanded and normalized past the point of being reinjectable, and
      # `__dmex_match__/2` is built by reinjecting them.
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
    match = matcher(env)

    quote do
      @doc """
      This handler's resolved declaration.
      """
      @spec __dmex__() :: DMEx.Handler.declaration()
      def __dmex__, do: unquote(Macro.escape(declaration))

      unquote(match)
    end
  end

  @doc false
  @spec __declared__(module(), keyword()) :: %{
          logs: [String.t()],
          id: String.t(),
          selective: boolean(),
          config: keyword()
        }
  def __declared__(module, opts) do
    validate_keys!(module, opts)

    %{
      logs: logs!(module, opts),
      id: fetch_identifier!(module, opts, :id),
      selective: selective!(module, opts),
      config: config!(module, opts)
    }
  end

  @doc false
  @spec __declaration__(Macro.Env.t()) :: declaration()
  def __declaration__(env) do
    %{logs: logs, id: id, selective: selective, config: config} =
      Module.get_attribute(env.module, :dmex_declared)

    messages = messages!(env)

    %{
      handler: env.module,
      logs: logs,
      id: id,
      messages: messages,
      config: config,
      # `selective:` is not carried through — a `nil` fingerprint *is* the
      # answer, and the two together could only disagree. A non-selective
      # handler therefore also never risks the compile error an unbuildable
      # fingerprint would raise.
      fingerprint: if(selective, do: fingerprint!(env, messages))
    }
  end

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
  @spec subscription_id(module(), atom() | String.t()) :: String.t()
  def subscription_id(handler, log) when is_atom(handler) do
    declaration = declaration!(handler)
    log = to_string(log)

    unless log in declaration.logs, do: raise_unknown_log(declaration, log)

    log <> "/" <> declaration.id
  end

  @doc "Every subscription `handler` declares — one per log it reads."
  @spec subscription_ids(module()) :: [String.t()]
  def subscription_ids(handler) when is_atom(handler) do
    declaration = declaration!(handler)

    Enum.map(declaration.logs, &(&1 <> "/" <> declaration.id))
  end

  @doc """
  Resolves which of `handler`'s logs a `log:` option means.

  A handler reading one log needs no option; one reading several has to be told,
  since every operator-facing call addresses exactly one cursor.
  """
  @spec log!(module(), keyword()) :: String.t()
  def log!(handler, opts) when is_atom(handler) and is_list(opts) do
    declaration = declaration!(handler)

    case {Keyword.get(opts, :log), declaration.logs} do
      {nil, [only]} ->
        only

      {nil, logs} ->
        raise ArgumentError,
              "#{inspect(handler)} reads #{length(logs)} logs (#{Enum.join(logs, ", ")}), so " <>
                "this call has to say which one it means: pass `log: :#{hd(logs)}`."

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
    unless handler?(handler) do
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
  defp fingerprint!(env, messages) do
    types = Enum.map(messages, & &1.type())

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
  defp matcher(env) do
    heads = Enum.reverse(Module.get_attribute(env.module, :dmex_heads))
    {:v1, :def, _meta, clauses} = Module.get_definition(env.module, {:handle_message, 2})

    unless length(heads) == length(clauses), do: raise_unseen_clauses(env, heads, clauses)

    accepts = Enum.map(heads, &matcher_clause/1)

    quote do
      @doc false
      @spec __dmex_match__(struct(), map()) :: boolean()
      unquote_splicing(accepts)
      def __dmex_match__(_message, _metadata), do: false
    end
  end

  defp matcher_clause({message, metadata, guards}) do
    head = quote do: __dmex_match__(unquote(message), unquote(metadata))
    head = if guards == [], do: head, else: {:when, [], [head, chain(guards)]}

    quote do
      def unquote(generated(head)), do: true
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
    unless Module.defines?(env.module, {:handle_message, 2}, :def) do
      raise ArgumentError, """
      #{inspect(env.module)} defines no handle_message/2, so there are no message types \
      to derive.

      A handler declares what it consumes by matching on it:

          def handle_message(%MyApp.Messages.OrderPlaced{} = message, _metadata), do: :ok
      """
    end

    {:v1, :def, _meta, clauses} = Module.get_definition(env.module, {:handle_message, 2})

    # Every clause is checked, including the second one matching a struct an
    # earlier clause already named — the set collapses the duplicate, but only
    # after `message!/2` has had its say about it.
    clauses
    |> Enum.map(&message!(env, &1))
    |> MapSet.new()
  end

  defp message!(env, {meta, [pattern | _rest], _guards, _body}) do
    module = struct!(env, meta, pattern)

    # Not the cheaper `Code.ensure_loaded?/1`: expanding `%module{}` in a clause
    # head resolves the struct through the parallel compiler *without* putting
    # the module in the code server, so `type/0` is not callable yet. Only this
    # blocks until the module is genuinely compiled, and it costs no dependency
    # — the handler→message edge stays an export dependency, and
    # `mix xref graph --label compile` shows no edge between them.
    #
    # The bang matters: `Code.ensure_compiled/1` answers
    # `{:error, :unavailable}` for a module caught in a compiler deadlock, which
    # is not the same fact as "this is not a message".
    Code.ensure_compiled!(module)

    unless DMEx.Message.message?(module), do: raise_not_a_message(env, meta, module)

    module
  end

  # `%Mod{}`, and `%Mod{} = message` either way round — the ways a clause names
  # its type.
  defp struct!(_env, _meta, {:%, _, [module, _fields]}) when is_atom(module), do: module

  defp struct!(env, meta, {:=, _, [left, right]}) do
    case {left, right} do
      {{:%, _, [module, _]}, _} when is_atom(module) -> module
      {_, {:%, _, [module, _]}} when is_atom(module) -> module
      _ -> raise_not_a_struct(env, meta)
    end
  end

  defp struct!(env, meta, _pattern), do: raise_not_a_struct(env, meta)

  defp raise_not_a_message(env, meta, module) do
    raise ArgumentError,
          "#{at(env, meta)}: #{inspect(env.module)} matches on #{inspect(module)}, which is " <>
            "not a DMEx message module. A handler's clauses are what declare the types its " <>
            "subscription consumes, so each one has to be a struct built with " <>
            "`use DMEx.Message`."
  end

  defp raise_not_a_struct(env, meta) do
    raise ArgumentError, """
    #{at(env, meta)}: this handle_message/2 clause does not match on a struct.

    A handler's clauses are what declare the message types its subscription
    consumes, so each one has to name its type:

        def handle_message(%MyApp.Messages.OrderPlaced{} = message, _metadata), do: :ok

    A catch-all clause cannot serve as a fallback here — it declares no type,
    and the pipeline only ever delivers declared ones. A message a narrower
    clause does not match is skipped, so there is nothing for it to catch.
    """
  end

  defp at(env, meta) do
    "#{Path.relative_to_cwd(env.file)}:#{Keyword.get(meta, :line, env.line)}"
  end

  ## Compile-time validation

  defp validate_keys!(module, opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError,
            "`use DMEx.Handler` takes a keyword list in #{inspect(module)}, " <>
              "got: #{inspect(opts)}"
    end

    known = @handler_opts ++ Config.handler_keys()

    case Enum.reject(Keyword.keys(opts), &(&1 in known)) do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "unknown option#{if length(unknown) > 1, do: "s"} " <>
                "#{inspect(unknown)} passed to `use DMEx.Handler` in " <>
                "#{inspect(module)}. Known options: #{inspect(Enum.sort(known))}"
    end
  end

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
            "A handler reads at least one: `use DMEx.Handler, logs: [:orders], id: :projection`"
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

    unless Regex.match?(@identifier, value), do: raise_identifier(module, key, value)

    value
  end

  defp raise_missing(module, key) do
    raise ArgumentError,
          "`use DMEx.Handler` requires #{inspect(key)} in #{inspect(module)}, e.g. " <>
            "`use DMEx.Handler, logs: [:orders], id: :order_projection`"
  end

  defp selective!(module, opts) do
    case Keyword.get(opts, :selective, true) do
      value when is_boolean(value) ->
        value

      other ->
        raise ArgumentError,
              ":selective in #{inspect(module)} must be true (the default — this " <>
                "subscription's message types are selected server-side) or false (the " <>
                "log's whole log, narrowed locally), got: #{inspect(other)}"
    end
  end

  defp raise_identifier(module, key, value) do
    raise ArgumentError,
          "#{inspect(key)} in #{inspect(module)} must be an atom or a string matching " <>
            "#{inspect(@identifier)}, got: #{inspect(value)}. Both halves of a subscription " <>
            "identifier are restricted to that charset so \"/\" can separate them with no " <>
            "escaping — which rules out UUIDs, dots and dashes."
  end
end
