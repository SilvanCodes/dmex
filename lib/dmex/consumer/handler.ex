defmodule DMEx.Consumer.Handler do
  @moduledoc """
  What a consumer consumes, from whom, and what it does with it.

      defmodule MyApp.OrderProjection do
        use DMEx.Consumer.Handler, producer: :orders, id: :order_projection

        @impl true
        def handle_message(%MyApp.Messages.OrderPlaced{} = message, _metadata) do
          # side effects go here; raise (or let something raise) to signal failure
          :ok
        end

        def handle_message(%MyApp.Messages.OrderCancelled{} = message, _metadata), do: :ok
      end

  The module is the complete declaration of a subscription. `DMEx.Consumer`
  binds `:orders` to a URL and starts it; nothing at the call site repeats what
  is written here.

  ## The message types are read from the callback, not declared

  A subscription's message list is derived at compile time from the structs
  `c:handle_message/2` matches on — `[OrderPlaced, OrderCancelled]` above. There
  is no option for it, because there is nothing an option could say that the
  clauses do not already say.

  A separate list could only ever *describe* the clauses, and the two could
  disagree: a subscription naming a type its handler has no clause for is an
  ordinary typo, and it surfaces as a `FunctionClauseError` on a real message,
  at runtime, straight into the retry-then-`on_failure` path. With one list,
  derived, that state is unrepresentable rather than merely discouraged.

  ### What it costs to check, measured

  Confirming each matched struct really is a `DMEx.Message` needs
  `Code.ensure_compiled!/1`. A clause head does *not* leave the module loaded:
  the parallel compiler resolves a struct pattern without putting the module in
  the code server, so `c:DMEx.Message.type/0` is not callable until something
  blocks on the compilation finishing.

  That blocking does not cost a compile-time dependency. The handler→message
  edge stays an **export** dependency — `mix xref graph --label compile` shows
  no edge between them — so editing a message module recompiles its handlers
  only when its struct or exports change, exactly as the pattern match alone
  would have. Catching `%DateTime{}` in a clause head is therefore free.

  `Code.ensure_compiled!/1` rather than `Code.ensure_compiled/1`, deliberately:
  the latter answers `{:error, :unavailable}` for a module caught in a compiler
  deadlock, which is not the same fact as "this is not a message" and must not
  be reported as one.

  ### What is rejected, and why

    * **A clause whose first argument is not a struct pattern.** A bare variable
      says nothing about type, so a handler written only that way would derive
      an empty list and quietly receive nothing. As a *fallback* alongside
      struct clauses it is worse than useless: the pipeline only ever delivers
      the types these clauses declare, so nothing could reach it.
    * **A struct that is not a `DMEx.Message`.** Matching `%DateTime{}` is a
      mistake worth naming at compile time.
    * **No `c:handle_message/2` at all.** There is nothing to derive from.

  ### What it does not check

  Derivation covers **types, not values**. A clause narrowed to
  `%OrderPlaced{status: "paid"}` still declares `OrderPlaced`, so an unpaid one
  arrives and raises exactly as before. Match the struct and branch inside the
  body if you want every value handled.

  ## Options

    * `:producer` — required; the logical name of the producer to read from, as
      an atom or a string. Resolved against the `producer:` map on
      `DMEx.Consumer`, never against a URL written here.
    * `:id` — required; this subscription's own name, as an atom or a string.

  ## The subscription id is the durable identity

  `"\#{producer}/\#{id}"` — `"orders/order_projection"` for the module above —
  is what keys `dmex_cursors` and half of `dmex_dead_messages`. Both halves must
  match `~r/\\A[a-z][a-z0-9_]*\\z/`, the same charset `DMEx.Producer`'s app key
  uses, and that restriction is load-bearing: it is what makes `/` an
  unambiguous separator needing no escaping, so two distinct pairs can never
  render to one string. The cost is that an id cannot be a UUID and cannot
  contain a dot or a dash.

  Atoms and strings render identically, so `producer: :orders` and
  `producer: "orders"` are the same subscription.

  **Renaming this module is free.** Identity is the declared pair, not the
  module name, so an ordinary refactor does not reset a cursor. What *does*
  reset one is changing `id:` or the producer's key in the `DMEx.Consumer` map —
  both deliberate acts rather than something a refactoring tool does on its own.
  `DMEx.Consumer.Cursors.rename!/3` exists to migrate those.

  ## One handler, one producer

  A subscription reads exactly one producer's log, structurally: fanning in from
  two producers means two handler modules, each with its own id and its own
  cursor.

  ## A handler sees its message and the publisher's metadata — nothing else

  `c:handle_message/2` takes the decoded struct and the row's `metadata` map.
  Everything else in the log row stays with DMEx: the `id` that drives
  resumption and duplicate suppression, the `message_type` that chose this
  module, the frame's `max_id`. None of that is a property of the *message*, and
  none of it is something a consumer should have to reason about — so a handler
  is a plain function of its domain data plus whatever context the publisher
  chose to attach, testable by constructing a struct and a plain map.

  ## Succeeding is what advances the cursor

  DMEx runs the callback and the `last_seen` update in **one transaction**
  against the consumer's own storage. Returning normally commits both; raising
  rolls both back, so the message is redelivered. Application code never manages
  that bookkeeping, and there is no `:ok`/`:error` protocol to remember — the
  callback's return value is ignored, and raising is how failure is signalled.

  ## What that does and does not guarantee

  Side effects that go through the consumer's **own repo are exactly-once for
  free**, because they roll back with the same transaction that would have
  recorded them. Effects that leave that transaction — an HTTP call, a message
  to another service, a write to a different database — must be idempotent on
  something in the message or the publisher's metadata, since a crash between
  the effect landing and the transaction committing is indistinguishable from
  the message never having been processed.

  Putting external effects in a handler is the case to think twice about; a
  local write plus a separate outbox is the pattern that stays correct.

  ## Adding a clause handles that type from now on, not from the beginning

  The message list does not participate in the identity, so adding a clause for
  a new type keeps the cursor exactly where it was: the new type is handled from
  the tip onward and its history is *not* replayed. That is the cheap default
  rather than the safe one, chosen because the alternative — minting a new
  identity per message list — makes a routine additive change silently replay
  the entire log and orphan that handler's dead messages.

  When you do want the history, ask for it deliberately with
  `DMEx.Consumer.Cursors.move!/3`.
  """

  @identifier ~r/\A[a-z][a-z0-9_]*\z/
  @handler_opts [:producer, :id]

  @typedoc """
  A handler's compile-time declaration, as returned by its `__dmex__/0`.

  `:id` is the full composite identifier — `"orders/order_projection"` — not the
  `id:` half that was declared. `:messages` is derived from the callback's
  clauses, in the order they were written.
  """
  @type declaration :: %{
          handler: module(),
          producer: String.t(),
          id: String.t(),
          messages: [module(), ...]
        }

  @doc """
  Handles one message.

  Receives the decoded struct and the publisher's `metadata` map. The return
  value is ignored; raise to signal failure.

  **The first argument must be a struct pattern.** That is what declares which
  message types this subscription consumes — see the module documentation.
  """
  @callback handle_message(message :: struct(), metadata :: map()) :: any()

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour DMEx.Consumer.Handler

      # Only the two halves of the identity are given here. The message list is
      # read off the callback's own clauses, which cannot happen until they all
      # exist — hence @before_compile rather than an attribute set right here.
      @dmex_identity DMEx.Consumer.Handler.__identity__(__MODULE__, opts)

      @before_compile DMEx.Consumer.Handler
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    declaration = __declaration__(env)

    quote do
      @doc """
      This handler's resolved declaration.

      Also what makes a module recognisable as a handler, so `subscribe/2` can
      reject one that is not with a decent message rather than an
      `UndefinedFunctionError` on the first row.
      """
      @spec __dmex__() :: DMEx.Consumer.Handler.declaration()
      def __dmex__, do: unquote(Macro.escape(declaration))
    end
  end

  @doc false
  @spec __identity__(module(), keyword()) :: %{producer: String.t(), id: String.t()}
  def __identity__(module, opts) do
    validate_keys!(module, opts)

    producer = identifier!(module, opts, :producer)

    %{producer: producer, id: producer <> "/" <> identifier!(module, opts, :id)}
  end

  @doc false
  @spec __declaration__(Macro.Env.t()) :: declaration()
  def __declaration__(env) do
    %{producer: producer, id: id} = Module.get_attribute(env.module, :dmex_identity)

    %{handler: env.module, producer: producer, id: id, messages: messages!(env)}
  end

  @doc "Whether `module` was built with `use DMEx.Consumer.Handler`."
  @spec handler?(module()) :: boolean()
  def handler?(module) when is_atom(module) do
    Code.ensure_loaded?(module) and function_exported?(module, :__dmex__, 0)
  end

  @doc """
  `handler`'s durable subscription identifier.

  The one place anything turns a module into the string `dmex_cursors` and
  `dmex_dead_messages` are keyed by.
  """
  @spec subscription_id(module()) :: String.t()
  def subscription_id(handler) when is_atom(handler) do
    unless handler?(handler) do
      raise ArgumentError,
            "#{inspect(handler)} is not a DMEx handler. " <>
              "Add `use DMEx.Consumer.Handler` to it."
    end

    handler.__dmex__().id
  end

  ## Deriving the message list

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

    clauses
    |> Enum.map(&message!(env, &1))
    |> Enum.uniq()
  end

  defp message!(env, {meta, [pattern | _rest], _guards, _body}) do
    module = struct!(env, meta, pattern)

    # Not the cheaper `Code.ensure_loaded?/1`, and the difference is not
    # cosmetic: expanding `%module{}` in the clause head above resolved the
    # struct through the parallel compiler *without* putting the module in the
    # code server, so `type/0` is not callable yet. Only this blocks until the
    # module is genuinely compiled.
    #
    # It costs no dependency, measured rather than assumed — the handler→message
    # edge stays an export dependency, and `mix xref graph --label compile`
    # shows no edge between them.
    #
    # The bang matters too. `Code.ensure_compiled/1` answers
    # `{:error, :unavailable}` for a module caught in a compiler deadlock, which
    # is not the same fact as "this is not a message" and must not be reported
    # as one.
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

    A catch-all clause cannot serve as a fallback here — the pipeline only ever
    delivers the types these clauses declare, so nothing would reach it. Match
    the struct and branch inside the body instead.
    """
  end

  defp at(env, meta) do
    "#{Path.relative_to_cwd(env.file)}:#{Keyword.get(meta, :line, env.line)}"
  end

  ## Compile-time validation

  defp validate_keys!(module, opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError,
            "`use DMEx.Consumer.Handler` takes a keyword list in #{inspect(module)}, " <>
              "got: #{inspect(opts)}"
    end

    case Enum.reject(Keyword.keys(opts), &(&1 in @handler_opts)) do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "unknown option#{if length(unknown) > 1, do: "s"} " <>
                "#{inspect(unknown)} passed to `use DMEx.Consumer.Handler` in " <>
                "#{inspect(module)}. Known options: #{inspect(@handler_opts)}"
    end
  end

  # Atoms and binaries must render identically — otherwise a config style change
  # would silently mint a second identity and reset a cursor.
  defp identifier!(module, opts, key) do
    value =
      case Keyword.fetch(opts, key) do
        {:ok, value} when is_atom(value) and not is_nil(value) -> Atom.to_string(value)
        {:ok, value} when is_binary(value) -> value
        {:ok, other} -> raise_identifier(module, key, other)
        :error -> raise_missing(module, key)
      end

    unless Regex.match?(@identifier, value), do: raise_identifier(module, key, value)

    value
  end

  defp raise_missing(module, key) do
    raise ArgumentError,
          "`use DMEx.Consumer.Handler` requires #{inspect(key)} in #{inspect(module)}, e.g. " <>
            "`use DMEx.Consumer.Handler, producer: :orders, id: :order_projection`"
  end

  defp raise_identifier(module, key, value) do
    raise ArgumentError,
          "#{inspect(key)} in #{inspect(module)} must be an atom or a string matching " <>
            "#{inspect(@identifier)}, got: #{inspect(value)}. Both halves of a subscription " <>
            "identifier are restricted to that charset so \"/\" can separate them with no " <>
            "escaping — which rules out UUIDs, dots and dashes."
  end
end
