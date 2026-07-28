defmodule DMEx.Consumer.Handler do
  @moduledoc """
  What a consumer consumes, from whom, and what it does with it.

      defmodule MyApp.OrderProjection do
        use DMEx.Consumer.Handler,
          producer: :orders,
          id: :order_projection,
          messages: [MyApp.Messages.OrderPlaced, MyApp.Messages.OrderCancelled]

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

  ## Options

    * `:producer` — required; the logical name of the producer to read from, as
      an atom or a string. Resolved against the `producer:` map on
      `DMEx.Consumer`, never against a URL written here.
    * `:id` — required; this subscription's own name, as an atom or a string.
    * `:messages` — required; a non-empty list of `DMEx.Message` modules this
      handler processes.

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
  module name, so an ordinary refactor no longer resets a cursor. What *does*
  reset one is changing `id:` or the producer's key in the `DMEx.Consumer` map —
  both deliberate acts rather than something a refactoring tool does on its own.
  `DMEx.Consumer.Cursors.rename!/3` exists to migrate those.

  ## One handler, one producer

  A subscription reads exactly one producer's log, and that is now structural
  rather than incidental: fanning in from two producers means two handler
  modules, each with its own id and its own cursor.

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

  ## Adding a message type handles it from now on, not from the beginning

  `:messages` does not participate in the identity, so appending a type to the
  list keeps the cursor exactly where it was: the new type is handled from the
  tip onward and its history is *not* replayed. That is the cheap default rather
  than the safe one, chosen because the alternative — minting a new identity per
  message list — makes a routine additive change silently replay the entire log
  and orphan that handler's dead messages.

  When you do want the history, ask for it deliberately with
  `DMEx.Consumer.Cursors.move!/4`.
  """

  @identifier ~r/\A[a-z][a-z0-9_]*\z/
  @handler_opts [:producer, :id, :messages]

  @typedoc """
  A handler's compile-time declaration, as returned by its `__dmex__/0`.

  `:id` is the full composite identifier — `"orders/order_projection"` — not the
  `id:` half that was declared.
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
  """
  @callback handle_message(message :: struct(), metadata :: map()) :: any()

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour DMEx.Consumer.Handler

      # Resolved once at compile time, so nothing on the hot path rebuilds it —
      # and so a malformed declaration fails the build rather than the first
      # subscribe.
      @dmex DMEx.Consumer.Handler.__declaration__(__MODULE__, opts)

      @doc """
      This handler's resolved declaration.

      Also what makes a module recognisable as a handler, so `subscribe/2` can
      reject one that is not with a decent message rather than an
      `UndefinedFunctionError` on the first row.
      """
      @spec __dmex__() :: DMEx.Consumer.Handler.declaration()
      def __dmex__, do: @dmex
    end
  end

  @doc false
  @spec __declaration__(module(), keyword()) :: declaration()
  def __declaration__(module, opts) do
    validate_keys!(module, opts)

    producer = identifier!(module, opts, :producer)
    id = identifier!(module, opts, :id)

    %{
      handler: module,
      producer: producer,
      id: producer <> "/" <> id,
      messages: messages!(module, opts)
    }
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

  defp messages!(module, opts) do
    case Keyword.fetch(opts, :messages) do
      # Shape only. Calling `type/0` or `Code.ensure_compiled?/1` here would make
      # every handler compile-time dependent on every message module it lists, so
      # touching one struct would recompile all of them. That the modules exist
      # and are `DMEx.Message`s is checked at runtime, in
      # `DMEx.Consumer.Pipeline.build/1`.
      {:ok, [_ | _] = messages} ->
        case Enum.reject(messages, &is_atom/1) do
          [] ->
            messages

          bad ->
            raise_messages(module, "#{inspect(bad)} #{plural(bad, "is", "are")} not a module")
        end

      {:ok, []} ->
        raise_messages(module, "the list is empty")

      {:ok, other} ->
        raise_messages(module, "got: #{inspect(other)}")

      :error ->
        raise_missing(module, :messages)
    end
  end

  defp plural([_], singular, _plural), do: singular
  defp plural(_, _singular, plural), do: plural

  defp raise_missing(module, key) do
    raise ArgumentError,
          "`use DMEx.Consumer.Handler` requires #{inspect(key)} in #{inspect(module)}, e.g. " <>
            "`use DMEx.Consumer.Handler, producer: :orders, id: :order_projection, " <>
            "messages: [MyApp.Messages.OrderPlaced]`"
  end

  defp raise_identifier(module, key, value) do
    raise ArgumentError,
          "#{inspect(key)} in #{inspect(module)} must be an atom or a string matching " <>
            "#{inspect(@identifier)}, got: #{inspect(value)}. Both halves of a subscription " <>
            "identifier are restricted to that charset so \"/\" can separate them with no " <>
            "escaping — which rules out UUIDs, dots and dashes."
  end

  defp raise_messages(module, detail) do
    raise ArgumentError,
          ":messages in #{inspect(module)} must be a non-empty list of DMEx.Message " <>
            "modules — #{detail}"
  end
end
