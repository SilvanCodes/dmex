defmodule DMEx.Consumer.Handler do
  @moduledoc """
  What a consumer does with a message.

      defmodule MyApp.OrderHandler do
        use DMEx.Consumer.Handler

        @impl true
        def handle_message(%MyApp.Messages.OrderPlaced{} = message, _metadata) do
          # side effects go here; raise (or let something raise) to signal failure
          :ok
        end
      end

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

  ## The handler module is the subscription's identity

  `last_seen` is keyed by handler module, so `MyApp.OrderHandler` has exactly
  one cursor wherever it reads from. Two consequences follow, and both are worth
  knowing before they bite:

    * **One live subscription per handler.** Subscribing the same handler to two
      producers would have both sharing and clobbering one cursor, so DMEx
      rejects the second `DMEx.Consumer.subscribe/2`.

    * **Renaming a handler module resets its cursor**, and the next start
      therefore replays the producer's log from the beginning. That is a routine
      refactor with a decidedly non-routine consequence — migrate the
      `dmex_cursors` row deliberately.
  """

  @doc """
  Handles one message.

  Receives the decoded struct and the publisher's `metadata` map. The return
  value is ignored; raise to signal failure.
  """
  @callback handle_message(message :: struct(), metadata :: map()) :: any()

  defmacro __using__(opts) do
    if opts != [] do
      raise ArgumentError,
            "use DMEx.Consumer.Handler takes no options, got: #{inspect(opts)}. " <>
              "Subscription settings belong on DMEx.Consumer.subscribe/2."
    end

    quote do
      @behaviour DMEx.Consumer.Handler

      # Lets subscribe/2 reject a module that is not a handler with a decent
      # message, rather than an UndefinedFunctionError on the first row.
      @doc false
      def __dmex_handler__, do: true
    end
  end

  @doc "Whether `module` was built with `use DMEx.Consumer.Handler`."
  @spec handler?(module()) :: boolean()
  def handler?(module) when is_atom(module) do
    Code.ensure_loaded?(module) and function_exported?(module, :__dmex_handler__, 0)
  end
end
