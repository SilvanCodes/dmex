defmodule DMEx.Consumer.Pipeline do
  @moduledoc """
  Processes one row: suppress, resolve, decode, handle, advance.

  Every consumer-side semantic lives here, and the order of operations *is* the
  specification. It is deliberately separate from `DMEx.Consumer.Stream` so it
  can be tested against a real database with no socket in sight.

  ## 1. Duplicate suppression, by id

  A row whose `id` is at or below the cursor was already committed, so it is
  dropped before any transaction opens. This needs no dedupe table and no new
  state: delivery within a subscription is strictly ordered and `last_seen` is
  durable, so "already processed" is exactly "id not greater than the cursor".

  It makes transport-level re-delivery harmless — a reconnect overlap, a
  replayed frame, a generic client resuming via `Last-Event-ID`. It cannot cover
  a crash between a handler's side effects landing and its transaction
  committing, because it is that very commit that records the message as
  processed.

  ## 2. An unknown message type is skipped, not an error

  This is the direct consequence of v1 having no server-side selection: a
  subscription receives the producer's entire log, so rows the consumer has no
  module for are the normal case, not a fault.

  **Such a row advances the cursor exactly as a handled one does.** It must, or
  a consumer would wedge forever on the first message type it does not know.
  Skips get a telemetry event, because a consumer silently discarding most of a
  stream and a consumer silently discarding the one type it wanted look
  identical otherwise.

  ## 3. Decoding happens outside the transaction

  `c:DMEx.Message.cast/1` is CPU work, not IO. Holding a database connection
  across it would inflate transaction time — and therefore lock time — for
  nothing.

  ## 4. Failures are retried a bounded number of times, then resolved

  When a handler raises, the transaction rolls back and the message is still in
  hand — no refetch is needed, so retrying is simply calling it again. That is
  worth a few attempts: a deadlock, a blipped downstream, or a momentarily
  unavailable connection are all transient, and reconnecting the whole
  subscription to recover from them would be absurdly heavy.

  What it must not become is a way to hide a permanently-failing handler, so the
  count is small and the schedule short — jittered, for the same reason
  reconnects are: a shared downstream going down fails every consumer's handler
  at roughly the same instant. Once the attempts are spent,
  `DMEx.Consumer.Failure` decides what happens.

  ## 5. The handler and the cursor share one transaction

  `handler.handle_message/2` succeeding is what advances `last_seen`, and both
  happen in one transaction against the consumer's own storage, so DMEx's
  bookkeeping cannot drift from the callback. If the handler raises, the
  transaction rolls back, the cursor does not move, and the exception
  propagates.
  """

  alias DMEx.Backoff
  alias DMEx.Consumer.Failure
  alias DMEx.Consumer.Handler

  @typedoc "Everything processing a row needs, built once per subscription."
  @type t :: %{
          required(:storage) => module(),
          required(:storage_opts) => DMEx.Storage.opts(),
          required(:handler) => module(),
          required(:subscription) => String.t(),
          required(:modules) => %{String.t() => module()},
          required(:url) => String.t(),
          required(:cursor) => non_neg_integer(),
          required(:config) => DMEx.Config.Consumer.t()
        }

  @doc """
  Runs one row through the pipeline and returns the new cursor.

  The row is a decoded log row from a `DMEx.Frame` — string-keyed, with `data`
  and `metadata` already one level decoded.

  Propagates whatever the handler raises. That is the `:crash` behaviour: the
  stream process dies, its supervisor restarts it, it re-reads its persisted
  cursor — still pointing at the message before this one, since that transaction
  never committed — and the producer's ordinary range scan re-streams the same
  message.
  """
  @spec run(map(), t()) :: {:ok, non_neg_integer()}
  def run(row, state) do
    id = Map.fetch!(row, "id")

    if id <= state.cursor do
      {:ok, state.cursor}
    else
      dispatch(row, id, state)
    end
  end

  @doc """
  Runs a list of rows in order, stopping at the first that raises.

  Returns the cursor reached. Rows are processed strictly one at a time: a batch
  is a wire-framing convenience and never a processing unit, so a mid-batch
  failure only ever rolls back that one message.
  """
  @spec run_all([map()], t()) :: {:ok, non_neg_integer()}
  def run_all(rows, state) do
    Enum.reduce(rows, {:ok, state.cursor}, fn row, {:ok, cursor} ->
      run(row, %{state | cursor: cursor})
    end)
  end

  ## Internals

  defp dispatch(row, id, state) do
    case Map.fetch(state.modules, Map.fetch!(row, "message_type")) do
      {:ok, module} -> handle(row, id, module, state)
      :error -> skip(row, id, state)
    end
  end

  defp handle(row, id, module, state) do
    message = module.cast(Map.fetch!(row, "data"))
    metadata = Map.get(row, "metadata") || %{}

    attempt(row, id, message, metadata, state, 1)
  end

  # One attempt, one telemetry span. A retried message therefore emits an
  # `:exception` event per failed try and a `:stop` for the one that worked,
  # and — importantly for the adaptive controller later — no span ever covers
  # backoff. Sleeping is a failure mode, not capacity.
  defp attempt(row, id, message, metadata, state, attempt) do
    telemetry = %{
      subscription: state.subscription,
      handler: state.handler,
      message_type: Map.fetch!(row, "message_type"),
      url: state.url,
      id: id,
      attempt: attempt
    }

    :telemetry.span([:dmex, :consumer, :message], telemetry, fn ->
      commit!(state, fn ->
        # The callback's return value is deliberately ignored: raising is how a
        # handler signals failure, so there is no :ok/:error protocol to get
        # wrong.
        _ = state.handler.handle_message(message, metadata)

        advance!(state, id)
      end)

      {{:ok, id}, telemetry}
    end)
  rescue
    exception ->
      stacktrace = __STACKTRACE__

      if attempt < state.config.max_attempts do
        # Still in hand — no refetch is needed, and the transaction rolled back,
        # so retrying is simply calling the handler again. Blocking here is
        # correct rather than lazy: the stream processes one message at a time,
        # so there is nothing else it could usefully do, and pausing it is what
        # applies backpressure to the producer while a handler is struggling.
        attempt
        |> Backoff.delay(state.config.retry_backoff, state.config.retry_backoff_max)
        |> Process.sleep()

        attempt(row, id, message, metadata, state, attempt + 1)
      else
        Failure.resolve(state, row, id, exception, stacktrace, attempt)
      end
  end

  defp skip(row, id, state) do
    commit!(state, fn -> advance!(state, id) end)

    :telemetry.execute(
      [:dmex, :consumer, :skip],
      %{id: id},
      %{
        subscription: state.subscription,
        handler: state.handler,
        message_type: Map.fetch!(row, "message_type"),
        url: state.url
      }
    )

    {:ok, id}
  end

  defp commit!(state, fun) do
    {:ok, result} = state.storage.transaction(state.storage_opts, fun)

    result
  end

  defp advance!(state, id) do
    :ok = state.storage.advance(state.storage_opts, state.subscription, id, state.url)
  end

  @doc """
  Builds pipeline state from the handler's declaration, once per subscription.

  The message list is derived at compile time from the structs the handler's
  `c:DMEx.Consumer.Handler.handle_message/2` clauses match on, and every one of
  them was checked to be a `DMEx.Message` there — so there is nothing left for
  this to validate but the handler itself.

  Resolving `message_type` per row is a map lookup against the resulting
  registry — never `String.to_atom/1` on whatever the producer sent, which would
  be an unbounded-atom-table risk on attacker-controlled input.
  """
  @spec build(keyword()) :: t()
  def build(opts) do
    handler = Keyword.fetch!(opts, :handler)

    unless Handler.handler?(handler) do
      raise ArgumentError,
            "#{inspect(handler)} is not a DMEx handler. " <>
              "Add `use DMEx.Consumer.Handler` to it."
    end

    declaration = handler.__dmex__()

    %{
      storage: Keyword.fetch!(opts, :storage),
      storage_opts: Keyword.fetch!(opts, :storage_opts),
      handler: handler,
      subscription: Keyword.get(opts, :subscription, declaration.id),
      modules: DMEx.Message.registry(declaration.messages),
      url: Keyword.fetch!(opts, :url),
      cursor: Keyword.get(opts, :cursor, 0),
      config: Keyword.get_lazy(opts, :config, fn -> DMEx.Config.consumer!([]) end)
    }
  end
end
