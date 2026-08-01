defmodule DMEx.Handler.Pipeline do
  @moduledoc """
  Processes one row: suppress, resolve, decode, handle, advance.

  Every handler-side semantic lives here, and the order of operations *is* the
  specification. It is deliberately separate from `DMEx.Handler.Stream` so it
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

  Server-side selection makes this rare rather than obsolete, and the difference
  matters. **Filter inclusion is heuristic in one direction**: there are no
  false negatives, so every selected type arrives — but a false positive admits
  one entire message type for the whole life of a connection (see
  `DMEx.Fingerprint`). A handler declared `selective: false` asks for the
  whole log outright. Either way an undecodable row can arrive, so this path is a live
  defence rather than dead code, and an unknown type is **not** a fault to
  surface.

  **Such a row advances the cursor exactly as a handled one does.** It must, or
  a handler would wedge forever on the first message type it does not know.
  Skips get a telemetry event, because a handler silently discarding most of a
  stream and a handler silently discarding the one type it wanted look
  identical otherwise.

  ## 2a. A skip's advance is coalesced; a handled message's is not

  A skip runs no handler, so its cursor advance is atomic with *nothing* — there
  is no side effect for it to commit alongside. Paying a transaction and a
  `last_seen` write per skipped row therefore buys nothing, and on a mostly-
  skipped stream it is the entire cost of consuming: measured at ~250 rows/s
  against ~2 400 rows/s for the same bytes read and discarded.

  So a skip advances `:cursor`, sets the `:pending` flag, and returns without
  touching the database. The run is persisted by `flush/1` — driven by
  `DMEx.Handler.Stream` on a `checkpoint_interval` timer, and before anything
  that re-reads the durable cursor. There is no separate pending *position* to
  write: the cursor already is one, and a coalesced run is only ever settled at
  its head.

  **A handled message still commits immediately, and needs no flush of its
  own.** `c:DMEx.Storage.advance/3` sets `last_seen` absolutely rather than
  incrementally, so advancing to a handled row's id *subsumes* every pending
  skip below it — skips at 5, 6, 7 followed by a handled 8 leave the cursor at
  8 whether or not those skips were ever written. Clearing `:pending` is the
  whole of it. That is why coalescing costs the handler/cursor guarantee in
  section 5 nothing at all.

  What a crash with a pending run costs is re-reading those rows and skipping
  them again, bounded by `checkpoint_interval`. Nothing is lost and nothing is
  handled twice, because the rows in a pending run are by definition rows no
  code ran for.

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
  reconnects are: a shared downstream going down fails every handler's handler
  at roughly the same instant. Once the attempts are spent, `on_failure` decides
  what happens; section 6 is that decision.

  ## 5. The handler and the cursor share one transaction

  `handler.handle_message/2` succeeding is what advances `last_seen`, and both
  happen in one transaction against the handler's own storage, so DMEx's
  bookkeeping cannot drift from the callback. If the handler raises, the
  transaction rolls back, the cursor does not move, and the exception
  propagates.

  ## 6. `on_failure` is the terminal action, and there are three

  The retry step above is identical in all three; only what happens after it
  differs. Both non-crashing modes commit an **absolute** advance to the failing
  row's id, which carries any coalesced run of skips below it (section 2a) — so
  `:pending` is cleared rather than flushed separately, and a dead-message row
  stays atomic with the advance that moved past it.

  ### `:dead_message` (the default)

  Advances past the failing message, but writes it to `dmex_dead_messages`
  first, **in the same transaction**. That atomicity is the point: a dead
  message and the cursor that skipped past it commit together or not at all, so
  there is no state where a message was skipped without being recorded, or
  recorded without being skipped.

  It is the default because it is the only mode that refuses both bad outcomes.
  One message a handler cannot process is a bug in that handler or a malformed
  payload, not a reason for every *later* message to stop arriving — and it is
  not a reason to lose the failing one either. Recording it and moving on keeps
  the subscription live and keeps the evidence, and `DMEx.Handler.DeadMessages`
  is how you inspect, replay or discard what accumulated.

  **What is stored is the message, and only the message.** The dead-messages
  table is structurally `dmex_messages` plus the subscription id that says whose
  row it is, so a stored row *is* a log row — replaying one is feeding it back
  to a handler with no reshaping in between. Equal as a value rather than
  byte-for-byte: the payload is re-encoded from the term the frame decoded.

  The exception, its message and the attempt count are **not** stored. A failure
  is an event, not a property of the message: it goes to the log and to
  `[:dmex, :handler, :failed]`. Persisting it would make each row a mixture of
  a message and an incident, and stop it being replayable as the former.

  ### `:crash`

  Re-raises, with the original stacktrace, and without advancing — so a pending
  run of skips is simply dropped and re-skipped after the restart, which is what
  a pending run always costs. The stream process dies, its supervisor restarts
  it, and on restart it re-reads its persisted cursor — still pointing at the
  message *before* this one — so the log's ordinary range scan re-streams
  the very message that killed it. A permanently-failing handler crash-loops on
  it forever.

  Choose this when a stuck subscription is the *better* failure: when later
  messages genuinely must not be processed without this one, or when a loud,
  impossible-to-ignore alarm beats a growing table nobody is watching. Reconnect
  backoff inside `DMEx.Handler.Stream` keeps the loop from hammering the
  log's database, and `DMEx.Handler.Supervisor`'s restart budget is set so
  it cannot take its sibling subscriptions down with it.

  ### `:skip`

  Commits an advance past the failing message *without* the handler having
  succeeded, and carries on. The message is simply gone from this handler's
  perspective.

  DMEx records nothing durable: the failure is logged and emitted as
  `[:dmex, :handler, :failed]`, and that is the only trace. It is
  `:dead_message` minus the evidence, which is worth being deliberate about.
  """

  alias DMEx.Backoff
  alias DMEx.Handler

  require Logger

  @typedoc """
  Everything processing a row needs, built once per subscription.

  `:cursor` is the in-memory read position — the authority for duplicate
  suppression, and always at or ahead of the durable one. `:pending` says
  whether it has been written yet; see section 2a.

  A flag rather than a position, because a position would be a second copy of
  `:cursor`: a skip is the only thing that sets it, and it sets both in the same
  update. Nothing can advance the cursor and leave a stale pending id behind if
  there is no pending id to go stale.
  """
  @type t :: %{
          required(:storage) => module(),
          required(:storage_opts) => DMEx.Storage.opts(),
          required(:handler) => module(),
          required(:subscription) => String.t(),
          required(:modules) => %{String.t() => module()},
          required(:url) => String.t(),
          required(:cursor) => non_neg_integer(),
          required(:pending) => boolean(),
          required(:config) => DMEx.Config.handler()
        }

  @doc """
  Runs one row through the pipeline and returns the updated state.

  The row is a decoded log row from a `DMEx.SSE.Frame` — string-keyed, with `data`
  and `metadata` already one level decoded.

  Read `:cursor` off the result for the new read position, and `:pending` for
  whether it is waiting on `flush/1` to be written.

  Propagates whatever the handler raises, once retries are exhausted and
  `on_failure` says `:crash`: the stream process dies, its supervisor restarts
  it, it re-reads its persisted cursor — still pointing at the message before
  this one, since that transaction never committed — and the log's ordinary
  range scan re-streams the same message.
  """
  @spec run(map(), t()) :: {:ok, t()}
  def run(row, state) do
    id = Map.fetch!(row, "id")

    if id <= state.cursor do
      {:ok, state}
    else
      dispatch(row, id, state)
    end
  end

  @doc """
  Persists a coalesced run of skips, if there is one.

  A no-op when nothing is pending, which is the common case — any handled
  message clears the marker on its way past, since its own advance already
  covered the skips below it.
  """
  @spec flush(t()) :: {:ok, t()}
  def flush(%{pending: false} = state), do: {:ok, state}

  def flush(%{pending: true, cursor: cursor} = state) do
    commit!(state, fn -> advance!(state, cursor) end)

    :telemetry.execute(
      [:dmex, :handler, :checkpoint],
      %{id: cursor},
      %{subscription: state.subscription, handler: state.handler, url: state.url}
    )

    {:ok, %{state | pending: false}}
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
  # `:exception` event per failed try and a `:stop` for the one that worked, and
  # no span ever covers backoff — an attempt's duration is what the handler
  # costs, and a retry sequence's is mostly sleeping.
  defp attempt(row, id, message, metadata, state, attempt) do
    telemetry = %{
      subscription: state.subscription,
      handler: state.handler,
      message_type: Map.fetch!(row, "message_type"),
      url: state.url,
      id: id,
      attempt: attempt
    }

    :telemetry.span([:dmex, :handler, :message], telemetry, fn ->
      commit!(state, fn ->
        # The callback's return value is deliberately ignored: raising is how a
        # handler signals failure, so there is no :ok/:error protocol to get
        # wrong.
        _ = state.handler.handle_message(message, metadata)

        advance!(state, id)
      end)

      # `pending` is cleared rather than flushed: the advance just committed is
      # absolute, so it already carried every skip below this row with it.
      {{:ok, %{state | cursor: id, pending: false}}, telemetry}
    end)
  rescue
    exception ->
      stacktrace = __STACKTRACE__

      if attempt < state.config.max_attempts do
        # Still in hand — no refetch is needed, and the transaction rolled back,
        # so retrying is simply calling the handler again. Blocking here is
        # correct rather than lazy: the stream processes one message at a time,
        # so there is nothing else it could usefully do, and pausing it is what
        # applies backpressure to the log while a handler is struggling.
        attempt
        |> Backoff.delay(state.config.retry_backoff, state.config.retry_backoff_max)
        |> Process.sleep()

        attempt(row, id, message, metadata, state, attempt + 1)
      else
        resolve(state, row, id, exception, stacktrace, attempt)
      end
  end

  ## Terminal actions — see section 6

  defp resolve(state, row, id, exception, stacktrace, attempts) do
    case state.config.on_failure do
      :crash -> reraise(exception, stacktrace)
      :skip -> give_up(state, row, id, exception, attempts, :skip)
      :dead_message -> give_up(state, row, id, exception, attempts, :dead_message)
    end
  end

  defp give_up(state, row, id, exception, attempts, mode) do
    commit!(state, fn ->
      # The dead-message row and the advance past it are one transaction,
      # deliberately: they are the same fact, and a state where one landed
      # without the other is not one anybody could act on.
      if mode == :dead_message, do: record_dead!(state, row, id)

      advance!(state, id)
    end)

    report(state, row, id, exception, attempts, mode)

    {:ok, %{state | cursor: id, pending: false}}
  end

  defp record_dead!(state, row, id) do
    :ok =
      state.storage.dead_message(state.storage_opts, %{
        subscription: state.subscription,
        id: id,
        message_type: Map.fetch!(row, "message_type"),
        data: encode(Map.get(row, "data")),
        metadata: encode(Map.get(row, "metadata"))
      })
  end

  # The row arrived through `DMEx.SSE.Frame`, which decodes one level, so `data`
  # and `metadata` are terms here and the storage adapter wants JSON text.
  defp encode(nil), do: "{}"
  defp encode(value), do: JSON.encode!(value)

  defp report(state, row, id, exception, attempts, mode) do
    Logger.error(fn ->
      "DMEx #{state.subscription} (#{inspect(state.handler)}) gave up on message #{id} " <>
        "(#{Map.fetch!(row, "message_type")}) from #{state.url} after #{attempts} " <>
        "attempt(s): #{Exception.message(exception)} — on_failure: #{mode}"
    end)

    :telemetry.execute(
      [:dmex, :handler, :failed],
      %{id: id, attempts: attempts},
      %{
        subscription: state.subscription,
        handler: state.handler,
        message_type: Map.fetch!(row, "message_type"),
        url: state.url,
        on_failure: mode,
        reason: Exception.message(exception)
      }
    )
  end

  # No transaction and no write. The advance is deferred into `:pending` and
  # settled by `flush/1` or by the next handled message — see section 2a.
  defp skip(row, id, state) do
    :telemetry.execute(
      [:dmex, :handler, :skip],
      %{id: id},
      %{
        subscription: state.subscription,
        handler: state.handler,
        message_type: Map.fetch!(row, "message_type"),
        url: state.url
      }
    )

    {:ok, %{state | cursor: id, pending: true}}
  end

  defp commit!(state, fun) do
    {:ok, result} = state.storage.transaction(state.storage_opts, fun)

    result
  end

  defp advance!(state, id) do
    :ok = state.storage.advance(state.storage_opts, state.subscription, id)
  end

  @doc """
  Builds pipeline state from the handler's declaration, once per subscription.

  The message set is derived at compile time from the structs the handler's
  `c:DMEx.Handler.handle_message/2` clauses match on, and every one of
  them was checked to be a `DMEx.Message` there — so there is nothing left for
  this to validate but the handler itself.

  Resolving `message_type` per row is a map lookup against the resulting
  registry — never `String.to_atom/1` on whatever the log sent, which would
  be an unbounded-atom-table risk on attacker-controlled input.
  """
  @spec build(keyword()) :: t()
  def build(opts) do
    handler = Keyword.fetch!(opts, :handler)

    unless Handler.handler?(handler) do
      raise ArgumentError,
            "#{inspect(handler)} is not a DMEx handler. " <>
              "Add `use DMEx.Handler` to it."
    end

    declaration = handler.__dmex__()

    %{
      storage: Keyword.fetch!(opts, :storage),
      storage_opts: Keyword.fetch!(opts, :storage_opts),
      handler: handler,
      subscription: Keyword.get_lazy(opts, :subscription, fn -> subscription!(declaration) end),
      modules: DMEx.Message.registry(declaration.messages),
      url: Keyword.fetch!(opts, :url),
      cursor: Keyword.get(opts, :cursor, 0),
      pending: false,
      config: Keyword.get_lazy(opts, :config, fn -> DMEx.Config.handler!([]) end)
    }
  end

  # `DMEx.Handler.Stream` always names the subscription it is running, since it
  # is one of possibly several the handler declares. A caller that did not — a
  # test, or a replay — only has an answer when there is exactly one.
  defp subscription!(%{logs: [only], id: id}), do: only <> "/" <> id

  defp subscription!(%{logs: logs, handler: handler}) do
    raise ArgumentError,
          "#{inspect(handler)} reads #{length(logs)} logs (#{Enum.join(logs, ", ")}), so a " <>
            "pipeline for it has to be built with the `subscription:` it is running."
  end
end
