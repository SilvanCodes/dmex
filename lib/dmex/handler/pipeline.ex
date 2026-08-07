defmodule DMEx.Handler.Pipeline do
  @moduledoc """
  Processes one row: suppress, resolve, decode, handle, advance.

  Every handler-side semantic lives here, and the order of operations *is* the
  specification. `DMEx.Handler.DeadMessages` replays through `deliver/3` rather
  than repeating any of it — see there for the one section it deliberately does
  not inherit.

  ## 1. Duplicate suppression, by id

  A row whose `id` is at or below the cursor was already committed, so it is
  dropped before any transaction opens. This needs no dedupe table: delivery
  within a subscription is strictly ordered and `last_seen` is durable, so
  "already processed" is exactly "id not greater than the cursor". It cannot
  cover a crash between a handler's side effects landing and its transaction
  committing, because it is that commit that records the message as processed.

  ## 2. A message no clause matches is skipped, not an error

  Two rows land here. One whose `type` resolves to no module at all: a
  filter false positive admits one, and a whole-log handler is sent every type
  its log holds, including those its application has never heard of. And one
  whose type *does* resolve but whose value no clause head accepts, because
  selection is by type while a clause may narrow further —
  `%OrderPlaced{status: "paid"}` declares `OrderPlaced` and the log has no way to
  send only the paid ones.

  Neither is a fault. Both advance the cursor exactly as a handled row does —
  they must, or a handler would wedge forever on the first row it does not want
  — and both emit `[:dmex, :handler, :skip]`.

  **They are one question, so they are one call.** `__dmex_cast__/3` — which
  `DMEx.Handler` compiles from the handler's own clauses — decodes the row and
  hands it back only if a clause head accepts it, and answers `nil` otherwise.
  There is no separate type lookup to disagree with the clauses, and no way to
  decode a row without asking whether it is wanted.

  It is asked *before* the transaction opens, so a row nothing wants costs a
  call and a struct match. Nothing calls the handler and rescues a
  `FunctionClauseError` to find out, which leaves raising meaning only what
  section 4 says it means.

  ## 2a. A skip's advance is coalesced; a handled message's is not

  A skip runs no handler, so its advance is atomic with *nothing*, and on a
  mostly-skipped stream a transaction per row is the entire cost of consuming
  (measured — see ARCHITECTURE's *What has been measured*). So a skip advances
  `:cursor`, sets `:pending`, and returns without touching the database; the run
  is persisted by `flush/1` on a `checkpoint_interval` timer and before anything
  re-reads the durable cursor.

  **A handled message commits immediately and needs no flush.**
  `c:DMEx.Storage.advance/3` sets `last_seen` absolutely, so advancing to a
  handled row's id *subsumes* every pending skip below it — skips at 5, 6, 7
  followed by a handled 8 leave the cursor at 8 either way. So coalescing leaves
  the handler/cursor transaction untouched.

  A crash with a pending run costs re-reading and re-skipping those rows,
  bounded by `checkpoint_interval`. Nothing is lost and nothing is handled
  twice, because a pending run is by definition rows no code ran for.

  ## 3. Decoding happens outside the transaction

  `c:DMEx.Message.cast/1` is CPU work, not IO, and holding a connection across it
  would inflate transaction time — and therefore lock time — for nothing.

  ## 4. Failures are retried a bounded number of times, then resolved

  A raise rolls the transaction back with the message still in hand, so a retry
  is just calling the handler again — worth `max_attempts` tries for a transient
  deadlock or a blipped downstream. Jittered, because a shared downstream fails
  every handler at once.

  **Only the handler's own failure counts.** `c:DMEx.Storage.transaction/2`
  answering `{:error, reason}` says the message was never processed — a checkout
  that timed out, a dropped connection, a serialization failure — so it raises
  `DMEx.Storage.Error` and nothing here rescues it. Retrying would re-run side
  effects that never failed, and section 6 would resolve a message no handler
  rejected. The stream dies, the supervisor restarts it, and it resumes from its
  durable cursor — which still points *below* this message, since the advance
  went down with the transaction.

  ## 5. The handler and the cursor share one transaction

  `handle_message/2` succeeding is what advances `last_seen`, both in one
  transaction. A raise rolls both back and propagates.

  ## 6. `on_failure` is the terminal action, and there are three

  The retry step is identical in all three. Both non-crashing modes commit an
  **absolute** advance to the failing row's id, carrying any coalesced run of
  skips below it (section 2a).

  **What reaches here is a handler that ran and raised, `max_attempts` times.**
  A storage failure never does (section 4), and neither does a row no clause
  wanted (section 2) — those are not messages a handler could not process.

  **`:dead_message`** (the default) advances past the failing message, writing it
  to `dmex_dead_messages` first, in the same transaction — so there is no state
  where a message was skipped without being recorded, or recorded without being
  skipped. The default because it is the only mode refusing both bad outcomes:
  one message a handler cannot process is not a reason for every *later* message
  to stop arriving, nor a reason to lose the failing one. What is stored is the
  message and only the message — see `c:DMEx.Storage.dead_message/2`.

  **`:crash`** re-raises with the original stacktrace and without advancing, so a
  pending run of skips is re-skipped after the restart. The supervisor restarts
  the stream, it re-reads its persisted cursor — still pointing at the message
  *before* this one — and the log's range scan re-streams the message that
  killed it, forever. Choose it when later messages must not be processed
  without this one, or when a loud alarm beats a growing table nobody watches.
  A jittered `reconnect_backoff_base` pause before the re-raise keeps the loop off the
  log's database and off its siblings; `DMEx.Handler.Subscriptions` deliberately
  sets no restart budget.

  **`:skip`** advances past the failing message without the handler having
  succeeded. Nothing durable is recorded: the log line and
  `[:dmex, :handler, :failed]` are the only trace.
  """

  alias DMEx.Handler
  alias DMEx.Storage.Error

  require Logger

  @typedoc """
  One row as a transport yields it: string-keyed, with `data` and `metadata`
  decoded one level.

  Turning `data` into a message struct is the message module's job, here, per
  row — so a transport produces the same shape whether it parsed a frame or read
  the columns directly.
  """
  @type decoded_row :: %{String.t() => term()}

  @typedoc """
  Everything processing a row needs, built once per subscription.

  `:cursor` is the in-memory read position — the authority for duplicate
  suppression, always at or ahead of the durable one. `:pending` says whether it
  has been written yet (section 2a). A flag rather than a position, because a
  position would be a second copy of `:cursor` that could go stale.
  """
  @type t :: %{
          required(:storage) => module(),
          required(:storage_opts) => DMEx.Storage.opts(),
          required(:handler) => module(),
          required(:subscription) => String.t(),
          required(:origin) => term(),
          required(:cursor) => non_neg_integer(),
          required(:pending) => boolean(),
          required(:config) => DMEx.Config.handler()
        }

  @doc """
  Runs one row through the pipeline and returns the updated state.

  Read `:cursor` off the result for the new read position, and `:pending` for
  whether it is waiting on `flush/1`.

  Propagates whatever the handler raises once retries are exhausted and
  `on_failure` says `:crash`, and `DMEx.Storage.Error` whenever storage itself
  failed — that one at once, and whatever `on_failure` says.
  """
  @spec run(decoded_row(), t()) :: {:ok, t()}
  def run(row, state) do
    id = Map.fetch!(row, "id")

    if id <= state.cursor, do: {:ok, state}, else: settle(row, state, id)
  end

  defp settle(row, state, id) do
    case deliver(state, row, fn -> advance!(state, id) end) do
      # No transaction and no write happened. The advance is deferred into
      # `:pending` and settled by `flush/1` or by the next handled message —
      # see section 2a.
      :skipped ->
        {:ok, %{state | cursor: id, pending: true}}

      {:handled, _attempts} ->
        {:ok, %{state | cursor: id, pending: false}}

      {:failed, exception, stacktrace, attempts} ->
        resolve(state, row, id, exception, stacktrace, attempts)
    end
  end

  @typedoc """
  What `deliver/3` did with a row, and after how many handler calls.
  """
  @type verdict ::
          :skipped
          | {:handled, pos_integer()}
          | {:failed, Exception.t(), Exception.stacktrace(), pos_integer()}

  @doc """
  Resolves a row, hands it to the handler, and commits `commit` with it.

  Sections 2 to 5 in one call: the cast that decides skip-or-deliver, the
  `[:dmex, :handler, :skip]` event, the retry schedule, the
  `[:dmex, :handler, :message]` span, and the rule that storage failing is not
  the handler failing. `commit` runs inside the handler's transaction, after it
  returns — the cursor advance here, the dead row's delete in
  `DMEx.Handler.DeadMessages`, which is the only thing that differs between the
  two callers of this.

  It never applies `on_failure`: a handler that ran out of attempts comes back
  as `{:failed, ...}` and section 6 is `run/2`'s answer to it, so a caller for
  which those terminal actions are meaningless does not inherit them.
  """
  @spec deliver(t(), decoded_row(), (-> any())) :: verdict()
  def deliver(state, row, commit) do
    id = Map.fetch!(row, "id")
    type = Map.fetch!(row, "type")
    metadata = Map.get(row, "metadata") || %{}

    case state.handler.__dmex_cast__(type, Map.fetch!(row, "data"), metadata) do
      nil ->
        skip(state, id, type)

      message ->
        attempt(
          state,
          %{id: id, type: type, message: message, metadata: metadata},
          commit,
          1
        )
    end
  end

  @doc """
  Persists a coalesced run of skips, if there is one.

  A no-op when nothing is pending — a handled message's own advance already
  covered the skips below it.
  """
  @spec flush(t()) :: {:ok, t()}
  def flush(%{pending: false} = state), do: {:ok, state}

  def flush(%{pending: true, cursor: cursor} = state) do
    commit!(state, fn -> advance!(state, cursor) end)

    :telemetry.execute(
      [:dmex, :handler, :checkpoint],
      %{cursor: cursor},
      %{subscription: state.subscription, handler: state.handler, origin: state.origin}
    )

    {:ok, %{state | pending: false}}
  end

  @doc """
  Builds pipeline state from the handler's declaration, once per subscription.

  The message set was derived and validated at compile time, so there is nothing
  left for this to check but the handler itself — and nothing to carry, since
  resolving a `type` is the handler's own `__dmex_cast__/3`, compiled
  from that set. Never `String.to_atom/1` on whatever the log sent, which would
  be an unbounded-atom-table risk on attacker-controlled input.
  """
  @spec new(keyword()) :: t()
  def new(opts) do
    handler = Keyword.fetch!(opts, :handler)

    if !Handler.handler?(handler) do
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
      origin: Keyword.fetch!(opts, :origin),
      cursor: Keyword.get(opts, :cursor, 0),
      pending: false,
      config: Keyword.get_lazy(opts, :config, fn -> DMEx.Config.handler!([]) end)
    }
  end

  # `DMEx.Handler.Subscription` always names the subscription it runs, since it is one
  # of possibly several. A caller that did not — a test — only has an answer
  # when there is exactly one, and it renders that answer through
  # `Handler.subscription_id!/2` rather than concatenating its own.
  defp subscription!(%{logs: [only], handler: handler}) do
    Handler.subscription_id!(handler, only)
  end

  defp subscription!(%{logs: logs, handler: handler}) do
    raise ArgumentError,
          "#{inspect(handler)} reads #{length(logs)} logs (#{Enum.join(logs, ", ")}), so a " <>
            "pipeline for it has to be built with the `subscription:` it is running."
  end

  ## Internals

  defp attempt(state, delivery, commit, attempt) do
    telemetry = %{
      subscription: state.subscription,
      handler: state.handler,
      type: delivery.type,
      origin: state.origin,
      id: delivery.id,
      attempt: attempt
    }

    :telemetry.span([:dmex, :handler, :message], telemetry, fn ->
      commit!(state, fn ->
        _ = state.handler.handle_message(delivery.message, delivery.metadata)

        commit.()
      end)

      {{:handled, attempt}, telemetry}
    end)
  rescue
    # Storage failing is not the handler failing. Retrying re-runs side effects
    # that never failed, and `on_failure` would dead-letter a message the
    # handler accepted — so this leaves by the same door `give_up/6` and
    # `flush/1` use, with the cursor still below the message.
    storage_error in Error ->
      reraise storage_error, __STACKTRACE__

    exception ->
      stacktrace = __STACKTRACE__

      if attempt < state.config.max_attempts do
        # Blocking here is correct rather than lazy: the stream processes one
        # message at a time, so there is nothing else it could usefully do, and
        # pausing it applies backpressure to the log while a handler struggles.
        attempt
        |> DMEx.backoff(state.config.retry_backoff_base, state.config.retry_backoff_max)
        |> Process.sleep()

        attempt(state, delivery, commit, attempt + 1)
      else
        {:failed, exception, stacktrace, attempt}
      end
  end

  ## Terminal actions — see section 6

  defp resolve(state, row, id, exception, stacktrace, attempts) do
    case state.config.on_failure do
      :crash ->
        # The pause has to happen *here*, in the process that is about to die.
        # A restart builds fresh state and connects straight away, so nothing
        # after the crash is in a position to pace the loop — and without this a
        # poison message reconnects to another application's log as fast as the
        # supervisor can restart this process, which is why
        # `DMEx.Handler.Subscriptions` can afford to set no restart budget.
        state.config
        |> crash_backoff()
        |> Process.sleep()

        reraise(exception, stacktrace)

      mode ->
        give_up(state, row, id, exception, attempts, mode)
    end
  end

  # A flat jittered draw rather than an escalating one: the counter an escalating
  # schedule needs cannot survive the restart it is meant to pace.
  defp crash_backoff(config) do
    DMEx.backoff(1, config.reconnect_backoff_base, config.reconnect_backoff_max)
  end

  defp give_up(state, row, id, exception, attempts, mode) do
    # Outside any rescue, and so is `attempt/4`'s commit now: a `DMEx.Storage.Error`
    # from either one leaves the same way, with the cursor still below the
    # message. The record and the advance are one transaction, so a failure here
    # means neither happened and the restart re-delivers the row.
    commit!(state, fn ->
      if mode == :dead_message, do: record_dead!(state, row, id)

      advance!(state, id)
    end)

    report(state, row, id, exception, attempts, mode)

    {:ok, %{state | cursor: id, pending: false}}
  end

  defp record_dead!(state, row, id) do
    :ok =
      state.storage.record_dead_message(state.storage_opts, %{
        subscription: state.subscription,
        id: id,
        type: Map.fetch!(row, "type"),
        data: encode(Map.get(row, "data")),
        metadata: encode(Map.get(row, "metadata"))
      })
  end

  # The transport decoded the row one level, so these are terms here and the
  # storage adapter wants JSON text.
  defp encode(nil), do: "{}"
  defp encode(value), do: JSON.encode!(value)

  defp report(state, row, id, exception, attempts, mode) do
    Logger.error(fn ->
      "DMEx #{state.subscription} (#{inspect(state.handler)}) gave up on message #{id} " <>
        "(#{Map.fetch!(row, "type")}) from #{inspect(state.origin)} after #{attempts} " <>
        "attempt(s): #{Exception.message(exception)} — on_failure: #{mode}"
    end)

    :telemetry.execute(
      [:dmex, :handler, :failed],
      %{id: id, attempts: attempts},
      %{
        subscription: state.subscription,
        handler: state.handler,
        type: Map.fetch!(row, "type"),
        origin: state.origin,
        on_failure: mode,
        reason: Exception.message(exception)
      }
    )
  end

  defp skip(state, id, type) do
    :telemetry.execute(
      [:dmex, :handler, :skip],
      %{id: id},
      %{
        subscription: state.subscription,
        handler: state.handler,
        type: type,
        origin: state.origin
      }
    )

    :skipped
  end

  # `{:error, reason}` is storage saying *it* failed — a checkout that timed
  # out, a connection that dropped, a serialization failure. Not a verdict on
  # the message, so it gets its own exception rather than whatever a failed
  # match would have produced, and every caller lets it through.
  defp commit!(state, fun) do
    case state.storage.transaction(state.storage_opts, fun) do
      {:ok, result} -> result
      {:error, reason} -> raise Error, reason
    end
  end

  defp advance!(state, last_seen) do
    :ok = state.storage.advance_cursor(state.storage_opts, state.subscription, last_seen)
  end
end
