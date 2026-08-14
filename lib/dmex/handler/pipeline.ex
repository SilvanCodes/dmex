defmodule DMEx.Handler.Pipeline do
  @moduledoc """
  Processes one row: suppress, resolve, decode, handle, advance.

  The order of operations *is* the specification, and every handler-side semantic
  lives here. `DMEx.Handler.DeadMessages` replays through `deliver/3` rather than
  repeating any of it — bar section 6, which it says why it does not inherit.

  ## 1. Duplicate suppression, by id

  A row at or below the cursor was already committed, so it is dropped before any
  transaction opens. No dedupe table: delivery within a subscription is strictly
  ordered and `last_seen` is durable, so "already processed" is exactly "id not
  greater than the cursor". The one crash window it cannot cover is
  ARCHITECTURE's *Delivery semantics*.

  ## 2. A message no clause matches is skipped, not an error

  Two rows land here: one whose `type` resolves to no module at all (a filter
  false positive, or a whole-log handler sent a type its application never
  declared), and one whose type does resolve but whose value no clause head
  accepts, since selection is by type while a clause may narrow further.

  Both advance the cursor exactly as a handled row does — they must, or a handler
  would wedge on the first row it does not want — and both emit
  `[:dmex, :handler, :skip]`.

  **One question, so one call.** `__dmex_cast__/3`, compiled from the handler's
  own clauses, decodes the row and returns it only if a clause head accepts it,
  so no type lookup can disagree with the clauses. It is asked *before* the
  transaction opens, and never by rescuing a `FunctionClauseError` — which leaves
  raising meaning only what section 4 says it means.

  ## 2a. A skip's advance is coalesced; a handled message's is not

  A skip runs no handler, so its advance is atomic with *nothing*, and on a
  mostly-skipped stream a transaction per row is the entire cost of consuming
  (measured — `bench/e2e/catchup_drain.exs`). So it advances `:cursor`, sets
  `:pending`, and touches no database; `flush/1` persists the run on a
  `checkpoint_interval` timer and before anything re-reads the durable cursor.

  A handled message commits its own advance, and
  `c:DMEx.Storage.advance_cursor/3` is absolute, so that advance *subsumes* every
  pending skip below it — skips at 5, 6, 7 then a handled 8 leave the cursor at 8
  either way, which is what leaves the handler/cursor transaction untouched. A
  crash with a run pending costs re-reading and re-skipping rows no code ran for,
  bounded by `checkpoint_interval`.

  ## 3. Decoding happens outside the transaction

  `c:DMEx.Message.cast/1` is CPU work, not IO, and holding a connection across it
  would inflate transaction time — and therefore lock time — for nothing.

  ## 4. Failures are retried a bounded number of times, then resolved

  A raise rolls the transaction back with the message still in hand, so a retry
  is just calling the handler again — `max_attempts` of them, jittered because a
  shared downstream fails every handler at once.

  **Only the handler's own failure counts**, tagged rather than enumerated:
  `handle!/2` is the one place a handler is called and it wraps what that call
  raised in `Failure`, which is all the retry schedule rescues. A checkout
  timeout, a driver error, an adapter's failed match on its own query result, or
  the cursor write itself is neither retried nor resolved by section 6 — that
  would re-run side effects which never failed and resolve a message no handler
  rejected. It kills the stream instead, and the restart resumes from a durable
  cursor still *below* this message, the advance having gone down with the
  transaction. No allowlist of driver exceptions stays complete; "what the
  handler raised" is one call.

  ## 5. The handler and the cursor share one transaction

  `handle_message/2` succeeding is what advances `last_seen`, both in one
  transaction. A raise rolls both back and propagates.

  ## 6. `on_failure` is the terminal action, and there are three

  What reaches here ran and raised `max_attempts` times: never a storage failure
  (section 4), never a row no clause wanted (section 2). The retry step is
  identical in all three modes, and both non-crashing modes commit an
  **absolute** advance to the failing row's id, carrying the coalesced skips
  below it (section 2a).

  **`:dead_message`** (the default) writes the row to `dmex_dead_messages` in that
  same transaction, so a message is never skipped without being recorded or
  recorded without being skipped. The only mode refusing both bad outcomes: one
  message a handler cannot process stopping every *later* message, and losing the
  failing one. The message and only the message is stored — see
  `c:DMEx.Storage.record_dead_message/2`.

  **`:crash`** re-raises the handler's own exception and stacktrace without
  advancing, so section 4's restart-and-resume happens here too and the log
  re-streams the message that killed it, forever. Choose it when later messages
  must not be processed without this one, or when a loud alarm beats a growing
  table nobody watches. A jittered `reconnect_backoff_base` pause before the
  re-raise keeps the loop off the log's database and off its siblings;
  `DMEx.Handler.Subscriptions` sets no restart budget.

  **`:skip`** advances past it with nothing durable recorded: the log line and
  `[:dmex, :handler, :failed]` are the only trace.
  """

  alias DMEx.Handler
  alias DMEx.Storage

  require Logger

  defmodule Failure do
    @moduledoc false

    # What `handle_message/2` raised, tagged so the pipeline can tell a verdict on
    # one message from the infrastructure under it failing — section 4. It carries
    # the original and its stacktrace, so `on_failure: :crash` can re-raise what a
    # reader of the crash needs and not the tag it travelled in.
    #
    # Visible from outside at `[:dmex, :handler, :message, :exception]`, where it
    # is what says the handler raised rather than storage — see `DMEx.Telemetry`.
    defexception [:exception, :stacktrace]

    @impl true
    def message(%__MODULE__{exception: exception}), do: Exception.message(exception)
  end

  @typedoc """
  One row as a transport yields it: string-keyed, `data` and `metadata` decoded
  one level and always objects, never `nil`. A row short of that is malformed and
  a transport rejects it there, so nothing here re-checks or defaults.
  """
  @type decoded_row :: %{String.t() => term()}

  @typedoc """
  Everything processing a row needs, built once per subscription.

  `:cursor` is the in-memory read position, always at or ahead of the durable
  one; `:pending` says whether it has been written yet (section 2a) — a flag
  rather than a position, which would be a second copy of `:cursor` to go stale.
  """
  @type t :: %{
          required(:storage) => Storage.opts(),
          required(:handler) => module(),
          required(:subscription) => String.t(),
          required(:origin) => term(),
          required(:cursor) => non_neg_integer(),
          required(:pending) => boolean(),
          required(:config) => DMEx.Config.handler()
        }

  @doc """
  Runs one row through the pipeline and returns the updated state: `:cursor` is
  the new read position, `:pending` whether it awaits `flush/1`.

  Propagates what the handler raised once retries are exhausted under
  `on_failure: :crash`, and anything storage raised at once, whatever
  `on_failure` says — section 4.
  """
  @spec run(decoded_row(), t()) :: {:ok, t()}
  def run(%{"id" => id}, %{cursor: cursor} = state) when id <= cursor, do: {:ok, state}

  def run(%{"id" => id} = row, state) do
    case deliver(state, row, fn -> advance!(state, id) end) do
      # No transaction and no write happened: the advance is deferred into
      # `:pending` and settled by `flush/1` or by the next handled message.
      :skipped ->
        {:ok, %{state | cursor: id, pending: true}}

      :handled ->
        {:ok, %{state | cursor: id, pending: false}}

      {:failed, failure, attempts} ->
        resolve(state, row, failure, attempts)
    end
  end

  @typedoc """
  What `deliver/3` did with a row, and after how many handler calls. The failure
  is the `Failure` the handler's raise was tagged with, so its exception and
  stacktrace travel as one thing.
  """
  @type verdict :: :skipped | :handled | {:failed, Exception.t(), pos_integer()}

  @doc """
  Resolves a row, hands it to the handler, and commits `commit` with it.

  Sections 2 to 5 in one call. `commit` runs inside the handler's transaction
  after it returns — the cursor advance here, the dead row's delete in
  `DMEx.Handler.DeadMessages`, the only difference between the two callers.

  It never applies `on_failure`: exhausted attempts come back as `{:failed, ...}`
  and section 6 is `run/2`'s answer to that, so a caller for which those terminal
  actions are meaningless does not inherit them.
  """
  @spec deliver(t(), decoded_row(), (-> any())) :: verdict()
  def deliver(state, %{"id" => id, "type" => type, "data" => data, "metadata" => metadata}, commit) do
    case state.handler.__dmex_cast__(type, data, metadata) do
      nil ->
        skip(state, id, type)

      message ->
        attempt(state, %{id: id, type: type, message: message, metadata: metadata}, commit, 1)
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
    transaction!(state, fn -> advance!(state, cursor) end)

    :telemetry.execute(
      [:dmex, :handler, :checkpoint],
      %{cursor: cursor},
      %{subscription: state.subscription, handler: state.handler, origin: state.origin}
    )

    {:ok, %{state | pending: false}}
  end

  @doc """
  Builds pipeline state from the handler's declaration, once per subscription.

  The message set was derived and validated at compile time, and resolving a
  `type` is the handler's own `__dmex_cast__/3` compiled from it, so there is
  nothing left to check but the handler and nothing to carry.
  """
  @spec new(keyword()) :: t()
  def new(opts) do
    handler = Keyword.fetch!(opts, :handler)

    if !Handler.handler?(handler) do
      raise ArgumentError,
            "#{inspect(handler)} is not a DMEx handler. " <>
              "Add `use DMEx.Handler` to it."
    end

    %{
      storage: Keyword.fetch!(opts, :storage),
      handler: handler,
      subscription: Keyword.get_lazy(opts, :subscription, fn -> subscription!(handler.__dmex__()) end),
      origin: Keyword.fetch!(opts, :origin),
      cursor: Keyword.get(opts, :cursor, 0),
      pending: false,
      config: Keyword.fetch!(opts, :config)
    }
  end

  ## Internals

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
      transaction!(state, fn ->
        handle!(state, delivery)

        commit.()
      end)

      {:handled, telemetry}
    end)
  rescue
    # The tag, and nothing else — section 4. Anything storage raised is not
    # rescued at all, so it leaves by the same door `resolve/4` and `flush/1`
    # use, with the cursor still below the message.
    failure in Failure ->
      if attempt < state.config.max_attempts do
        # Blocking here is correct rather than lazy: the stream processes one
        # message at a time, so there is nothing else it could usefully do, and
        # pausing it applies backpressure to the log while a handler struggles.
        attempt
        |> DMEx.backoff(state.config.retry_backoff_base, state.config.retry_backoff_max)
        |> Process.sleep()

        attempt(state, delivery, commit, attempt + 1)
      else
        {:failed, failure, attempt}
      end
  end

  # The one call whose raise is a verdict on the message. Everything else between
  # here and the commit belongs to storage, which is why this is what gets
  # tagged: the set of ways a database can fail is open-ended, and the set of
  # ways a handler is invoked is one.
  defp handle!(state, delivery) do
    _ = state.handler.handle_message(delivery.message, delivery.metadata)
  rescue
    exception ->
      reraise %Failure{exception: exception, stacktrace: __STACKTRACE__}, __STACKTRACE__
  end

  ## Terminal actions — see section 6

  defp resolve(state, %{"id" => id} = row, failure, attempts) do
    case state.config.on_failure do
      :crash ->
        1
        |> DMEx.backoff(state.config.reconnect_backoff_base, state.config.reconnect_backoff_max)
        |> Process.sleep()

        reraise(failure.exception, failure.stacktrace)

      mode ->
        transaction!(state, fn ->
          if mode == :dead_message, do: record_dead!(state, row)

          advance!(state, id)
        end)

        report(state, row, failure, attempts, mode)

        {:ok, %{state | cursor: id, pending: false}}
    end
  end

  defp record_dead!(state, row) do
    dead = Map.put(row, "subscription", state.subscription)

    Storage.record_dead_message(state.storage, dead)
  end

  defp report(state, %{"id" => id, "type" => type}, failure, attempts, mode) do
    Logger.error(fn ->
      "DMEx #{state.subscription} (#{inspect(state.handler)}) gave up on message #{id} " <>
        "(#{type}) from #{inspect(state.origin)} after #{attempts} " <>
        "attempt(s): #{Exception.message(failure)} — on_failure: #{mode}"
    end)

    :telemetry.execute(
      [:dmex, :handler, :failed],
      %{id: id, attempts: attempts},
      %{
        subscription: state.subscription,
        handler: state.handler,
        type: type,
        origin: state.origin,
        on_failure: mode,
        reason: Exception.message(failure)
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

  # `{:error, reason}` is storage saying *it* failed (section 4), not a verdict on
  # the message. Raised rather than returned because no caller would do anything
  # different with it, and untagged because that is exactly what makes the retry
  # schedule pass it by. It names the subscription, since a crash log is where it
  # will be read.
  defp transaction!(state, fun) do
    case Storage.transaction(state.storage, fun) do
      {:ok, result} ->
        result

      {:error, reason} ->
        raise "DMEx #{state.subscription}: the storage transaction did not commit: #{inspect(reason)}"
    end
  end

  defp advance!(state, last_seen) do
    Storage.advance_cursor(state.storage, state.subscription, last_seen)
  end
end
