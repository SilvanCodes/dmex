defmodule DMEx.Handler.Pipeline do
  @moduledoc """
  Processes one row: suppress, resolve, decode, handle, advance.

  Every handler-side semantic lives here, and the order of operations *is* the
  specification.

  ## 1. Duplicate suppression, by id

  A row whose `id` is at or below the cursor was already committed, so it is
  dropped before any transaction opens. This needs no dedupe table: delivery
  within a subscription is strictly ordered and `last_seen` is durable, so
  "already processed" is exactly "id not greater than the cursor". It cannot
  cover a crash between a handler's side effects landing and its transaction
  committing, because it is that commit that records the message as processed.

  ## 2. An unknown message type is skipped, not an error

  An unknown type is **not** a fault: a filter false positive admits one, and a
  handler declared `selective: false` asks for the whole log outright. Such a
  row advances the cursor exactly as a handled one does — it must, or a handler
  would wedge forever on the first type it does not know — and emits
  `[:dmex, :handler, :skip]`.

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

  ## 5. The handler and the cursor share one transaction

  `handle_message/2` succeeding is what advances `last_seen`, both in one
  transaction. A raise rolls both back and propagates.

  ## 6. `on_failure` is the terminal action, and there are three

  The retry step is identical in all three. Both non-crashing modes commit an
  **absolute** advance to the failing row's id, carrying any coalesced run of
  skips below it (section 2a).

  **`:dead_message`** (the default) advances past the failing message, writing it
  to `dmex_dead_messages` first, in the same transaction — so there is no state
  where a message was skipped without being recorded, or recorded without being
  skipped. The default because it is the only mode refusing both bad outcomes:
  one message a handler cannot process is not a reason for every *later* message
  to stop arriving, nor a reason to lose the failing one. What is stored is the
  message and only the message — see `DMEx.Storage.Migration`.

  **`:crash`** re-raises with the original stacktrace and without advancing, so a
  pending run of skips is re-skipped after the restart. The supervisor restarts
  the stream, it re-reads its persisted cursor — still pointing at the message
  *before* this one — and the log's range scan re-streams the message that
  killed it, forever. Choose it when later messages must not be processed
  without this one, or when a loud alarm beats a growing table nobody watches.
  Reconnect backoff keeps the loop off the log's database and off its siblings;
  `DMEx.Handler.Supervisor` deliberately sets no restart budget.

  **`:skip`** advances past the failing message without the handler having
  succeeded. Nothing durable is recorded: the log line and
  `[:dmex, :handler, :failed]` are the only trace.
  """

  alias DMEx.Handler

  require Logger

  @typedoc """
  One row as a transport yields it: string-keyed, with `data` and `metadata`
  decoded one level.

  Turning `data` into a message struct is the message module's job, here, per
  row — so a transport produces the same shape whether it parsed a frame or read
  the columns directly.
  """
  @type row :: %{String.t() => term()}

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
          required(:modules) => %{String.t() => module()},
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
  `on_failure` says `:crash`.
  """
  @spec run(row(), t()) :: {:ok, t()}
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

  A no-op when nothing is pending — a handled message's own advance already
  covered the skips below it.
  """
  @spec flush(t()) :: {:ok, t()}
  def flush(%{pending: false} = state), do: {:ok, state}

  def flush(%{pending: true, cursor: cursor} = state) do
    commit!(state, fn -> advance!(state, cursor) end)

    :telemetry.execute(
      [:dmex, :handler, :checkpoint],
      %{id: cursor},
      %{subscription: state.subscription, handler: state.handler, origin: state.origin}
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

  defp attempt(row, id, message, metadata, state, attempt) do
    telemetry = %{
      subscription: state.subscription,
      handler: state.handler,
      message_type: Map.fetch!(row, "message_type"),
      origin: state.origin,
      id: id,
      attempt: attempt
    }

    :telemetry.span([:dmex, :handler, :message], telemetry, fn ->
      commit!(state, fn ->
        _ = state.handler.handle_message(message, metadata)

        advance!(state, id)
      end)

      {{:ok, %{state | cursor: id, pending: false}}, telemetry}
    end)
  rescue
    exception ->
      stacktrace = __STACKTRACE__

      if attempt < state.config.max_attempts do
        # Blocking here is correct rather than lazy: the stream processes one
        # message at a time, so there is nothing else it could usefully do, and
        # pausing it applies backpressure to the log while a handler struggles.
        attempt
        |> DMEx.backoff(state.config.retry_backoff, state.config.retry_backoff_max)
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

  # The transport decoded the row one level, so these are terms here and the
  # storage adapter wants JSON text.
  defp encode(nil), do: "{}"
  defp encode(value), do: JSON.encode!(value)

  defp report(state, row, id, exception, attempts, mode) do
    Logger.error(fn ->
      "DMEx #{state.subscription} (#{inspect(state.handler)}) gave up on message #{id} " <>
        "(#{Map.fetch!(row, "message_type")}) from #{inspect(state.origin)} after #{attempts} " <>
        "attempt(s): #{Exception.message(exception)} — on_failure: #{mode}"
    end)

    :telemetry.execute(
      [:dmex, :handler, :failed],
      %{id: id, attempts: attempts},
      %{
        subscription: state.subscription,
        handler: state.handler,
        message_type: Map.fetch!(row, "message_type"),
        origin: state.origin,
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
        origin: state.origin
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

  The message set was derived and validated at compile time, so there is nothing
  left for this to check but the handler itself.

  Resolving `message_type` per row is a map lookup against the resulting
  registry — never `String.to_atom/1` on whatever the log sent, which would be
  an unbounded-atom-table risk on attacker-controlled input.
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
      origin: Keyword.fetch!(opts, :origin),
      cursor: Keyword.get(opts, :cursor, 0),
      pending: false,
      config: Keyword.get_lazy(opts, :config, fn -> DMEx.Config.handler!([]) end)
    }
  end

  # `DMEx.Handler.Stream` always names the subscription it runs, since it is one
  # of possibly several. A caller that did not — a test, or a replay — only has
  # an answer when there is exactly one.
  defp subscription!(%{logs: [only], id: id}), do: only <> "/" <> id

  defp subscription!(%{logs: logs, handler: handler}) do
    raise ArgumentError,
          "#{inspect(handler)} reads #{length(logs)} logs (#{Enum.join(logs, ", ")}), so a " <>
            "pipeline for it has to be built with the `subscription:` it is running."
  end
end
