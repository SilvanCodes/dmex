defmodule DMEx.Log.SendLoop do
  @moduledoc """
  The log's send loop: read, send, wait, repeat.

  There is no catch-up mode: a consumer far behind the tip is this same loop
  finding a full `max_batch_rows` on every read, and a selective connection is
  this same loop with one extra predicate on its read. This module reads, paces,
  tracks the cursor and holds the deadlines; framing and sockets are
  `DMEx.Transport`'s, a row goes out exactly as the read returned it, and
  ARCHITECTURE's "producer memory is one batch", "reads are gated on sends" and
  "pacing has one owner" are what the loop's shape buys — `DMEx.Config` says what
  each bound caps.

  ## Every step that can wedge is taken under a deadline

  Three of them: the read, and either write. Each arms `stall_timeout` in this
  connection's `DMEx.Log.Status` first, and `DMEx.Log.Watcher` enforces it from
  outside. The idle wait and the throttle **disarm** instead: both are
  self-terminating — a `receive` with an `after`, and a `Process.sleep/1` — so a
  deadline over either would be a second, disagreeing expression of
  `DMEx.Config.effective_heartbeat/1`.
  """

  alias DMEx.Log.Selection
  alias DMEx.Log.Status
  alias DMEx.Log.Streams
  alias DMEx.Storage
  alias DMEx.Transport

  require Logger

  @typedoc """
  Everything the loop carries between iterations.

  The log's settings ride as `config:` rather than copied out field by field, so
  there is no second spelling of `t:DMEx.Config.log/0` to add a setting to and
  forget. `batch_interval` is the one derived value here, taken once per
  connection rather than per iteration.
  """
  @type state :: %{
          required(:storage) => Storage.opts(),
          required(:log) => module(),
          required(:transport) => module(),
          required(:cursor) => non_neg_integer(),
          required(:config) => DMEx.Config.log(),
          required(:batch_interval) => non_neg_integer(),
          required(:last_sent_at) => integer() | nil,
          required(:status) => Status.t(),
          required(:types) => :all | [String.t()]
        }

  @doc """
  Runs the send loop until the sink closes, then returns it.

  Called inline from whichever process owns the sink — the request process for
  `DMEx.Transport.SSE`, the reader for `DMEx.Transport.Local` — so the loop is
  sequential and blocking, which is what makes reads gated on sends.
  """
  @spec loop(Transport.sink(), state()) :: Transport.sink()
  def loop(conn, state) do
    # Both cleared *before* the read, never after. An append landing between the
    # clear and the read is picked up by that read; one landing after re-sets
    # the flag, so the next `await_wakeup/1` returns immediately.
    drain_wakeups()
    Status.clear(state.status)

    case read_and_send(conn, state) do
      {:sent, conn, state} ->
        # Work in hand: keep draining rather than waiting on a wakeup.
        throttle(state)
        loop(conn, state)

      {:empty, conn, state} ->
        # Nothing to send, so block until something is appended or the heartbeat
        # interval expires. Reading *before* this wait lets a freshly-connected
        # consumer start draining a backlog immediately.
        case maybe_heartbeat(conn, state, await_wakeup(state)) do
          {:ok, conn} ->
            throttle(state)
            loop(conn, state)

          {:closed, conn} ->
            conn
        end

      {:closed, conn} ->
        conn
    end
  end

  # True when the wait expired with no append — the stream is idle and owes the
  # consumer a liveness signal. Self-terminating, so it runs with no deadline at
  # all: `after` is its own budget.
  defp await_wakeup(state) do
    Status.disarm(state.status)

    receive do
      :wakeup -> false
    after
      state.config.heartbeat_interval -> true
    end
  end

  @doc """
  Registers this process as one of `log`'s streams and builds the loop's state.

  Registering comes first: before the first read, so an append landing in between
  is picked up by it, and before anything that can block, so the cursor query is
  under `DMEx.Log.Watcher`. `{:error, reason}` leaves the entry registered for the
  caller to drop.

  ## Options

    * `:transport` — required; the `DMEx.Transport` this loop writes through.
    * `:position` — `:now` for the log's tip, or a cursor. Defaults to `0`, the
      whole log.
    * `:selection` — the message types to read, as `t:DMEx.Log.Selection.t/0`.
  """
  @spec start(module(), keyword()) :: {:ok, state()} | {:error, term()}
  def start(log, opts) do
    selection = Keyword.get(opts, :selection, :all)
    %{storage: storage, config: config} = log.__dmex__()

    {:ok, status} = Streams.register(log, config.stall_timeout, selection)

    with {:ok, cursor} <- resolve_cursor(storage, Keyword.get(opts, :position, 0)) do
      :telemetry.execute(
        [:dmex, :stream, :connect],
        %{cursor: cursor, matched: Selection.size(selection)},
        %{log: log, selective: Selection.selective?(selection)}
      )

      {:ok,
       %{
         log: log,
         transport: Keyword.fetch!(opts, :transport),
         storage: storage,
         cursor: cursor,
         config: config,
         batch_interval: DMEx.Config.batch_interval(config),
         last_sent_at: nil,
         status: status,
         # Fixed for the connection's lifetime: a consumer wanting a different
         # selection asks for a different resource, which is a different URL.
         types: Selection.to_list(selection)
       }}
    end
  end

  # `:now` is the log's tip, which costs a query. An error is returned rather
  # than fallen back from: 0 would turn a momentary pool exhaustion into a
  # whole-log replay, and every subscription with no cursor yet arrives here, so
  # that is the default path rather than an exotic one.
  defp resolve_cursor(storage, :now), do: Storage.max_id(storage)

  defp resolve_cursor(_storage, cursor) when is_integer(cursor), do: {:ok, cursor}

  ## The loop's steps

  defp read_and_send(conn, state) do
    arm(state)

    case Storage.read(
           state.storage,
           state.cursor,
           state.config.max_batch_rows,
           state.config.max_batch_bytes,
           state.types
         ) do
      {:ok, []} ->
        {:empty, conn, state}

      {:ok, rows} ->
        send_rows(conn, state, rows)

      {:error, reason} ->
        # If the log cannot be read there is nothing to send and nothing to say
        # on the wire. Dropping the connection lets the consumer reconnect from
        # its durable cursor, the same recovery path every failure takes.
        Logger.warning("DMEx stream read failed for #{inspect(state.log)}: #{inspect(reason)}")

        {:closed, conn}
    end
  end

  # A whole read's rows in one call, so syscalls and deadline arming do not
  # multiply by `max_batch_rows`.
  defp send_rows(conn, state, rows) do
    last_id = List.last(rows).id

    case write(conn, state, rows) do
      {:ok, conn, measurements} ->
        :telemetry.execute(
          [:dmex, :stream, :write],
          Map.put(measurements, :rows, length(rows)),
          %{log: state.log}
        )

        {:sent, conn, %{state | cursor: last_id, last_sent_at: System.monotonic_time(:millisecond)}}

      {:error, _reason} ->
        {:closed, conn}
    end
  end

  # Rows of its own are a stream's liveness signal, so a heartbeat is owed only
  # when the wait expired with nothing to send.
  defp maybe_heartbeat(conn, _state, false), do: {:ok, conn}

  defp maybe_heartbeat(conn, state, true) do
    arm(state)

    case Transport.heartbeat(state.transport, conn) do
      {:ok, conn} ->
        :telemetry.execute([:dmex, :stream, :heartbeat], %{}, %{log: state.log})

        {:ok, conn}

      {:error, _reason} ->
        {:closed, conn}
    end
  end

  # Measured from the last frame *sent*, so time parked in `await_wakeup/1` does
  # not count against the interval. A no-op on an idle stream, and self-terminating
  # when it is not, so it runs with no deadline.
  defp throttle(%{last_sent_at: nil}), do: :ok

  defp throttle(state) do
    remaining = state.last_sent_at + state.batch_interval - System.monotonic_time(:millisecond)

    if remaining > 0 do
      Status.disarm(state.status)
      Process.sleep(remaining)
    end
  end

  defp drain_wakeups do
    receive do
      :wakeup -> drain_wakeups()
    after
      0 -> :ok
    end
  end

  # Every write goes through here, so every write is under a deadline.
  # `Status.arm/2` takes `:infinity` itself, so a log that runs no watcher needs no
  # branch here.
  defp write(conn, state, rows) do
    arm(state)

    Transport.write(state.transport, conn, rows)
  end

  defp arm(state), do: Status.arm(state.status, state.config.stall_timeout)
end
