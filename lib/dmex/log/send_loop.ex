defmodule DMEx.Log.SendLoop do
  @moduledoc """
  The log's send loop: read, send, wait, repeat.

  There is no separate catch-up mode and no distinction between a "catching up"
  and a "live tailing" connection — the same loop, differing only in whether it
  finds rows waiting when it reads. A consumer far behind the tip finds a full
  `max_batch_rows` on every read and writes batches back-to-back until it
  drains.

  A selective connection is not a distinct mode either: its read carries one
  extra predicate, the type list its `DMEx.Log.Selection` resolved to at connect.

  This module reads, paces, tracks the cursor and holds the deadlines; it never
  frames anything and never touches a socket (`DMEx.Transport` is the other half).

  It does nothing to a row: what a read returns is what a transport writes.

  Memory per connection is one batch and reads are gated on sends completing, so
  a stalled reader stalls its own loop and nothing else — ARCHITECTURE's
  "producer memory is one batch" and "reads are gated on sends". The cursor is
  the only source of truth.

  ## Every step that can wedge is taken under a deadline

  Three of them: the read, and either write. Each arms `stall_timeout` in this
  connection's `DMEx.Log.Status` first, and `DMEx.Log.Watcher` enforces it from
  outside.

  The idle wait and the throttle **disarm** instead. Both are self-terminating —
  a `receive` with an `after`, and a `Process.sleep/1` — so neither can wedge, and
  a deadline over one is a budget for a wait that already has its own. They used to
  arm `stall_timeout` plus the interval they were waiting out, which was a second
  arithmetic on top of `DMEx.Config.effective_heartbeat/1` and did not agree with
  it.

  ## Pacing

  The loop is sequential, so `max_batch_frequency`, `max_batch_rows` and
  `max_batch_bytes` compose: it reads at most `max_batch_rows` rows or
  `max_batch_bytes`, `max_batch_frequency` times a second. `DMEx.Config` says
  what each caps, and ARCHITECTURE's "pacing has one owner" why the producer owns
  all three.
  """

  alias DMEx.Log.Selection
  alias DMEx.Log.Status
  alias DMEx.Log.Streams
  alias DMEx.Storage
  alias DMEx.Transport

  require Logger

  @typedoc """
  Everything the loop carries between iterations.

  The log's settings are carried as `config:` rather than copied out field by
  field: they are read once per *read*, never per row, and a second spelling of
  `t:DMEx.Config.log/0` is a second place for a setting to be added and forgotten.
  Only `batch_interval` is here in its own right, because it is derived — the
  reciprocal of `max_batch_frequency`, taken once rather than per iteration.
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

  Called inline from whichever process owns the sink — a web server's request
  process for `DMEx.Transport.SSE`, since there the request *is* the stream, or
  the reader process `DMEx.Transport.Local` starts. Either way the loop is
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

  The whole connect ceremony, in one call because it is one thing happening: the
  registration, the cursor `position` resolves to, the
  `[:dmex, :stream, :connect]` event and the state. Both transports come through
  here, so neither can register differently, resolve "from now on" differently, or
  report a connect the other would report differently — and there is no `:status`
  option because registering is what produces one.

  Registering first is deliberate: it is before the first read, so an append
  landing in between is picked up by that read, and before anything that can
  block, so resolving the cursor is under `DMEx.Log.Watcher` like every other
  query. `{:error, reason}` means the cursor could not be resolved; the entry is
  registered by then, and unregistering it is the caller's — `DMEx.Log.Plug` does
  it on every path out.

  ## Options

    * `:transport` — required; the `DMEx.Transport` this loop writes through.
    * `:position` — `:now` for the log's tip, or a cursor. Defaults to `0`, the
      whole log.
    * `:selection` — the message types to read, as `t:DMEx.Log.Selection.t/0`.
  """
  @spec start(module(), keyword()) :: {:ok, state()} | {:error, term()}
  def start(log, opts) do
    selection = Keyword.get(opts, :selection, :all)
    config = log.__dmex__().config

    {:ok, status} = Streams.register(log, config.stall_timeout, selection)

    with {:ok, cursor} <- resolve_cursor(log, Keyword.get(opts, :position, 0)) do
      {:ok, connect(log, Keyword.fetch!(opts, :transport), cursor, selection, status)}
    end
  end

  @doc false
  @spec connect(module(), module(), non_neg_integer(), Selection.t(), Status.t()) :: state()
  def connect(log, transport, cursor, selection, status) do
    %{storage: storage, config: config} = log.__dmex__()

    :telemetry.execute(
      [:dmex, :stream, :connect],
      %{cursor: cursor, matched: Selection.size(selection)},
      %{log: log, selective: Selection.selective?(selection)}
    )

    %{
      log: log,
      transport: transport,
      storage: storage,
      cursor: cursor,
      config: config,
      batch_interval: DMEx.Config.batch_interval(config),
      last_sent_at: nil,
      status: status,
      # Fixed for the connection's lifetime: a consumer wanting a different
      # selection asks for a different resource, which is a different URL.
      types: Selection.to_list(selection)
    }
  end

  @doc """
  Resolves what a connect asked for into the cursor the loop takes.

  `:now` is the log's tip, which is the log's own to name and costs a query;
  anything else is already a position.

  An error is returned rather than fallen back from: 0 would turn a momentary
  pool exhaustion into a whole-log replay, and since every subscription with no
  cursor yet arrives here that is the default path, not an exotic one. What to
  do about it is the caller's — a 503, or an exit its subscription retries
  after — and both cost one jittered reconnect.
  """
  @spec resolve_cursor(module(), :now | non_neg_integer()) :: {:ok, non_neg_integer()} | {:error, term()}
  def resolve_cursor(log, :now), do: Storage.max_id(log.__dmex__().storage)

  def resolve_cursor(_log, cursor) when is_integer(cursor), do: {:ok, cursor}

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
