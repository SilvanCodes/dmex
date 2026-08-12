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
  a stalled reader stalls its own loop and nothing else — ARCHITECTURE
  invariants 2 and 3. The cursor is the only source of truth.

  ## Every blocking step is taken under a deadline

  The read, either write, the idle wait and the throttle each arm a deadline in
  this connection's `DMEx.Log.Status` first; `DMEx.Log.Watcher` enforces it from
  outside. Each budget is `stall_timeout` plus however long that step is
  *expected* to take, so a wait doing its job is never mistaken for one that is
  stuck.

  ## Pacing

  The loop is sequential, so `max_batch_frequency`, `max_batch_rows` and
  `max_batch_bytes` compose: it reads at most `max_batch_rows` rows or
  `max_batch_bytes`, `max_batch_frequency` times a second. `DMEx.Config` says
  what each caps, and ARCHITECTURE invariant 9 why the producer owns all three.
  """

  alias DMEx.Log.Selection
  alias DMEx.Log.Status
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
          required(:status) => Status.t() | nil,
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
  # consumer a liveness signal. The one step whose whole purpose is to block, so
  # its budget carries the interval it is waiting out.
  defp await_wakeup(state) do
    arm(state, state.config.heartbeat_interval)

    receive do
      :wakeup -> false
    after
      state.config.heartbeat_interval -> true
    end
  end

  @doc """
  Announces a connect and builds the loop's state for it.

  One function rather than two because the `[:dmex, :stream, :connect]` event
  and the loop it announces describe the same connection: both transports get
  the measurements and the metadata from here, so neither can report a connect
  the other would report differently.

  Registration is deliberately *not* here: both paths make the same
  `DMEx.Log.Streams.register/3` call, but each at a moment only it can pick —
  that function and `DMEx.Log.Plug` say why.

  ## Options

    * `:transport` — required; the `DMEx.Transport` this loop writes through.
    * `:cursor` — where to resume from. Defaults to `0`, the whole log.
    * `:status` — this connection's `DMEx.Log.Status`, or `nil` for a loop no
      watcher is holding a deadline against.
    * `:selection` — the message types to read, as `t:DMEx.Log.Selection.t/0`.
  """
  @spec connect(module(), keyword()) :: state()
  def connect(log, opts) do
    %{storage: storage, config: config} = log.__dmex__()

    cursor = Keyword.get(opts, :cursor, 0)
    selection = Keyword.get(opts, :selection, :all)

    :telemetry.execute(
      [:dmex, :stream, :connect],
      %{cursor: cursor, matched: Selection.size(selection)},
      %{log: log, selective: Selection.selective?(selection)}
    )

    %{
      log: log,
      transport: Keyword.fetch!(opts, :transport),
      storage: storage,
      cursor: cursor,
      config: config,
      batch_interval: DMEx.Config.batch_interval(config),
      last_sent_at: nil,
      status: Keyword.get(opts, :status),
      # Fixed for the connection's lifetime: a consumer wanting a different
      # selection asks for a different resource, which is a different URL.
      types: Selection.to_list(selection)
    }
  end

  @doc """
  Resolves what a connect asked for into the cursor `connect/2` takes.

  `:now` is the log's tip, which is the log's own to name and costs a query;
  anything else is already a position. Both connect paths come through here —
  `DMEx.Log.Plug` for a request's `Last-Event-ID`, `DMEx.Transport.Local` for a
  subscription's durable cursor — so "from now on" cannot come to mean two
  things.

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
  # not count against the interval. A no-op on an idle stream.
  defp throttle(%{last_sent_at: nil}), do: :ok

  defp throttle(state) do
    remaining = state.last_sent_at + state.batch_interval - System.monotonic_time(:millisecond)

    if remaining > 0 do
      arm(state, remaining)
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

  ## Deadlines

  # Every write goes through here, so every write is under a deadline.
  defp write(conn, state, rows) do
    arm(state)

    Transport.write(state.transport, conn, rows)
  end

  # `stall_timeout` is the budget for the blocking itself; `extra` is however
  # long the step is legitimately expected to sit there first.
  defp arm(state, extra \\ 0)

  defp arm(%{config: %{stall_timeout: :infinity}} = state, _extra), do: Status.arm(state.status, :infinity)

  defp arm(state, extra), do: Status.arm(state.status, state.config.stall_timeout + extra)
end
