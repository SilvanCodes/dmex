defmodule DMEx.Log.Stream do
  @moduledoc """
  The log's send loop: read, send, wait, repeat.

  There is no separate catch-up mode and no distinction between a "catching up"
  and a "live tailing" connection — the same loop, differing only in whether it
  finds rows waiting when it reads. A consumer far behind the tip finds a full
  `max_batch_size` on every read and writes batches back-to-back until it
  drains.

  A selective connection is not a distinct mode either: its read carries one
  extra predicate, the type list `DMEx.Log.Plug` expanded from its filter at
  connect. What it sees is a *sparse subset of the same id space*, never a space
  of its own, so every consumer's cursor stays comparable with every other's.

  ## What is done with the rows is the transport's

  This module reads, paces, tracks the cursor and holds the deadlines. It never
  frames anything and never touches a socket: `c:DMEx.Transport.write/2` takes a
  whole read's rows and reports whether the sink is still there. Batching is
  therefore a *write* batch, not a wire construct — nothing about it reaches the
  reader, which sees a plain run of one-row frames and cannot tell how many
  arrived together, so there is no reader-side configuration to keep in step.

  ## Four properties, and they are the whole reason for this shape

    * **Memory per connection is one batch**, in bytes and not only in rows.
      There is no accumulator, and the read is bounded both ways inside the
      query. See `c:DMEx.Storage.read/5`.
    * **Reads are gated on sends completing**, so producer-side overload is
      unrepresentable rather than detected and handled.
    * **A stalled reader stalls its own loop and nothing else.** It reads no
      more rows, allocates no more memory, and issues no more queries.
    * **The cursor is the only source of truth.** A wakeup is a hint that work
      may exist, never a carrier of work.

  ## Every blocking step is taken under a deadline

  The loop cannot reclaim itself: a process blocked in a write is not running.
  So before each step that can block — the read, either write, the idle wait,
  the throttle — it arms a deadline in its `DMEx.Log.Status`, and
  `DMEx.Log.Watcher` kills it if the step does not clear. Each budget is
  `stall_timeout` plus however long that step is *expected* to take, so a wait
  doing its job is never mistaken for one that is stuck.

  ## Pacing

  `max_batch_frequency` caps how often this loop goes back to the database;
  `max_batch_size` and `max_batch_bytes` cap what one read brings back. The loop
  is sequential, so together they cap this connection at `max_batch_size` rows
  or `max_batch_bytes` of payload, `max_batch_frequency` times a second.

  The first protects something the log shares: a hundred connections at the
  default five reads per second is five hundred short range scans a second
  against the pool, whatever the append rate does — which is why the ceiling is
  a rate rather than a reaction to load. **All three are the log's, and only the
  log's.** A reader asks for nothing and is told nothing.
  """

  alias DMEx.Log.Status

  require Logger

  @typedoc "Everything the loop carries between iterations."
  @type state :: %{
          required(:storage) => module(),
          required(:storage_opts) => DMEx.Storage.opts(),
          required(:log) => module(),
          required(:transport) => module(),
          required(:cursor) => non_neg_integer(),
          required(:batch_interval) => non_neg_integer(),
          required(:max_batch_size) => pos_integer(),
          required(:heartbeat_ms) => pos_integer(),
          required(:max_batch_bytes) => pos_integer(),
          required(:stall_timeout) => Status.budget(),
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
  @spec loop(DMEx.Transport.sink(), state()) :: DMEx.Transport.sink()
  def loop(conn, state) do
    # Both cleared *before* the read, never after. An append landing between the
    # clear and the read is picked up by that read; one landing after re-sets
    # the flag, so the next `await_wakeup/1` returns immediately.
    drain_wakeups()
    Status.clear(state.status)

    case read_and_send(conn, state) do
      {:sent, conn, state} ->
        # Work in hand: keep draining at `max_batch_frequency` rather than
        # waiting on a wakeup. This is what makes catch-up the ordinary loop
        # running hot rather than a distinct mode.
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
    arm(state, state.heartbeat_ms)

    receive do
      :wakeup -> false
    after
      state.heartbeat_ms -> true
    end
  end

  @doc """
  Builds the loop's state for one connection.

  ## Options

    * `:transport` — required; the `DMEx.Transport` this loop writes through.
    * `:cursor` — where to resume from. Defaults to `0`, the whole log.
    * `:status` — this connection's `DMEx.Log.Status`, or `nil` for a loop no
      watcher is holding a deadline against.
    * `:selection` — the message types to read, as `t:DMEx.Log.Registry.selection/0`.
  """
  @spec initial_state(module(), keyword()) :: state()
  def initial_state(log, opts) do
    %{storage: storage, storage_opts: storage_opts, config: config} = log.__dmex__()

    %{
      log: log,
      transport: Keyword.fetch!(opts, :transport),
      storage: storage,
      storage_opts: storage_opts,
      cursor: Keyword.get(opts, :cursor, 0),
      # The reciprocal of `max_batch_frequency`, taken once rather than per
      # iteration: the operator chooses a rate, the loop sleeps out a period.
      batch_interval: DMEx.Config.batch_interval(config),
      max_batch_size: config.max_batch_size,
      heartbeat_ms: config.heartbeat_ms,
      max_batch_bytes: config.max_batch_bytes,
      stall_timeout: config.stall_timeout,
      last_sent_at: nil,
      status: Keyword.get(opts, :status),
      # Fixed for the connection's lifetime: a consumer wanting a different
      # selection asks for a different resource, which is a different URL. A
      # list rather than the registry's `MapSet`, because this is a query
      # parameter now, not a membership test.
      types: types(Keyword.get(opts, :selection, :all))
    }
  end

  defp types(:all), do: :all
  defp types(selection), do: MapSet.to_list(selection)

  ## The loop's steps

  defp read_and_send(conn, state) do
    # A read is a blocking step like any other: a pool checkout that never
    # returns wedges this loop exactly as a stalled socket does — and unlike a
    # stalled socket, no transport-level timeout can see it.
    arm(state)

    read =
      state.storage.read(
        state.storage_opts,
        state.cursor,
        state.max_batch_size,
        state.max_batch_bytes,
        state.types
      )

    case read do
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

  # A whole read's rows in one call: writing them individually would multiply
  # the syscalls and the deadline arming by `max_batch_size` for rows going out
  # together anyway. Nothing is trimmed here — the read came back already
  # bounded in bytes as well as rows (see `c:DMEx.Storage.read/5`), and trimming
  # here would mean the surplus had already been materialised.
  defp send_rows(conn, state, rows) do
    last_id = rows |> List.last() |> Map.fetch!(:id)

    case write(conn, state, rows) do
      {:ok, conn, measurements} ->
        :telemetry.execute(
          [:dmex, :stream, :write],
          Map.put(measurements, :rows, length(rows)),
          %{log: state.log}
        )

        {:sent, conn,
         %{state | cursor: last_id, last_sent_at: System.monotonic_time(:millisecond)}}

      {:error, _reason} ->
        {:closed, conn}
    end
  end

  # Rows of its own are a stream's liveness signal, so a heartbeat is owed only
  # when the wait expired with nothing to send.
  defp maybe_heartbeat(conn, _state, false), do: {:ok, conn}

  defp maybe_heartbeat(conn, state, true) do
    arm(state)

    case state.transport.heartbeat(conn) do
      {:ok, conn} ->
        :telemetry.execute([:dmex, :stream, :heartbeat], %{}, %{log: state.log})

        {:ok, conn}

      {:error, _reason} ->
        {:closed, conn}
    end
  end

  # Measured from the last frame *sent*, not from the top of the iteration. The
  # interval is a floor on the gap between writes, and measuring it from the top
  # would include time parked in `await_wakeup/1` — so a message on a quiet
  # stream would wait out a whole interval *after* the wakeup announcing it,
  # making end-to-end latency a full `1 / max_batch_frequency` rather than the
  # half the design expects. On an idle stream this is a no-op.
  defp throttle(%{last_sent_at: nil}), do: :ok

  defp throttle(state) do
    remaining = state.last_sent_at + state.batch_interval - System.monotonic_time(:millisecond)

    if remaining > 0 do
      # Budgeted for the sleep it is about to take, so the watcher measures
      # against what the throttle is *for*.
      arm(state, remaining)

      Process.sleep(remaining)
    else
      :ok
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

  # Every write goes through here, so every write is under a deadline the
  # watcher can see. This is the step the whole mechanism exists for: a reader
  # that stops taking rows blocks this call with no way to time itself out.
  defp write(conn, state, rows) do
    arm(state)

    state.transport.write(conn, rows)
  end

  # `stall_timeout` is the budget for the blocking itself; `extra` is however
  # long the step is legitimately expected to sit there first.
  defp arm(state, extra \\ 0)

  defp arm(%{stall_timeout: :infinity} = state, _extra), do: Status.arm(state.status, :infinity)

  defp arm(state, extra), do: Status.arm(state.status, state.stall_timeout + extra)
end
