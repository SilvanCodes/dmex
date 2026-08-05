defmodule DMEx.Log.Stream do
  @moduledoc """
  The log's send loop: read, send, wait, repeat.

  There is no separate catch-up mode and no distinction between a "catching up"
  and a "live tailing" connection — the same loop, differing only in whether it
  finds rows waiting when it reads. A consumer far behind the tip finds a full
  `max_batch_size` on every read and writes batches back-to-back until it
  drains.

  A selective connection is not a distinct mode either: its read carries one
  extra predicate, the type list its `DMEx.Log.Selection` resolved to at connect.

  This module reads, paces, tracks the cursor and holds the deadlines; it never
  frames anything and never touches a socket. `c:DMEx.Transport.write/2` takes a
  whole read's rows and reports whether the sink is still there.

  It does nothing to a row: what a read returns is what a transport writes.

  Memory per connection is one batch and reads are gated on sends completing, so
  a stalled reader stalls its own loop and nothing else — ARCHITECTURE
  invariants 2 and 3. The cursor is the only source of truth.

  ## Every blocking step is taken under a deadline

  Before each step that can block — the read, either write, the idle wait, the
  throttle — the loop arms a deadline in its `DMEx.Log.Status`, and
  `DMEx.Log.Watcher` kills it if the step does not clear. Each budget is
  `stall_timeout` plus however long that step is *expected* to take, so a wait
  doing its job is never mistaken for one that is stuck.

  ## Pacing

  `max_batch_frequency` caps how often this loop reads; `max_batch_size` and
  `max_batch_bytes` cap what one read returns. The loop is sequential, so
  together they cap the connection at `max_batch_size` rows or
  `max_batch_bytes`, `max_batch_frequency` times a second. All three are log
  configuration (ARCHITECTURE invariant 9).
  """

  alias DMEx.Log.Selection
  alias DMEx.Log.Status

  require Logger

  @typedoc """
  Everything the loop carries between iterations.

  The log's settings are carried as `config:` rather than copied out field by
  field: they are read once per *read*, never per row, and a second spelling of
  `DMEx.Config.log/0` is a second place for a setting to be added and forgotten.
  Only `batch_interval` is here in its own right, because it is derived — the
  reciprocal of `max_batch_frequency`, taken once rather than per iteration.
  """
  @type state :: %{
          required(:storage) => module(),
          required(:storage_opts) => DMEx.Storage.opts(),
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
  @spec loop(DMEx.Transport.sink(), state()) :: DMEx.Transport.sink()
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
    arm(state, state.config.heartbeat_ms)

    receive do
      :wakeup -> false
    after
      state.config.heartbeat_ms -> true
    end
  end

  @doc """
  Resolves what a connect asked for into a cursor to read from.

  The two spellings of a position both arrive here: `DMEx.Log.Params` tags an
  instant `{:at, at}` so a caller cannot mistake it for a cursor, and
  `DMEx.Handler.Request.position/2` — which is where the id form's `- 1` lives,
  for both transports — returns one bare. They mean the same thing, and the
  query that answers it is the same query, so both transports ask it here.

  An error is fatal to the connect rather than a fallback to 0. Falling back
  would turn a pool timeout into a whole-log replay — and since every
  `stream_from: :current` subscription resolves an instant, that is the default
  path, not an exotic one. `DMEx.Log.Plug` answers it with a 503; a local
  subscription retries on its own jittered schedule. Either costs one reconnect.
  """
  @spec resolve_cursor(module(), non_neg_integer() | DateTime.t() | {:at, DateTime.t()}) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def resolve_cursor(log, {:at, at}), do: resolve_cursor(log, at)

  def resolve_cursor(log, %DateTime{} = at) do
    %{storage: storage, storage_opts: storage_opts} = log.__dmex__()

    storage.cursor_at(storage_opts, at)
  end

  def resolve_cursor(_log, cursor) when is_integer(cursor), do: {:ok, cursor}

  @doc """
  Announces a connect and builds the loop's state for it.

  One function rather than two because the `[:dmex, :stream, :connect]` event
  and the loop it announces describe the same connection: both transports get
  the measurements and the metadata from here, so neither can report a connect
  the other would report differently.

  Registration is deliberately *not* here — the two paths make the same
  `DMEx.Log.Streams.register/3` call at different moments. `DMEx.Log.Plug`
  registers before resolving its cursor, so `DMEx.Log.Watcher` covers that query
  too; `DMEx.Transport.Local` registers inside the reader process, which is the
  process that must own the entry.

  ## Options

    * `:transport` — required; the `DMEx.Transport` this loop writes through.
    * `:cursor` — where to resume from. Defaults to `0`, the whole log.
    * `:status` — this connection's `DMEx.Log.Status`, or `nil` for a loop no
      watcher is holding a deadline against.
    * `:selection` — the message types to read, as `t:DMEx.Log.Selection.t/0`.
  """
  @spec connect(module(), keyword()) :: state()
  def connect(log, opts) do
    %{storage: storage, storage_opts: storage_opts, config: config} = log.__dmex__()

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
      storage_opts: storage_opts,
      cursor: cursor,
      config: config,
      # The reciprocal of `max_batch_frequency`, taken once rather than per
      # iteration: the operator chooses a rate, the loop sleeps out a period.
      batch_interval: DMEx.Config.batch_interval(config),
      last_sent_at: nil,
      status: Keyword.get(opts, :status),
      # Fixed for the connection's lifetime: a consumer wanting a different
      # selection asks for a different resource, which is a different URL.
      types: Selection.to_list(selection)
    }
  end

  ## The loop's steps

  defp read_and_send(conn, state) do
    arm(state)

    read =
      state.storage.read(
        state.storage_opts,
        state.cursor,
        state.config.max_batch_size,
        state.config.max_batch_bytes,
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

  # A whole read's rows in one call, so syscalls and deadline arming do not
  # multiply by `max_batch_size`.
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

  # Measured from the last frame *sent*, so time parked in `await_wakeup/1` does
  # not count against the interval. A no-op on an idle stream.
  defp throttle(%{last_sent_at: nil}), do: :ok

  defp throttle(state) do
    remaining = state.last_sent_at + state.batch_interval - System.monotonic_time(:millisecond)

    if remaining > 0 do
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

  # Every write goes through here, so every write is under a deadline.
  defp write(conn, state, rows) do
    arm(state)

    state.transport.write(conn, rows)
  end

  # `stall_timeout` is the budget for the blocking itself; `extra` is however
  # long the step is legitimately expected to sit there first.
  defp arm(state, extra \\ 0)

  defp arm(%{config: %{stall_timeout: :infinity}} = state, _extra) do
    Status.arm(state.status, :infinity)
  end

  defp arm(state, extra), do: Status.arm(state.status, state.config.stall_timeout + extra)
end
