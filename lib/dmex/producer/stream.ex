defmodule DMEx.Producer.Stream do
  @moduledoc """
  The producer's send loop: read, send, wait, repeat.

  There is no separate catch-up mode, no separate endpoint for it, and no
  distinction between a "catching up" and a "live tailing" connection. They are
  the same loop; they differ only in whether it finds rows waiting when it
  reads. A consumer far behind the tip simply finds a full `max_batch_size` on
  every read and sends full frames back-to-back until it drains.

  A selective connection is not a distinct mode either. Its read carries one
  extra predicate — the type list `DMEx.Producer.Plug` expanded from its filter
  at connect — and everything else about the loop is identical. What it sees is
  a *sparse subset of the same id space*, never a space of its own: `max_id`
  stays the tip of the whole log, so every consumer's cursor stays directly
  comparable with every other's regardless of what it selected.

  ## Four properties, and they are the whole reason for this shape

    * **Memory per connection is one batch.** There is no accumulator. The
      producer never holds rows on behalf of a consumer; it holds at most the
      frame it is currently writing.

    * **Reads are gated on sends completing.** This is the key inversion. If
      reads were driven by the publisher's rate, that decoupling *would be* an
      unbounded buffer. Here the next read cannot happen until the previous
      write returns, so read rate is gated by the consumer's actual drain rate.
      Backpressure is structural rather than monitored: overload is not detected
      and handled, it is unrepresentable.

    * **A stalled consumer stalls its own loop and nothing else.** If the
      consumer stops reading, its receive window closes, `chunk/2` blocks, and
      this connection simply stops making progress. It reads no more rows,
      allocates no more memory, and issues no more queries.

    * **The cursor is the only source of truth.** A wakeup is a hint that work
      may exist, never a carrier of work. Missing one costs latency and never
      correctness, so there is no gap-versus-overlap reconciliation anywhere.

  ## Every blocking step is taken under a deadline

  What the loop cannot do is reclaim itself: a process blocked in `chunk/2`
  cannot time itself out, because it is not running. So before each step that can
  block — the read, either write, the idle wait, the throttle — it arms a
  deadline in its `DMEx.Producer.Status`, and `DMEx.Producer.Watcher` kills it if
  the step does not clear in time. Each budget is `stall_timeout` plus however
  long that particular step is *expected* to take, so a wait that is doing its
  job is never mistaken for one that is stuck.

  ## Pacing

  `batch_window` is a hard floor on frame *rate*; `max_batch_size` is a hard cap
  on frame *size*. Because the loop is sequential, neither can preempt the
  other, and together they cap this connection's work at `max_batch_size` rows
  per `batch_window`. Both are per-connection and fixed for the connection's
  lifetime — a consumer wanting different pacing reconnects, which costs it one
  read it was going to issue anyway.
  """

  alias DMEx.Producer.Status
  alias DMEx.SSE.Frame

  require Logger

  @typedoc "Everything the loop carries between iterations."
  @type state :: %{
          required(:storage) => module(),
          required(:storage_opts) => DMEx.Storage.opts(),
          required(:producer) => module(),
          required(:cursor) => non_neg_integer(),
          required(:batch_window) => pos_integer(),
          required(:max_batch_size) => pos_integer(),
          required(:heartbeat_ms) => pos_integer(),
          required(:frame_max_bytes) => pos_integer(),
          required(:stall_timeout) => Status.budget(),
          required(:last_sent_at) => integer() | nil,
          required(:status) => Status.t() | nil,
          required(:types) => :all | [String.t()]
        }

  @doc """
  Runs the send loop until the connection closes, then returns the conn.

  Called inline from the request process, because on the producer side the
  request *is* the stream: `chunk/2` works naturally, the socket closes when the
  process dies, and the web server's own supervision covers it.
  """
  @spec loop(Plug.Conn.t(), state()) :: Plug.Conn.t()
  def loop(conn, state) do
    # Both cleared *before* the read, never after. A publish landing between the
    # clear and the read is picked up by that read; one landing after the read
    # re-sets the flag and enqueues a wakeup, so the next `await_wakeup/1`
    # returns immediately. Spurious wakeups are harmless, missed ones would not
    # be, and this ordering makes them impossible.
    drain_wakeups()
    Status.clear(state.status)

    case read_and_send(conn, state) do
      {:sent, conn, state} ->
        # Work in hand: keep draining at `batch_window` pace rather than waiting
        # on a wakeup. This is what makes catch-up "the ordinary loop running
        # hot" rather than a distinct mode.
        throttle(state)
        loop(conn, state)

      {:empty, conn, state} ->
        # Nothing to send, so block until something is published or the
        # heartbeat interval expires. Reading *before* this wait is what lets a
        # freshly-connected consumer start draining a backlog immediately
        # instead of waiting out a heartbeat for its first frame.
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

  # Returns true when the wait expired with no publish — i.e. the stream is
  # genuinely idle and owes the consumer a liveness signal.
  #
  # This is the one step whose whole purpose is to block, so its budget carries
  # the interval it is waiting out. A stream parked here is idle, not stuck.
  defp await_wakeup(state) do
    arm(state, state.heartbeat_ms)

    receive do
      :wakeup -> false
    after
      state.heartbeat_ms -> true
    end
  end

  @doc false
  @spec initial_state(
          module(),
          DMEx.Producer.Params.t(),
          non_neg_integer(),
          Status.t() | nil,
          DMEx.Producer.Registry.selection()
        ) :: state()
  def initial_state(producer, params, cursor, status \\ nil, selection \\ :all) do
    %{storage: storage, storage_opts: storage_opts, config: config} = producer.__dmex__()

    %{
      producer: producer,
      storage: storage,
      storage_opts: storage_opts,
      cursor: cursor,
      batch_window: params.batch_window,
      max_batch_size: params.max_batch_size,
      heartbeat_ms: config.heartbeat_ms,
      frame_max_bytes: config.frame_max_bytes,
      stall_timeout: config.stall_timeout,
      last_sent_at: nil,
      status: status,
      # Expanded once at connect and fixed for the connection's lifetime, for
      # the same reason pacing is: a consumer wanting a different selection
      # reconnects. As a list rather than the registry's `MapSet`, because this
      # is a query parameter now, not a membership test.
      types: types(selection)
    }
  end

  defp types(:all), do: :all
  defp types(selection), do: MapSet.to_list(selection)

  ## The loop's steps

  defp read_and_send(conn, state) do
    # A read is a blocking step like any other: a pool checkout that never
    # returns, or a database that stops answering mid-query, wedges this loop
    # exactly as a stalled socket does — and unlike a stalled socket, no
    # transport-level timeout can ever see it.
    arm(state)

    case state.storage.read(state.storage_opts, state.cursor, state.max_batch_size, state.types) do
      {:ok, {[], _max_id}} ->
        {:empty, conn, state}

      {:ok, {rows, max_id}} ->
        send_frame(conn, state, rows, max_id)

      {:error, reason} ->
        # The database is the log; if it cannot be read there is nothing to send
        # and nothing to say on the wire. Dropping the connection lets the
        # consumer reconnect from its own durable cursor, which is the same
        # recovery path every other transport failure takes.
        Logger.warning(
          "DMEx stream read failed for #{inspect(state.producer)}: #{inspect(reason)}"
        )

        {:closed, conn}
    end
  end

  defp send_frame(conn, state, rows, max_id) do
    {rows, truncated?} = cap_bytes(rows, state.frame_max_bytes)
    frame = Frame.encode_batch(rows, max_id)
    last_id = rows |> List.last() |> Map.fetch!(:id)

    case chunk(conn, state, frame) do
      {:ok, conn} ->
        :telemetry.execute(
          [:dmex, :stream, :frame],
          %{rows: length(rows), bytes: IO.iodata_length(frame), lag: max_id - last_id},
          %{producer: state.producer, truncated: truncated?}
        )

        {:sent, conn,
         %{state | cursor: last_id, last_sent_at: System.monotonic_time(:millisecond)}}

      {:error, _reason} ->
        {:closed, conn}
    end
  end

  # A frame's own bytes are its liveness signal, so a heartbeat is owed only
  # when the wait expired with nothing to send.
  defp maybe_heartbeat(conn, _state, false), do: {:ok, conn}

  defp maybe_heartbeat(conn, state, true) do
    case chunk(conn, state, Frame.encode_heartbeat()) do
      {:ok, conn} ->
        :telemetry.execute([:dmex, :stream, :heartbeat], %{}, %{producer: state.producer})

        {:ok, conn}

      {:error, _reason} ->
        {:closed, conn}
    end
  end

  # Measured from the last frame *sent*, not from the top of the iteration.
  #
  # `batch_window` is a floor on the interval between frames, and measuring it
  # any other way turns it into added latency. From the top of the iteration it
  # would include the time spent parked in `await_wakeup/1` — so a message
  # arriving on a quiet stream would wait out a whole window *after* the wakeup
  # that announced it, making end-to-end latency `batch_window` rather than the
  # `batch_window / 2` the design expects. A slow read does not add to it either.
  #
  # On a stream that has been idle the last send is long past, so this is a
  # no-op and the message goes out as soon as it is read.
  defp throttle(%{last_sent_at: nil}), do: :ok

  defp throttle(state) do
    remaining = state.last_sent_at + state.batch_window - System.monotonic_time(:millisecond)

    if remaining > 0 do
      # Budgeted for the sleep it is about to take, so the watcher measures the
      # deadline against what the throttle is *for* rather than against how long
      # a window happens to be.
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

  ## Frame byte cap

  # `max_batch_size` bounds rows; this bounds bytes, which is what the socket and
  # `stall_timeout` actually see. Truncating is safe because the cursor only ever
  # advances over what was sent, so the remainder arrives on the next iteration.
  #
  # At least one row always goes out, even if that row alone exceeds the cap:
  # refusing it would wedge the stream on it permanently, and a stuck stream is
  # a worse failure than one oversized frame.
  defp cap_bytes([first | rest], max_bytes) do
    {rows, total} =
      Enum.reduce_while(rest, {[first], row_bytes(first)}, fn row, {kept, total} ->
        total = total + row_bytes(row)

        if total > max_bytes do
          {:halt, {kept, total}}
        else
          {:cont, {[row | kept], total}}
        end
      end)

    {Enum.reverse(rows), total > max_bytes}
  end

  # Deliberately an over-estimate. The cap is a ceiling, so counting a row as
  # larger than it is truncates a frame early — harmless — while counting it
  # smaller lets the frame exceed the cap, which is the failure the cap exists
  # to prevent.
  #
  # `message_type` is included because it is a fully-qualified module name,
  # routinely 30-50 bytes; leaving it out under-counted a wide batch by enough
  # to overshoot. The constant covers the rest of the row's JSON scaffolding:
  # the four key names, quoting, separators, the id, and a 27-byte ISO 8601
  # timestamp, which comes to about 110 bytes.
  @row_overhead 128

  defp row_bytes(row) do
    byte_size(row.data) + byte_size(row.metadata) + byte_size(row.message_type) + @row_overhead
  end

  ## Deadlines

  # Every write goes through here, so every write is made under a deadline the
  # watcher can see. This is the step the whole mechanism exists for: a consumer
  # that stops reading closes its receive window, the output buffer fills, and
  # this call blocks with no way to time itself out.
  defp chunk(conn, state, iodata) do
    arm(state)

    Plug.Conn.chunk(conn, iodata)
  end

  # `stall_timeout` is the budget for the blocking itself; `extra` is however
  # long the step is legitimately expected to sit there first.
  defp arm(state, extra \\ 0)

  defp arm(%{stall_timeout: :infinity} = state, _extra), do: Status.arm(state.status, :infinity)

  defp arm(state, extra), do: Status.arm(state.status, state.stall_timeout + extra)
end
