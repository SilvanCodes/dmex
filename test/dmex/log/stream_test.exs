defmodule DMEx.Log.StreamTest do
  use DMEx.Case, async: true

  alias DMEx.Log.Status
  alias DMEx.Log.Stream, as: SendLoop
  alias DMEx.Test.ClosingAdapter
  alias DMEx.Test.Storage, as: Stub

  @moduletag :capture_log

  setup do
    %{opts: Stub.new()}
  end

  # The loop reads, paces and tracks a cursor; what happens to a read's rows is
  # the transport's, and this is the whole of that seam.
  defmodule StubTransport do
    @moduledoc false
    @behaviour DMEx.Transport

    @impl DMEx.Transport
    def write(%{owner: owner, limit: limit, sent: sent} = sink, rows) do
      send(owner, {:wrote, rows})

      if sent + 1 >= limit do
        {:error, :closed}
      else
        {:ok, %{sink | sent: sent + 1}, %{rows: :ignored, bytes: 42}}
      end
    end

    @impl DMEx.Transport
    def heartbeat(%{owner: owner} = sink) do
      send(owner, :heartbeat)

      {:ok, sink}
    end

    @impl DMEx.Transport
    def connect(_opts), do: {:error, :not_a_receiver}
    @impl DMEx.Transport
    def handle_message(_message, _source), do: :unknown
    @impl DMEx.Transport
    def close(_source), do: :ok
    @impl DMEx.Transport
    def liveness(_source), do: :infinity
  end

  describe "the transport seam" do
    test "hands a whole read's rows over in one write", %{opts: opts} do
      Stub.put_rows(opts, 3)

      run_with(opts, chunks: 1)

      assert_received {:wrote, rows}
      assert Enum.map(rows, & &1.id) == [1, 2, 3]

      # Storage rows, not frames: framing is the transport's business and the
      # loop never sees a byte.
      assert %{id: 1, message_type: _, data: _, metadata: _, inserted_at: _} = hd(rows)
    end

    test "leaves metadata exactly as read", %{opts: opts} do
      Stub.put_rows(opts, [Stub.row(1, metadata: ~s({"trace_id":"abc"}))])

      run_with(opts, chunks: 1)

      assert_received {:wrote, [row]}
      assert row.metadata == ~s({"trace_id":"abc"})
    end

    test "reports the transport's own measurements alongside the row count", %{opts: opts} do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :stream, :write]])

      # Two writes: the first succeeds and is measured, the second is refused and
      # ends the loop.
      Stub.put_rows(opts, 2)
      run_with(opts, chunks: 2, max_batch_size: 1)

      assert_receive {[:dmex, :stream, :write], ^ref, measurements, _metadata}, 1_000

      # The loop's own count wins over anything a transport claims for it.
      assert measurements == %{rows: 1, bytes: 42}
    end

    test "a transport that reports the sink closed ends the loop", %{opts: opts} do
      Stub.put_rows(opts, 10)

      start_loop(opts, transport: StubTransport, chunks: 1, max_batch_size: 1)

      assert_receive {:done, _}, 2_000
    end

    test "asks for a heartbeat when a wait expires with nothing to send", %{opts: opts} do
      start_loop(opts, transport: StubTransport, heartbeat_ms: 10, chunks: 5)

      assert_receive :heartbeat, 1_000
    end
  end

  describe "reading and framing" do
    # One frame per row, one `chunk/2` per read. Batching is a write batch and
    # nothing the consumer can observe: it sees three ordinary one-row frames.
    test "writes one frame per row, all of a read in one chunk", %{opts: opts} do
      Stub.put_rows(opts, 3)

      run(opts, chunks: 1)

      assert_received {:chunk, write}
      assert ids(write) == [1, 2, 3]
      refute_received {:chunk, _}
    end

    test "an empty read sends no frame at all", %{opts: opts} do
      # Nothing at all, not a zero-row frame. The heartbeat interval is long here
      # so that nothing is owed on the liveness side either.
      start_loop(opts, heartbeat_ms: 60_000)

      refute_receive {:chunk, _}, 50

      # It kept looping and reading rather than stopping, so an idle stream
      # stays open with nothing to say.
      refute Stub.reads(opts) == []
    end

    # Regression: with the wait ahead of the read, a consumer connecting to a
    # log with a backlog would sit silent for a whole heartbeat interval before
    # its first frame — 15 seconds by default, with rows already waiting.
    test "reads before waiting, so a backlog starts draining at once", %{opts: opts} do
      Stub.put_rows(opts, 10)

      start_loop(opts, chunks: 1, heartbeat_ms: 60_000)

      assert_receive {:chunk, _}, 1_000
    end

    test "drains back-to-back without waiting on a wakeup between frames", %{opts: opts} do
      # Catch-up is the ordinary loop running hot, not a distinct mode: three
      # frames must take three batch_windows, not three heartbeats.
      Stub.put_rows(opts, 10)

      start_loop(opts, chunks: 3, max_batch_size: 2, heartbeat_ms: 60_000, batch_window: 1)

      assert_receive {:done, _}, 1_000
      assert length(Stub.reads(opts)) == 3
    end

    # Each frame's `id:` is its own row's id — never "the highest in this write" —
    # which is what makes a generic client's `Last-Event-ID` land exactly.
    test "each frame's id is its own row's id", %{opts: opts} do
      Stub.put_rows(opts, 3)

      run(opts, chunks: 1)

      assert_received {:chunk, write}
      assert ids(write) == [1, 2, 3]
    end

    test "the message type is the event name", %{opts: opts} do
      Stub.put_rows(opts, 1)

      run(opts, chunks: 1)

      assert_received {:chunk, write}
      assert write =~ "event: order_placed\n"
    end

    test "never reads more than max_batch_size", %{opts: opts} do
      Stub.put_rows(opts, 100)

      run(opts, chunks: 3, max_batch_size: 7)

      assert Enum.all?(Stub.reads(opts), fn {_cursor, limit, _types} -> limit == 7 end)

      for _ <- 1..3 do
        assert_received {:chunk, write}
        assert length(ids(write)) <= 7
      end
    end

    test "advances the cursor to the last id sent", %{opts: opts} do
      Stub.put_rows(opts, 10)

      run(opts, chunks: 3, max_batch_size: 4)

      # Reads walk the log rather than re-reading it: 0 -> 4 -> 8.
      assert Stub.reads(opts) == [{0, 4, :all}, {4, 4, :all}, {8, 4, :all}]
    end

    test "starts from the cursor it was given", %{opts: opts} do
      Stub.put_rows(opts, 10)

      run(opts, chunks: 1, cursor: 6)

      assert [{6, _limit, :all}] = Stub.reads(opts)
      assert_received {:chunk, write}
      assert hd(ids(write)) == 7
    end
  end

  describe "heartbeats" do
    # Comment lines are ignored by every spec-compliant SSE client, so this
    # never pollutes the message stream or needs consumer-side handling.
    test "an idle stream sends a comment frame rather than nothing", %{opts: opts} do
      start_loop(opts, chunks: 5, heartbeat_ms: 10)

      assert_receive {:chunk, ": hb\n\n"}, 1_000
    end

    test "a stream with something to send does not also heartbeat", %{opts: opts} do
      # The frame's own bytes are the liveness signal; a heartbeat alongside it
      # would say nothing further.
      Stub.put_rows(opts, 1)

      run(opts, chunks: 1, heartbeat_ms: 10)

      assert_received {:chunk, write}
      assert ids(write) == [1]
      refute_received {:chunk, ": hb\n\n"}
    end

    test "a wakeup cancels the heartbeat owed for that interval", %{opts: opts} do
      pid = start_loop(opts, chunks: 5, heartbeat_ms: 60_000)

      eventually(fn -> Stub.reads(opts) != [] end)
      send(pid, :wakeup)

      # The wait ended with an append rather than with silence, so nothing is
      # owed — and the next interval is a fresh minute.
      refute_receive {:chunk, ": hb\n\n"}, 100
    end

    test "stops when the socket closes under a heartbeat", %{opts: opts} do
      # chunks: 1 makes the very first write fail, and that write is a heartbeat.
      assert %Plug.Conn{} = run(opts, chunks: 1, heartbeat_ms: 10)

      assert_received {:chunk, ": hb\n\n"}
    end

    test "reports heartbeats in telemetry", %{opts: opts} do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :stream, :heartbeat]])

      start_loop(opts, chunks: 5, heartbeat_ms: 10)

      assert_receive {[:dmex, :stream, :heartbeat], ^ref, _measurements, metadata}, 1_000
      assert metadata.log == __MODULE__
    end
  end

  describe "wakeups" do
    test "a wakeup short-circuits the idle wait", %{opts: opts} do
      Stub.put_rows(opts, 1)

      pid = start_loop(opts, chunks: 1, heartbeat_ms: 60_000)
      send(pid, :wakeup)

      # heartbeat_ms is a minute, so a frame arriving at all — let alone within
      # a second — proves the receive returned on the wakeup, not its timeout.
      assert_receive {:chunk, _}, 1_000
    end

    # Regression, found by bench/e2e/end_to_end_latency.exs: with the window
    # measured from the top of the iteration, the time parked in await_wakeup/1
    # counted toward it *and* the full window was slept anyway — so a message on
    # a quiet stream waited a whole batch interval after the wakeup announcing
    # it. End-to-end p50 was 201ms against a 200ms window, where the design
    # expects about half of one.
    test "a wakeup on a long-idle stream is not delayed by the batch window", %{opts: opts} do
      pid = start_loop(opts, chunks: 1, heartbeat_ms: 60_000, batch_interval: 2_000)

      # Let the loop do its connect read and park with nothing to send.
      eventually(fn -> Stub.reads(opts) != [] end)

      Stub.put_rows(opts, 1)
      sent_at = System.monotonic_time(:millisecond)
      send(pid, :wakeup)

      assert_receive {:chunk, _}, 1_000
      elapsed = System.monotonic_time(:millisecond) - sent_at

      # Nothing was sent for seconds, so the window owes this frame no wait at
      # all. Before the fix this took the full 2_000ms.
      assert elapsed < 500
    end

    test "still spaces frames a batch window apart when it has work", %{opts: opts} do
      # The other half of the same property: the window is a floor on frame
      # *rate*, so back-to-back frames must still be paced.
      Stub.put_rows(opts, 10)

      started = System.monotonic_time(:millisecond)
      start_loop(opts, chunks: 3, max_batch_size: 1, batch_interval: 100)

      assert_receive {:done, _}, 2_000

      # Three frames, so at least two windows between them.
      assert System.monotonic_time(:millisecond) - started >= 200
    end

    test "many queued wakeups cost one read, not one read each", %{opts: opts} do
      # An empty log and a minute-long heartbeat, so once the loop is parked in
      # await_wakeup/1 a wakeup is the *only* thing that can cause another read.
      pid = start_loop(opts, chunks: 1, heartbeat_ms: 60_000, batch_interval: 100)

      # The loop always reads once on connect — that is what lets a consumer
      # with a backlog start draining immediately — so wait it out and measure
      # from there rather than racing it.
      eventually(fn -> Stub.reads(opts) != [] end)
      before = length(Stub.reads(opts))

      for _ <- 1..20, do: send(pid, :wakeup)

      Process.sleep(300)

      # Bounded rather than pinned to exactly one: the loop wakes on the first
      # wakeup and drains whatever has arrived by the time it comes round, so a
      # burst that straddles that moment can legitimately cost a second pass.
      # What the coalescing claim actually rules out is twenty.
      assert length(Stub.reads(opts)) - before <= 2
    end
  end

  describe "closing" do
    test "returns the conn when the socket closes mid-stream", %{opts: opts} do
      Stub.put_rows(opts, 100)

      assert %Plug.Conn{} = run(opts, chunks: 2, max_batch_size: 1)

      # It stopped at the close rather than draining all hundred rows.
      assert length(Stub.reads(opts)) == 2
    end

    test "stops without sending when the read fails", %{opts: opts} do
      Stub.put_rows(opts, 5)
      Stub.fail(opts, :connection_lost)

      assert %Plug.Conn{} = run(opts, chunks: 5)

      refute_received {:chunk, _}
    end
  end

  # The bound the loop relies on is enforced in the *read*, not here — see
  # `c:DMEx.Storage.read/5`. What these assert is that the loop passes it down
  # and behaves correctly on a short batch.
  describe "the byte bound" do
    test "a batch cut short by bytes leaves the rest for the next read", %{opts: opts} do
      big = String.duplicate("x", 400)
      rows = for id <- 1..10, do: Stub.row(id, data: ~s({"note":"#{big}"}))
      Stub.put_rows(opts, rows)

      run(opts, chunks: 2, max_batch_bytes: 1_000)

      assert_received {:chunk, first}
      assert byte_size(first) < 2_000

      # The cursor only advanced over what was read, so the next read picks up
      # exactly where the first stopped — nothing lost, nothing repeated.
      [{first_cursor, _, _}, {second_cursor, _, _}] = Stub.reads(opts)
      assert first_cursor == 0
      assert second_cursor > 0
      assert second_cursor < 10
    end

    test "the loop passes its configured budget to the read", %{opts: opts} do
      Stub.put_rows(opts, 3)

      run(opts, chunks: 1, max_batch_bytes: 4_242)

      assert [{_cursor, _limit, _types} | _] = Stub.reads(opts)
      assert Stub.read_bytes(opts) == [4_242]
    end

    test "always sends at least one row, even one larger than the budget", %{opts: opts} do
      # Refusing it would wedge the stream on that row permanently, which is a
      # worse failure than one oversized write.
      huge = String.duplicate("x", 5_000)
      Stub.put_rows(opts, [Stub.row(1, data: ~s({"note":"#{huge}"}))])

      run(opts, chunks: 1, max_batch_bytes: 100)

      assert_received {:chunk, write}
      assert byte_size(write) > 5_000
    end
  end

  # A loop that stopped arming would look perfectly healthy in every other test
  # here and be unreclaimable in production, so the arming is asserted directly
  # rather than through the watcher.
  describe "deadlines" do
    test "a write is made under one", %{opts: opts} do
      status = Status.new(60_000)
      Stub.put_rows(opts, 3)

      run(opts, chunks: 1, status: status, stall_timeout: 5_000, batch_interval: 1)

      now = System.monotonic_time(:millisecond)

      # Armed for the write, not for the 60 s the status was created with.
      assert Status.deadline(status) <= now + 5_000 + 1
      refute Status.overdue?(status, now)
    end

    test "an idle wait is budgeted for the interval it is waiting out", %{opts: opts} do
      status = Status.new(1)

      start_loop(opts, status: status, stall_timeout: 5_000, heartbeat_ms: 60_000)

      # A stream parked in `await_wakeup/1` is idle, not stuck: its deadline
      # covers the heartbeat interval as well as the budget.
      eventually(fn ->
        Status.deadline(status) > System.monotonic_time(:millisecond) + 60_000
      end)
    end

    test "a read is under one too, which is the case no socket timeout can see", %{opts: opts} do
      status = Status.new(60_000)
      Stub.hang(opts)

      start_loop(opts, status: status, stall_timeout: 20, heartbeat_ms: 60_000)

      # The socket is fine; the query never comes back. Nothing but the armed
      # deadline distinguishes this from a healthy stream.
      eventually(fn -> Status.overdue?(status, System.monotonic_time(:millisecond)) end)
    end

    test "a log with no stall timeout arms nothing", %{opts: opts} do
      status = Status.new(:infinity)
      Stub.put_rows(opts, 3)

      run(opts, chunks: 1, status: status, stall_timeout: :infinity)

      refute Status.overdue?(status, System.monotonic_time(:millisecond) + 86_400_000)
    end
  end

  describe "telemetry" do
    test "reports the rows and bytes of each write", %{opts: opts} do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :stream, :write]])

      Stub.put_rows(opts, 10)

      run(opts, chunks: 2, max_batch_size: 4)

      # Matched in the pattern rather than asserted afterwards: telemetry
      # handlers are global, so an async sibling's write event can land in this
      # mailbox first and assert_receive scans for a match rather than taking
      # the head.
      assert_receive {[:dmex, :stream, :write], ^ref, measurements, %{log: _}}
                     when measurements.rows == 4

      assert measurements.bytes > 0
    end

    # There is no `truncated` flag any more, because the byte bound is applied
    # in the read rather than by trimming a batch already in hand — and `rows`
    # against `max_batch_size` says the same thing more precisely. A write
    # carrying fewer rows than the limit, with `bytes` near `max_batch_bytes`,
    # is a stream whose byte budget is what binds.
    test "a byte-bounded write reports fewer rows than the row limit", %{opts: opts} do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :stream, :write]])

      big = String.duplicate("x", 400)
      Stub.put_rows(opts, for(id <- 1..10, do: Stub.row(id, data: ~s({"note":"#{big}"}))))

      run(opts, chunks: 2, max_batch_size: 10, max_batch_bytes: 1_000)

      assert_receive {[:dmex, :stream, :write], ^ref, measurements, _metadata}
      assert measurements.rows < 10
      assert measurements.bytes > 0
    end
  end

  ## Harness

  # The ids of every frame in one write, in order.
  defp ids(write) do
    Regex.scan(~r{^id: (\d+)$}m, write) |> Enum.map(fn [_, id] -> String.to_integer(id) end)
  end

  # The loop only returns when the connection closes, and an idle stream never
  # closes — so it runs in its own process. That also puts wakeups where they
  # really arrive: in the loop's mailbox, not the test's.
  defp start_loop(opts, test_opts) do
    parent = self()

    state = state(opts, test_opts)
    sink = sink(parent, test_opts)
    pid = spawn(fn -> send(parent, {:done, SendLoop.loop(sink, state)}) end)

    on_exit(fn -> Process.exit(pid, :kill) end)

    pid
  end

  # The SSE transport's sink is a conn that stops accepting after N writes; the
  # stub's is a plain map that does the same, since the loop only ever hands it
  # back to the transport.
  defp sink(parent, test_opts) do
    chunks = Keyword.get(test_opts, :chunks, 1)

    case Keyword.get(test_opts, :transport, DMEx.Transport.SSE) do
      StubTransport -> %{owner: parent, limit: chunks, sent: 0}
      DMEx.Transport.SSE -> ClosingAdapter.conn(parent, chunks, state: :chunked)
    end
  end

  # Runs the loop to completion and returns the sink it finished with.
  defp run(opts, test_opts) do
    start_loop(opts, test_opts)

    assert_receive {:done, conn}, 2_000

    conn
  end

  defp run_with(opts, test_opts) do
    run(opts, Keyword.put(test_opts, :transport, StubTransport))
  end

  defp state(opts, test_opts) do
    %{
      log: __MODULE__,
      transport: Keyword.get(test_opts, :transport, DMEx.Transport.SSE),
      storage: Stub,
      storage_opts: opts,
      cursor: Keyword.get(test_opts, :cursor, 0),
      batch_interval: Keyword.get(test_opts, :batch_interval, 1),
      # Built by `DMEx.Config` rather than spelled out, so this harness cannot
      # drift from what a real log hands the loop.
      config:
        DMEx.Config.log!(
          max_batch_size: Keyword.get(test_opts, :max_batch_size, 500),
          heartbeat_ms: Keyword.get(test_opts, :heartbeat_ms, 60_000),
          max_batch_bytes: Keyword.get(test_opts, :max_batch_bytes, 1_048_576),
          stall_timeout: Keyword.get(test_opts, :stall_timeout, 30_000)
        ),
      last_sent_at: nil,
      status: Keyword.get(test_opts, :status) || Status.new(30_000),
      types: Keyword.get(test_opts, :types, :all)
    }
  end
end
