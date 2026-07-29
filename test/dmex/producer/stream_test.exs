defmodule DMEx.Producer.StreamTest do
  use DMEx.Case, async: true

  alias DMEx.Producer.Params
  alias DMEx.Producer.Status
  alias DMEx.Producer.Stream, as: SendLoop
  alias DMEx.Test.ClosingAdapter
  alias DMEx.Test.Storage, as: Stub

  @moduletag :capture_log

  setup do
    %{opts: Stub.new()}
  end

  describe "reading and framing" do
    test "sends one frame per non-empty read", %{opts: opts} do
      Stub.put_rows(opts, 3)

      run(opts, chunks: 1)

      assert_received {:chunk, frame}
      assert frame =~ "event: batch"
      refute_received {:chunk, _}
    end

    test "an empty read sends no frame at all", %{opts: opts} do
      # Not an empty batch frame, not a zero-row frame — nothing. Which is also
      # why a read returning no rows needs no max_id. The heartbeat interval is
      # long here so that nothing is owed on the liveness side either.
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

    test "the frame's id is the highest id it carries", %{opts: opts} do
      Stub.put_rows(opts, 3)

      run(opts, chunks: 1)

      assert_received {:chunk, frame}
      assert frame =~ "id: 3\n"
    end

    test "carries the log's tip as max_id, not the batch's highest id", %{opts: opts} do
      Stub.put_rows(opts, 10)

      run(opts, chunks: 1, max_batch_size: 4)

      assert_received {:chunk, frame}
      # Four rows sent, but the consumer must be able to see it is six behind.
      assert frame =~ ~s("max_id":10)
      assert frame =~ "id: 4\n"
    end

    test "never reads more than max_batch_size", %{opts: opts} do
      Stub.put_rows(opts, 100)

      run(opts, chunks: 3, max_batch_size: 7)

      assert Enum.all?(Stub.reads(opts), fn {_cursor, limit} -> limit == 7 end)

      for _ <- 1..3 do
        assert_received {:chunk, frame}
        assert length(String.split(frame, ~s("id":))) - 1 <= 7
      end
    end

    test "advances the cursor to the last id sent", %{opts: opts} do
      Stub.put_rows(opts, 10)

      run(opts, chunks: 3, max_batch_size: 4)

      # Reads walk the log rather than re-reading it: 0 -> 4 -> 8.
      assert Stub.reads(opts) == [{0, 4}, {4, 4}, {8, 4}]
    end

    test "starts from the cursor it was given", %{opts: opts} do
      Stub.put_rows(opts, 10)

      run(opts, chunks: 1, cursor: 6)

      assert [{6, _limit}] = Stub.reads(opts)
      assert_received {:chunk, frame}
      assert frame =~ ~s("id":7)
      refute frame =~ ~s("id":6)
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

      assert_received {:chunk, frame}
      assert frame =~ "event: batch"
      refute_received {:chunk, ": hb\n\n"}
    end

    test "a wakeup cancels the heartbeat owed for that interval", %{opts: opts} do
      pid = start_loop(opts, chunks: 5, heartbeat_ms: 60_000)

      eventually(fn -> Stub.reads(opts) != [] end)
      send(pid, :wakeup)

      # The wait ended with a publish rather than with silence, so nothing is
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
      assert metadata.producer == __MODULE__
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
    # a quiet stream waited a whole `batch_window` after the wakeup announcing
    # it. End-to-end p50 was 201ms against a 200ms window, where the design
    # expects about half of one.
    test "a wakeup on a long-idle stream is not delayed by the batch window", %{opts: opts} do
      pid = start_loop(opts, chunks: 1, heartbeat_ms: 60_000, batch_window: 2_000)

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
      start_loop(opts, chunks: 3, max_batch_size: 1, batch_window: 100)

      assert_receive {:done, _}, 2_000

      # Three frames, so at least two windows between them.
      assert System.monotonic_time(:millisecond) - started >= 200
    end

    test "many queued wakeups cost one read, not one read each", %{opts: opts} do
      # An empty log and a minute-long heartbeat, so once the loop is parked in
      # await_wakeup/1 a wakeup is the *only* thing that can cause another read.
      pid = start_loop(opts, chunks: 1, heartbeat_ms: 60_000, batch_window: 100)

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
      Stub.fail(opts, :read, :connection_lost)

      assert %Plug.Conn{} = run(opts, chunks: 5)

      refute_received {:chunk, _}
    end
  end

  describe "the frame byte cap" do
    test "truncates a frame that would exceed it, keeping the rest for next time", %{opts: opts} do
      big = String.duplicate("x", 400)
      rows = for id <- 1..10, do: Stub.row(id, data: ~s({"note":"#{big}"}))
      Stub.put_rows(opts, rows)

      run(opts, chunks: 2, frame_max_bytes: 1_000)

      assert_received {:chunk, first}
      assert byte_size(first) < 2_000

      # The cursor only advanced over what was sent, so the next read picks up
      # exactly where the truncated frame stopped — nothing lost, nothing
      # repeated.
      [{first_cursor, _}, {second_cursor, _}] = Stub.reads(opts)
      assert first_cursor == 0
      assert second_cursor > 0
      assert second_cursor < 10
    end

    test "always sends at least one row, even one larger than the cap", %{opts: opts} do
      # Refusing it would wedge the stream on that row permanently, which is a
      # worse failure than one oversized frame.
      huge = String.duplicate("x", 5_000)
      Stub.put_rows(opts, [Stub.row(1, data: ~s({"note":"#{huge}"}))])

      run(opts, chunks: 1, frame_max_bytes: 100)

      assert_received {:chunk, frame}
      assert byte_size(frame) > 5_000
    end

    test "reports truncation in telemetry", %{opts: opts} do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :stream, :frame]])

      big = String.duplicate("x", 400)
      Stub.put_rows(opts, for(id <- 1..10, do: Stub.row(id, data: ~s({"note":"#{big}"}))))

      run(opts, chunks: 2, frame_max_bytes: 1_000)

      assert_received {[:dmex, :stream, :frame], ^ref, measurements, %{truncated: true}}
      assert measurements.rows < 10
      assert measurements.bytes > 0
    end
  end

  # A loop that stopped arming would look perfectly healthy in every other test
  # here and be unreclaimable in production, so the arming is asserted directly
  # rather than through the watcher.
  describe "deadlines" do
    test "a write is made under one", %{opts: opts} do
      status = Status.new(60_000)
      Stub.put_rows(opts, 3)

      run(opts, chunks: 1, status: status, stall_timeout: 5_000, batch_window: 1)

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
      Stub.hang(opts, :read)

      start_loop(opts, status: status, stall_timeout: 20, heartbeat_ms: 60_000)

      # The socket is fine; the query never comes back. Nothing but the armed
      # deadline distinguishes this from a healthy stream.
      eventually(fn -> Status.overdue?(status, System.monotonic_time(:millisecond)) end)
    end

    test "a producer with no stall timeout arms nothing", %{opts: opts} do
      status = Status.new(:infinity)
      Stub.put_rows(opts, 3)

      run(opts, chunks: 1, status: status, stall_timeout: :infinity)

      refute Status.overdue?(status, System.monotonic_time(:millisecond) + 86_400_000)
    end
  end

  describe "telemetry" do
    test "reports rows, bytes and how far behind the frame left the consumer", %{opts: opts} do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :stream, :frame]])

      Stub.put_rows(opts, 10)

      run(opts, chunks: 2, max_batch_size: 4)

      # Matched in the pattern rather than asserted afterwards: telemetry
      # handlers are global, so an async sibling's frame event can land in this
      # mailbox first and assert_receive scans for a match rather than taking
      # the head.
      assert_receive {[:dmex, :stream, :frame], ^ref, measurements, %{truncated: false}}
                     when measurements.rows == 4

      assert measurements.bytes > 0
      # Six rows still unsent when this frame went out.
      assert measurements.lag == 6
    end
  end

  ## Harness

  # The loop only returns when the connection closes, and an idle stream never
  # closes — so it runs in its own process. That also puts wakeups where they
  # really arrive: in the loop's mailbox, not the test's.
  defp start_loop(opts, test_opts) do
    parent = self()
    chunks = Keyword.get(test_opts, :chunks, 1)

    conn = ClosingAdapter.conn(parent, chunks, state: :chunked)

    state = state(opts, test_opts)
    pid = spawn(fn -> send(parent, {:done, SendLoop.loop(conn, state)}) end)

    on_exit(fn -> Process.exit(pid, :kill) end)

    pid
  end

  # Runs the loop to completion and returns the conn it finished with.
  defp run(opts, test_opts) do
    start_loop(opts, test_opts)

    assert_receive {:done, conn}, 2_000

    conn
  end

  defp state(opts, test_opts) do
    params = %Params{
      cursor: 0,
      batch_window: Keyword.get(test_opts, :batch_window, 1),
      max_batch_size: Keyword.get(test_opts, :max_batch_size, 500)
    }

    %{
      producer: __MODULE__,
      storage: Stub,
      storage_opts: opts,
      cursor: Keyword.get(test_opts, :cursor, 0),
      batch_window: params.batch_window,
      max_batch_size: params.max_batch_size,
      heartbeat_ms: Keyword.get(test_opts, :heartbeat_ms, 60_000),
      frame_max_bytes: Keyword.get(test_opts, :frame_max_bytes, 1_048_576),
      stall_timeout: Keyword.get(test_opts, :stall_timeout, 30_000),
      last_sent_at: nil,
      status: Keyword.get(test_opts, :status) || Status.new(30_000)
    }
  end
end
