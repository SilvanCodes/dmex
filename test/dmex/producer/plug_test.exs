defmodule DMEx.Producer.PlugTest do
  use DMEx.Case, async: true

  alias DMEx.Producer.Plug, as: ProducerPlug
  alias DMEx.Producer.Registry, as: ProducerRegistry
  alias DMEx.Test.ClosingAdapter
  alias DMEx.Test.Storage, as: Stub
  alias DMEx.Test.StubProducer

  @moduletag :capture_log

  setup do
    start_supervised!(ProducerRegistry.child_spec(StubProducer))

    %{opts: Stub.new(), plug: ProducerPlug.init(producer: StubProducer)}
  end

  describe "init/1" do
    test "requires a producer, and says how to give it one" do
      assert_raise ArgumentError, ~r/requires a `:producer`.*init_opts:/s, fn ->
        ProducerPlug.init([])
      end
    end

    test "returns a prepared map without touching the database or registry" do
      # Under Phoenix's default init_mode this runs at compile time, so it must
      # not do either.
      assert %{producer: StubProducer} = ProducerPlug.init(producer: StubProducer)
    end
  end

  describe "rejecting a malformed request" do
    test "a garbage Last-Event-ID is a 400 naming the header", %{opts: opts, plug: plug} do
      conn = call(opts, plug, [{"last-event-id", "nonsense"}])

      assert conn.status == 400
      assert_received {:resp, 400, body}
      assert body =~ "invalid last-event-id header"
    end

    test "a garbage pacing header is a 400", %{opts: opts, plug: plug} do
      conn = call(opts, plug, [{"dmex-max-batch-size", "lots"}])

      assert conn.status == 400
      assert_received {:resp, 400, body}
      assert body =~ "invalid dmex-max-batch-size header"
    end

    test "a 400 carries text/plain and none of the stream headers", %{opts: opts, plug: plug} do
      conn = call(opts, plug, [{"last-event-id", "-1"}])

      assert get_header(conn, "content-type") =~ "text/plain"
      assert get_header(conn, "x-accel-buffering") == nil
      assert get_header(conn, "dmex-batch-window") == nil
    end

    test "a rejected request never opens a stream", %{opts: opts, plug: plug} do
      call(opts, plug, [{"last-event-id", "nope"}])

      assert Stub.reads(opts) == []
      assert ProducerRegistry.count(StubProducer) == 0
    end
  end

  describe "response headers on a 200" do
    setup %{opts: opts} do
      Stub.put_rows(opts, 1)

      :ok
    end

    test "carries the ordinary SSE set", %{opts: opts, plug: plug} do
      conn = stream(opts, plug)

      assert conn.status == 200
      assert get_header(conn, "content-type") =~ "text/event-stream"
      assert get_header(conn, "cache-control") == "no-cache"
    end

    # nginx buffers responses by default, which converts batch_window pacing
    # into "whenever the buffer happens to fill" and swallows heartbeats — so a
    # healthy stream looks dead to both the consumer and the proxy's own idle
    # timeout. The header is ignored by everything else.
    test "tells nginx not to buffer", %{opts: opts, plug: plug} do
      assert stream(opts, plug) |> get_header("x-accel-buffering") == "no"
    end

    test "sets keep-alive on HTTP/1.1, where it means something", %{opts: opts, plug: plug} do
      assert stream(opts, plug) |> get_header("connection") == "keep-alive"
    end

    test "echoes the pacing it actually applied", %{opts: opts, plug: plug} do
      conn = stream(opts, plug, [{"dmex-batch-window", "300"}, {"dmex-max-batch-size", "250"}])

      assert get_header(conn, "dmex-batch-window") == "300"
      assert get_header(conn, "dmex-max-batch-size") == "250"
    end

    # Without this a clamped consumer cannot tell "the producer is honouring my
    # request and I still can't keep up" from "the producer capped me and my
    # request never took effect" — opposite situations calling for opposite
    # reactions from an adaptive controller.
    test "echoes the clamped value, not the requested one", %{opts: opts, plug: plug} do
      conn = stream(opts, plug, [{"dmex-batch-window", "1"}, {"dmex-max-batch-size", "999999"}])

      assert conn.status == 200
      assert get_header(conn, "dmex-batch-window") == "50"
      assert get_header(conn, "dmex-max-batch-size") == "5000"
    end

    test "echoes the defaults for a client that asks for nothing", %{opts: opts, plug: plug} do
      # Which is every browser EventSource, permanently.
      conn = stream(opts, plug)

      assert get_header(conn, "dmex-batch-window") == "200"
      assert get_header(conn, "dmex-max-batch-size") == "500"
    end
  end

  describe "opening the stream" do
    test "advertises the effective heartbeat as retry: before anything else", %{
      opts: opts,
      plug: plug
    } do
      Stub.put_rows(opts, 1)

      stream(opts, plug, [{"dmex-batch-window", "300"}])

      # heartbeat_ms + batch_window, because the loop's throttle runs after the
      # heartbeat branch — the sum is the real upper bound on silence.
      assert_received {:chunk, "retry: 15300\n\n"}
    end

    test "then sends the log as batch frames", %{opts: opts, plug: plug} do
      Stub.put_rows(opts, 2)

      stream(opts, plug)

      assert_received {:chunk, "retry: " <> _}
      assert_received {:chunk, frame}
      assert frame =~ "event: batch"
      assert frame =~ ~s("id":1)
      assert frame =~ ~s("id":2)
    end

    test "registers the request process for wakeups", %{opts: opts, plug: plug} do
      Stub.put_rows(opts, 1)

      # Registered from inside the stream process — on the producer side the
      # request *is* the stream, so the entry lives and dies with the connection.
      pid = start_stream(opts, plug, [], chunks: 5)

      eventually(fn -> ProducerRegistry.count(StubProducer) == 1 end)

      Process.exit(pid, :kill)

      eventually(fn -> ProducerRegistry.count(StubProducer) == 0 end)
    end

    test "emits a connect event with the resolved cursor", %{opts: opts, plug: plug} do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :stream, :connect]])
      Stub.put_rows(opts, 1)

      # Resuming past the tip, so no data frame is ever sent and the connection
      # stays open — the event has to be observable without the stream ending.
      pid = start_stream(opts, plug, [{"last-event-id", "7"}], chunks: 5)

      assert_receive {[:dmex, :stream, :connect], ^ref, %{cursor: 7}, metadata}, 1_000
      assert metadata.batch_window == 200
      assert metadata.max_batch_size == 500

      Process.exit(pid, :kill)
    end
  end

  describe "resume position" do
    test "absent means the whole log", %{opts: opts, plug: plug} do
      Stub.put_rows(opts, 3)

      stream(opts, plug)

      assert [{0, _} | _] = Stub.reads(opts)
    end

    test "an id resumes after it", %{opts: opts, plug: plug} do
      Stub.put_rows(opts, 5)

      stream(opts, plug, [{"last-event-id", "3"}])

      assert [{3, _} | _] = Stub.reads(opts)
    end

    # This is how a consumer that only cares about what happens from now on
    # skips history, with no preflight request and no second parameter.
    test "`latest` resolves against the tip and streams nothing yet", %{opts: opts, plug: plug} do
      Stub.put_rows(opts, 10)

      pid = start_stream(opts, plug, [{"last-event-id", "latest"}], chunks: 5)

      assert_receive {:chunk, "retry: " <> _}
      eventually(fn -> Stub.reads(opts) != [] end)

      # It starts at the tip, so the first read finds nothing and no data frame
      # is ever sent for the ten rows of history.
      assert [{10, _} | _] = Stub.reads(opts)
      refute_receive {:chunk, "id: " <> _}, 50

      Process.exit(pid, :kill)
    end

    test "`latest` on an empty log is the same as from the beginning", %{opts: opts, plug: plug} do
      pid = start_stream(opts, plug, [{"last-event-id", "latest"}], chunks: 5)

      eventually(fn -> Stub.reads(opts) != [] end)

      assert [{0, _} | _] = Stub.reads(opts)

      Process.exit(pid, :kill)
    end
  end

  ## Harness

  defp call(opts, plug, req_headers) do
    StubProducer.install(opts)

    ProducerPlug.call(ClosingAdapter.conn(self(), 1, req_headers: req_headers), plug)
  end

  # Runs the plug to completion — the connection closes after `chunks` writes —
  # and returns the conn, so response headers are assertable.
  defp stream(opts, plug, req_headers \\ [], test_opts \\ []) do
    start_stream(opts, plug, req_headers, test_opts)

    assert_receive {:done, conn}, 2_000

    conn
  end

  defp start_stream(opts, plug, req_headers, test_opts) do
    parent = self()
    chunks = Keyword.get(test_opts, :chunks, 2)

    :ok = StubProducer.install(opts, Keyword.get(test_opts, :config, []))
    exported = StubProducer.export()

    conn = ClosingAdapter.conn(parent, chunks, req_headers: req_headers)

    pid =
      spawn(fn ->
        StubProducer.import(exported)
        send(parent, {:done, ProducerPlug.call(conn, plug)})
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)

    pid
  end

  defp get_header(conn, name) do
    case Enum.filter(conn.resp_headers, &match?({^name, _}, &1)) do
      [{^name, value} | _] -> value
      [] -> nil
    end
  end
end
