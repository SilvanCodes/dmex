defmodule DMEx.Log.PlugTest do
  use DMEx.Case, async: true

  alias DMEx.Log.Plug, as: LogPlug
  alias DMEx.Log.Registry, as: LogRegistry
  alias DMEx.Test.ClosingAdapter
  alias DMEx.Test.Storage, as: Stub
  alias DMEx.Test.StubLog

  @moduletag :capture_log

  setup do
    start_supervised!(LogRegistry.child_spec(StubLog))

    %{opts: Stub.new(), plug: LogPlug.init(log: StubLog)}
  end

  describe "init/1" do
    test "requires a log, and says how to give it one" do
      assert_raise ArgumentError, ~r/requires a `:log`.*init_opts:/s, fn ->
        LogPlug.init([])
      end
    end

    test "returns a prepared map without touching the database or registry" do
      # Under Phoenix's default init_mode this runs at compile time, so it must
      # not do either.
      assert %{log: StubLog} = LogPlug.init(log: StubLog)
    end
  end

  describe "rejecting a malformed request" do
    test "a garbage Last-Event-ID is a 400 naming the header", %{opts: opts, plug: plug} do
      conn = call(opts, plug, [{"last-event-id", "nonsense"}])

      assert conn.status == 400
      assert_received {:resp, 400, body}
      assert body =~ "invalid last-event-id header"
    end

    # There are no DMEx request headers, so one is just an unknown header — and
    # a log has no business rejecting a request over a header it does not
    # define.
    test "a header DMEx does not define is ignored, not rejected", %{opts: opts, plug: plug} do
      Stub.put_rows(opts, 1)

      conn = stream(opts, plug, [{"dmex-max-batch-size", "lots"}])

      assert conn.status == 200
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
      assert LogRegistry.count(StubLog) == 0
    end

    # Without this a POST to the mount opens a chunked stream and sits there,
    # which is a confusing way to learn about a typo in a router.
    test "anything but GET is a 405 with an allow header", %{opts: opts, plug: plug} do
      StubLog.install(opts)

      conn =
        LogPlug.call(
          ClosingAdapter.conn(self(), 1, method: "POST", req_headers: []),
          plug
        )

      assert conn.status == 405
      assert get_header(conn, "allow") == "GET"
      assert_received {:resp, 405, body}
      assert body =~ "only GET"

      assert Stub.reads(opts) == []
      assert LogRegistry.count(StubLog) == 0
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

    # nginx buffers responses by default, which converts max_batch_frequency pacing
    # into "whenever the buffer happens to fill" and swallows heartbeats — so a
    # healthy stream looks dead to both the consumer and the proxy's own idle
    # timeout. The header is ignored by everything else.
    test "tells nginx not to buffer", %{opts: opts, plug: plug} do
      assert stream(opts, plug) |> get_header("x-accel-buffering") == "no"
    end

    test "sets keep-alive on HTTP/1.1, where it means something", %{opts: opts, plug: plug} do
      assert stream(opts, plug) |> get_header("connection") == "keep-alive"
    end

    # There is no pacing echo, because there is no pacing request. A DMEx
    # consumer and a browser EventSource get byte-identical responses.
    test "carries no DMEx pacing headers", %{opts: opts, plug: plug} do
      conn = stream(opts, plug)

      assert get_header(conn, "dmex-batch-window") == nil
      assert get_header(conn, "dmex-max-batch-size") == nil
    end
  end

  describe "opening the stream" do
    test "advertises the effective heartbeat as retry: before anything else", %{
      opts: opts,
      plug: plug
    } do
      Stub.put_rows(opts, 1)

      stream(opts, plug)

      # heartbeat_ms + one batch interval, because the loop's throttle runs after the
      # heartbeat branch — the sum is the real upper bound on silence. Both are
      # log configuration, so every connection is told the same number.
      assert_received {:chunk, "retry: 15200\n\n"}
    end

    test "then sends the log as one frame per row", %{opts: opts, plug: plug} do
      Stub.put_rows(opts, 2)

      stream(opts, plug)

      assert_received {:chunk, "retry: " <> _}
      assert_received {:chunk, write}
      assert write =~ "id: 0/1\nevent: order_placed\n"
      assert write =~ "id: 1/2\nevent: order_placed\n"
    end

    test "registers the request process for wakeups", %{opts: opts, plug: plug} do
      Stub.put_rows(opts, 1)

      # Registered from inside the stream process — on the log side the
      # request *is* the stream, so the entry lives and dies with the connection.
      pid = start_stream(opts, plug, [], chunks: 5)

      eventually(fn -> LogRegistry.count(StubLog) == 1 end)

      Process.exit(pid, :kill)

      eventually(fn -> LogRegistry.count(StubLog) == 0 end)
    end

    test "emits a connect event with the resolved cursor", %{opts: opts, plug: plug} do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :stream, :connect]])
      Stub.put_rows(opts, 1)

      # Resuming past the tip, so no data frame is ever sent and the connection
      # stays open — the event has to be observable without the stream ending.
      pid = start_stream(opts, plug, [{"last-event-id", "7"}], chunks: 5)

      assert_receive {[:dmex, :stream, :connect], ^ref, %{cursor: 7}, metadata}, 1_000
      assert metadata.log == StubLog
      refute metadata.selective

      Process.exit(pid, :kill)
    end
  end

  describe "resume position" do
    test "absent means the whole log", %{opts: opts, plug: plug} do
      Stub.put_rows(opts, 3)

      stream(opts, plug)

      assert [{0, _, :all} | _] = Stub.reads(opts)
    end

    test "an id resumes after it", %{opts: opts, plug: plug} do
      Stub.put_rows(opts, 5)

      stream(opts, plug, [{"last-event-id", "3"}])

      assert [{3, _, :all} | _] = Stub.reads(opts)
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
      assert [{10, _, :all} | _] = Stub.reads(opts)
      refute_receive {:chunk, "id: " <> _}, 50

      Process.exit(pid, :kill)
    end

    test "`latest` on an empty log is the same as from the beginning", %{opts: opts, plug: plug} do
      pid = start_stream(opts, plug, [{"last-event-id", "latest"}], chunks: 5)

      eventually(fn -> Stub.reads(opts) != [] end)

      assert [{0, _, :all} | _] = Stub.reads(opts)

      Process.exit(pid, :kill)
    end
  end

  ## Harness

  defp call(opts, plug, req_headers) do
    StubLog.install(opts)

    LogPlug.call(ClosingAdapter.conn(self(), 1, req_headers: req_headers), plug)
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

    :ok = StubLog.install(opts, Keyword.get(test_opts, :config, []))
    exported = StubLog.export()

    conn = ClosingAdapter.conn(parent, chunks, req_headers: req_headers)

    pid =
      spawn(fn ->
        StubLog.import(exported)
        send(parent, {:done, LogPlug.call(conn, plug)})
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
