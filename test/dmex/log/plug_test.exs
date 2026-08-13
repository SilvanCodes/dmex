defmodule DMEx.Log.PlugTest do
  use DMEx.Case, async: true

  alias DMEx.Handler.Fingerprint
  alias DMEx.Log.Plug, as: LogPlug
  alias DMEx.Log.Streams
  alias DMEx.Test.ClosingAdapter
  alias DMEx.Test.Storage, as: Stub
  alias DMEx.Test.StubLog

  @moduletag :capture_log

  # An absent `last-event-id` now means "from now on", so a test that wants
  # history delivered has to ask for it.
  @whole_log [{"last-event-id", "0"}]

  setup do
    start_supervised!(Streams.child_spec(StubLog))

    %{opts: Stub.new(), plug: LogPlug.init(log_module: StubLog)}
  end

  describe "init/1" do
    test "requires a log, and says how to give it one" do
      assert_raise ArgumentError, ~r/requires a `:log_module`.*log_module: MyApp.Log/s, fn ->
        LogPlug.init([])
      end
    end

    test "returns a prepared map without touching the database or registry" do
      # Under Phoenix's default init_mode this runs at compile time, so it must
      # not do either.
      assert %{log: StubLog} = LogPlug.init(log_module: StubLog)
    end
  end

  # The selective branch of `select/4`, which is the only path that consults
  # `DMEx.Message.types/0` — and therefore the only one sensitive to how that
  # set is discovered. It had been
  # exercised only by `:integration` tests, which `mix test` excludes, so two
  # separate regressions in message discovery left the default run green.
  describe "the selective path" do
    test "a filter over a known type streams and reports the count",
         %{opts: opts, plug: plug} do
      Stub.put_rows(1, opts)
      {:ok, filter} = Fingerprint.build(["order_placed"])

      conn = stream(opts, plug, @whole_log, path_info: [Fingerprint.encode(filter)])

      assert conn.status == 200
      assert get_header(conn, "dmex-matched-types") == "1"
    end

    test "a whole-log stream sends no matched header", %{opts: opts, plug: plug} do
      Stub.put_rows(1, opts)

      conn = stream(opts, plug, @whole_log)

      assert get_header(conn, "dmex-matched-types") == nil
    end

    test "a filter matching none of the application's types is a 404",
         %{opts: opts, plug: plug} do
      {:ok, filter} = Fingerprint.build(["Nothing.Anyone.Has.Ever.Appended"])

      conn = call(opts, plug, [], [Fingerprint.encode(filter)])

      assert conn.status == 404
      assert_received {:resp, 404, body}
      assert body =~ "matched none of the message types"
    end

    test "a segment that does not decode is a 400 naming the filter",
         %{opts: opts, plug: plug} do
      conn = call(opts, plug, [], ["not-a-blob!"])

      assert conn.status == 400
      assert_received {:resp, 400, body}
      assert body =~ "invalid filter segment"
    end

    test "a 404 registers nothing", %{opts: opts, plug: plug} do
      {:ok, filter} = Fingerprint.build(["Nothing.Anyone.Has.Ever.Appended"])

      call(opts, plug, [], [Fingerprint.encode(filter)])

      assert Streams.count(StubLog) == 0
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
      Stub.put_rows(1, opts)

      conn = stream(opts, plug, @whole_log ++ [{"dmex-max-batch-size", "lots"}])

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
      assert Streams.count(StubLog) == 0
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
      assert Streams.count(StubLog) == 0
    end
  end

  describe "response headers on a 200" do
    setup %{opts: opts} do
      Stub.put_rows(1, opts)

      :ok
    end

    test "carries the ordinary SSE set", %{opts: opts, plug: plug} do
      conn = stream(opts, plug, @whole_log)

      assert conn.status == 200
      assert get_header(conn, "content-type") =~ "text/event-stream"
      assert get_header(conn, "cache-control") == "no-cache"
    end

    # nginx buffers responses by default, which converts max_batch_frequency pacing
    # into "whenever the buffer happens to fill" and swallows heartbeats — so a
    # healthy stream looks dead to both the consumer and the proxy's own idle
    # timeout. The header is ignored by everything else.
    test "tells nginx not to buffer", %{opts: opts, plug: plug} do
      assert opts |> stream(plug, @whole_log) |> get_header("x-accel-buffering") == "no"
    end

    test "sets keep-alive on HTTP/1.1, where it means something", %{opts: opts, plug: plug} do
      assert opts |> stream(plug, @whole_log) |> get_header("connection") == "keep-alive"
    end

    # There is no pacing echo, because there is no pacing request. A DMEx
    # consumer and a browser EventSource get byte-identical responses.
    test "carries no DMEx pacing headers", %{opts: opts, plug: plug} do
      conn = stream(opts, plug, @whole_log)

      assert get_header(conn, "dmex-batch-window") == nil
      assert get_header(conn, "dmex-max-batch-size") == nil
    end
  end

  describe "opening the stream" do
    test "advertises the effective heartbeat as retry: before anything else", %{
      opts: opts,
      plug: plug
    } do
      Stub.put_rows(1, opts)

      stream(opts, plug, @whole_log)

      # heartbeat_interval + one batch interval, because the loop's throttle runs after the
      # heartbeat branch — the sum is the real upper bound on silence. Both are
      # log configuration, so every connection is told the same number.
      assert_received {:chunk, "retry: 15010\n\n"}
    end

    test "then sends the log as one frame per row", %{opts: opts, plug: plug} do
      Stub.put_rows(2, opts)

      stream(opts, plug, @whole_log)

      assert_received {:chunk, "retry: " <> _}
      assert_received {:chunk, write}
      assert write =~ "id: 1\nevent: order_placed\n"
      assert write =~ "id: 2\nevent: order_placed\n"
    end

    # The publisher's metadata reaches the socket byte for byte: nothing DMEx
    # knows is merged into it anywhere.
    test "splices metadata exactly as stored", %{opts: opts, plug: plug} do
      Stub.put_rows([Stub.row(1, metadata: ~s({"trace_id":"abc"}))], opts)

      stream(opts, plug, @whole_log)

      assert_received {:chunk, "retry: " <> _}
      assert_received {:chunk, write}

      assert write =~
               ~s(data: {"data":{"order_id":1},"metadata":{"trace_id":"abc"}}\n\n)
    end

    test "registers the request process for wakeups", %{opts: opts, plug: plug} do
      Stub.put_rows(1, opts)

      # Registered from inside the stream process — on the log side the
      # request *is* the stream, so the entry lives and dies with the connection.
      pid = start_stream(opts, plug, [], chunks: 5)

      eventually(fn -> Streams.count(StubLog) == 1 end)

      Process.exit(pid, :kill)

      eventually(fn -> Streams.count(StubLog) == 0 end)
    end

    test "emits a connect event with the resolved cursor", %{opts: opts, plug: plug} do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :stream, :connect]])
      Stub.put_rows(1, opts)

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
    # How a consumer that only cares about what happens next skips history: no
    # header, no preflight request, and no tip token to define.
    test "absent means from now on", %{opts: opts, plug: plug} do
      Stub.put_rows(10, opts)

      pid = start_stream(opts, plug, [], chunks: 5)

      assert_receive {:chunk, "retry: " <> _}
      eventually(fn -> Stub.reads(opts) != [] end)

      # The cursor is the tip, so the first read finds nothing and no data frame
      # is ever sent for the ten rows of history.
      assert [{10, _, :all} | _] = Stub.reads(opts)
      refute_receive {:chunk, "id: " <> _}, 50

      Process.exit(pid, :kill)
    end

    test "absent on an empty log is the whole log, because the tip is 0",
         %{opts: opts, plug: plug} do
      pid = start_stream(opts, plug, [], chunks: 5)

      eventually(fn -> Stub.reads(opts) != [] end)

      assert [{0, _, :all} | _] = Stub.reads(opts)

      Process.exit(pid, :kill)
    end

    test "0 is the whole log", %{opts: opts, plug: plug} do
      Stub.put_rows(3, opts)

      stream(opts, plug, @whole_log)

      assert [{0, _, :all} | _] = Stub.reads(opts)
    end

    test "an id resumes after it", %{opts: opts, plug: plug} do
      Stub.put_rows(5, opts)

      stream(opts, plug, [{"last-event-id", "3"}])

      assert [{3, _, :all} | _] = Stub.reads(opts)
    end

    # Rather than falling back to 0, which would turn a momentary pool
    # exhaustion into a whole-log replay — and every subscription with no cursor
    # yet arrives on this path, so it is the default, not an exotic case.
    test "a storage failure is a 503, not a silent replay", %{opts: opts, plug: plug} do
      Stub.fail_max_id(:pool_timeout, opts)

      conn = call(opts, plug, [])

      assert conn.status == 503
      assert Stub.reads(opts) == []
    end

    # Bandit and Cowboy reuse one process per *connection* across HTTP/1.1
    # keep-alive requests, so a 503 hands that process back to the server alive.
    # An entry left behind is armed for a stall nobody will ever clear, and the
    # watcher reclaims a connection that never streamed — taking the next,
    # healthy stream on the same process with it.
    test "a 503 leaves nothing registered", %{opts: opts, plug: plug} do
      Stub.fail_max_id(:pool_timeout, opts)
      parent = self()

      pid =
        spawn(fn ->
          StubLog.install(opts)

          conn = LogPlug.call(ClosingAdapter.conn(parent, 1, req_headers: []), plug)

          send(parent, {:responded, conn.status})

          receive do
            :stop -> :ok
          end
        end)

      on_exit(fn -> Process.exit(pid, :kill) end)

      assert_receive {:responded, 503}

      assert Streams.count(StubLog) == 0
    end

    # The sharper case: the socket is *healthy* and the loop returns anyway,
    # because the log could not be read. Nothing on the wire says so, and the
    # process goes back to serving requests.
    test "a read failure leaves nothing registered", %{opts: opts, plug: plug} do
      Stub.fail(:boom, opts)
      parent = self()

      pid =
        spawn(fn ->
          StubLog.install(opts)

          conn = LogPlug.call(ClosingAdapter.conn(parent, 5, req_headers: []), plug)

          send(parent, {:responded, conn.status})

          receive do
            :stop -> :ok
          end
        end)

      on_exit(fn -> Process.exit(pid, :kill) end)

      assert_receive {:responded, 200}

      assert Process.alive?(pid)
      assert Streams.count(StubLog) == 0
    end
  end

  ## Harness

  defp call(opts, plug, req_headers, path_info \\ []) do
    StubLog.install(opts)

    LogPlug.call(
      ClosingAdapter.conn(self(), 1, req_headers: req_headers, path_info: path_info),
      plug
    )
  end

  # Runs the plug to completion — the connection closes after `chunks` writes —
  # and returns the conn, so response headers are assertable.
  defp stream(opts, plug, req_headers, test_opts \\ []) do
    start_stream(opts, plug, req_headers, test_opts)

    assert_receive {:done, conn}, 2_000

    conn
  end

  defp start_stream(opts, plug, req_headers, test_opts) do
    parent = self()
    chunks = Keyword.get(test_opts, :chunks, 2)

    :ok = StubLog.install(opts, Keyword.get(test_opts, :config, []))
    exported = StubLog.export()

    conn =
      ClosingAdapter.conn(parent, chunks,
        req_headers: req_headers,
        path_info: Keyword.get(test_opts, :path_info, [])
      )

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
