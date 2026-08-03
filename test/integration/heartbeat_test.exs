defmodule DMEx.Integration.HeartbeatTest do
  @moduledoc """
  Keeping a quiet stream alive, and noticing when it is not.

  When a connection is caught up and the log is quiet, the log sends
  nothing for as long as nobody appends — potentially hours. Heartbeats do
  three jobs: they stop idle-connection timeouts in intermediaries from killing
  a healthy stream, they let a consumer tell "nothing new to send" from
  "connection is dead but the socket never closed", and they keep unacked data
  on the wire, which is what bounds how long a peer that vanished without a FIN
  can go unnoticed — the one stall the log's watcher cannot see.
  """

  use DMEx.IntegrationCase

  alias DMEx.Test.Handlers
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Server
  alias DMEx.Transport.SSE.Decoder

  @moduletag :capture_log

  @heartbeat 150

  defmodule Log do
    @moduledoc "Beats fast enough that a test does not sit through 15 seconds of it."
    use DMEx.Log,
      repo: DMEx.Test.IntegrationRepo,
      app: :dmex_heartbeat,
      heartbeat_ms: 150,
      poll_interval: 100
  end

  defmodule Router do
    @moduledoc false
    use Plug.Router

    plug(:match)
    plug(:dispatch)

    forward("/stream",
      to: DMEx.Log.Plug,
      init_opts: [log: DMEx.Integration.HeartbeatTest.Log]
    )

    match _ do
      send_resp(conn, 404, "not found")
    end
  end

  defmodule SilentPlug do
    @moduledoc """
    Opens a stream, advertises an interval, and then never writes again.

    A log whose loop wedged after `send_chunked`, or a connection an
    intermediary has quietly stopped forwarding. Nothing at the socket layer
    reports either.
    """
    @behaviour Plug

    @impl Plug
    def init(opts), do: opts

    @impl Plug
    def call(conn, _opts) do
      conn =
        conn
        |> Plug.Conn.put_resp_content_type("text/event-stream")
        |> Plug.Conn.send_chunked(200)

      {:ok, conn} = Plug.Conn.chunk(conn, "retry: 100\n\n")

      Process.sleep(:infinity)

      conn
    end
  end

  setup do
    Handlers.report_to(self())

    start_supervised!({Log, otp_app: :dmex})
    base = Server.start!(plug: Router)
    uri = URI.parse(base)

    %{url: base <> "/stream", host: uri.host, port: uri.port}
  end

  describe "what a raw client sees" do
    test "a quiet stream sends comment frames rather than nothing", ctx do
      body = collect(ctx, @heartbeat * 5)

      # Comment lines are ignored by every spec-compliant SSE client, so this
      # never pollutes the message stream or needs consumer-side handling.
      assert body =~ ": hb"
    end

    test "the advertised retry: is the effective interval, not the raw one", ctx do
      body = collect(ctx, 300)

      # heartbeat_ms + one batch interval, because the loop's throttle runs after the
      # heartbeat branch — that sum is the real upper bound on silence, and a
      # consumer measuring against heartbeat_ms alone would disconnect a healthy
      # log.
      assert body =~ "retry: #{effective_heartbeat()}"
    end

    test "beats repeatedly, not once", ctx do
      body = collect(ctx, @heartbeat * 6)

      assert length(String.split(body, ": hb")) - 1 >= 2
    end

    test "a stream with something to send sends the frame, not a heartbeat", ctx do
      {:ok, _} = Log.append(%OrderPlaced{order_id: 1})

      body = collect(ctx, 300)

      # The frame's own bytes are the liveness signal.
      assert body =~ "event: order_placed"
    end
  end

  describe "a DMEx consumer on a quiet stream" do
    setup ctx do
      start_supervised!(
        {DMEx.Supervisor,
         name: TestSupervisor,
         otp_app: nil,
         repo: DMEx.Test.IntegrationRepo,
         logs: Server.logs(ctx.url)}
      )

      :ok
    end

    test "stays connected through a long quiet period", _ctx do
      {:ok, [pid]} = subscribe()

      {:ok, _} = Log.append(%OrderPlaced{order_id: 1})
      assert_receive {:handled, %OrderPlaced{order_id: 1}, _}, 5_000

      ref = Process.monitor(pid)

      # Many heartbeat intervals of no messages at all. It must not reconnect
      # and it must not die — which is the entire reason heartbeats exist.
      refute_receive {:DOWN, ^ref, :process, ^pid, _}, @heartbeat * 10

      {:ok, _} = Log.append(%OrderPlaced{order_id: 2})
      assert_receive {:handled, %OrderPlaced{order_id: 2}, _}, 5_000
    end

    test "counts heartbeat bytes as activity, so silence means silence", _ctx do
      {:ok, [pid]} = subscribe()

      eventually(fn -> :sys.get_state(pid).source != nil end)
      before = :sys.get_state(pid).last_activity

      # Nothing is appended for the whole wait, yet activity advances: a
      # heartbeat comment is bytes, and bytes are what liveness measures.
      eventually(fn -> :sys.get_state(pid).last_activity != before end, 5_000, 50)
    end

    test "adopts the log's advertised interval as its deadline", _ctx do
      {:ok, [pid]} = subscribe()

      eventually(fn -> retry(pid) != nil end)

      # Read off the wire rather than assumed: the log's own heartbeat plus
      # its own batch interval, neither of which this consumer had any say in.
      assert retry(pid) == effective_heartbeat()
    end
  end

  describe "a log that opens a stream and then goes silent" do
    # Exactly the failure heartbeats exist to expose, and the one no other
    # mechanism can see: the socket stays open, nothing closes, and nothing ever
    # arrives. Dropping it costs only a re-read from the durable cursor.
    test "is dropped and reconnected once the deadline passes" do
      base = Server.start!(plug: SilentPlug)

      start_supervised!(
        {DMEx.Supervisor,
         name: SilentHandler,
         otp_app: nil,
         repo: DMEx.Test.IntegrationRepo,
         logs: Server.logs(base <> "/stream")}
      )

      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :handler, :reconnect]])

      {:ok, _pid} =
        DMEx.Supervisor.subscribe(Handlers.Echo,
          supervisor: SilentHandler,
          missed_heartbeats: 1
        )

      assert_receive {[:dmex, :handler, :reconnect], ^ref, measurements, metadata}, 15_000
      assert {:silent_for, silent_for} = metadata.reason
      # It advertised 100ms and one missed interval was allowed.
      assert silent_for >= 100
      assert measurements.attempt == 1
    end

    test "keeps reconnecting rather than giving up", _ctx do
      base = Server.start!(plug: SilentPlug)

      start_supervised!(
        {DMEx.Supervisor,
         name: PersistentHandler,
         otp_app: nil,
         repo: DMEx.Test.IntegrationRepo,
         logs: Server.logs(base <> "/stream")}
      )

      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :handler, :reconnect]])

      {:ok, [pid]} =
        DMEx.Supervisor.subscribe(Handlers.Echo,
          supervisor: PersistentHandler,
          missed_heartbeats: 1,
          reconnect_backoff: 50,
          reconnect_backoff_max: 100
        )

      assert_receive {[:dmex, :handler, :reconnect], ^ref, _, _}, 15_000
      assert_receive {[:dmex, :handler, :reconnect], ^ref, _, _}, 15_000

      assert Process.alive?(pid)
    end
  end

  ## Helpers

  # `heartbeat_ms` plus one batch interval. Derived rather than written out,
  # because the batch interval follows `max_batch_frequency`'s default: a
  # literal here states the right thing only until that default moves, and then
  # states the wrong one loudly.
  defp effective_heartbeat do
    @heartbeat + DMEx.Config.batch_interval(Log.__dmex__().config)
  end

  # The advertised interval lives in the transport's decoder now — the
  # subscription itself only asks it how long silence may last.
  defp retry(pid) do
    case :sys.get_state(pid).source do
      %{decoder: decoder} -> Decoder.retry(decoder)
      nil -> nil
    end
  end

  defp subscribe do
    DMEx.Supervisor.subscribe(Handlers.Echo, supervisor: TestSupervisor)
  end

  # A bare Mint client that reads for a while and returns whatever arrived.
  defp collect(ctx, duration) do
    {:ok, conn} = Mint.HTTP.connect(:http, ctx.host, ctx.port, mode: :active)
    {:ok, conn, ref} = Mint.HTTP.request(conn, "GET", "/stream", [], nil)

    deadline = System.monotonic_time(:millisecond) + duration

    conn
    |> read_until(ref, deadline, [])
    |> IO.iodata_to_binary()
  end

  defp read_until(conn, ref, deadline, body) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      message ->
        case Mint.HTTP.stream(conn, message) do
          {:ok, conn, responses} ->
            body =
              Enum.reduce(responses, body, fn
                {:data, ^ref, chunk}, acc -> [acc, chunk]
                _other, acc -> acc
              end)

            read_until(conn, ref, deadline, body)

          _other ->
            read_until(conn, ref, deadline, body)
        end
    after
      timeout ->
        Mint.HTTP.close(conn)
        body
    end
  end
end
