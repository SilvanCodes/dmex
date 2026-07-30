defmodule DMEx.Integration.HeartbeatTest do
  @moduledoc """
  Keeping a quiet stream alive, and noticing when it is not.

  When a connection is caught up and the log is quiet, the producer sends
  nothing for as long as nobody publishes — potentially hours. Heartbeats do
  three jobs: they stop idle-connection timeouts in intermediaries from killing
  a healthy stream, they let a consumer tell "nothing new to send" from
  "connection is dead but the socket never closed", and they keep unacked data
  on the wire, which is what bounds how long a peer that vanished without a FIN
  can go unnoticed — the one stall the producer's watcher cannot see.
  """

  use DMEx.IntegrationCase

  alias DMEx.Consumer
  alias DMEx.SSE.Decoder
  alias DMEx.Test.Handlers
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Server

  @moduletag :capture_log

  @heartbeat 150

  defmodule Producer do
    @moduledoc "Beats fast enough that a test does not sit through 15 seconds of it."
    use DMEx.Producer,
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
      to: DMEx.Producer.Plug,
      init_opts: [producer: DMEx.Integration.HeartbeatTest.Producer]
    )

    match _ do
      send_resp(conn, 404, "not found")
    end
  end

  defmodule SilentPlug do
    @moduledoc """
    Opens a stream, advertises an interval, and then never writes again.

    A producer whose loop wedged after `send_chunked`, or a connection an
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

    start_supervised!(Producer)
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
      # producer.
      assert body =~ "retry: #{@heartbeat + 200}"
    end

    test "beats repeatedly, not once", ctx do
      body = collect(ctx, @heartbeat * 6)

      assert length(String.split(body, ": hb")) - 1 >= 2
    end

    test "a stream with something to send sends the frame, not a heartbeat", ctx do
      {:ok, _} = Producer.publish(%OrderPlaced{order_id: 1})

      body = collect(ctx, 300)

      # The frame's own bytes are the liveness signal.
      assert body =~ "event: DMEx.Test.Messages.OrderPlaced"
    end
  end

  describe "a DMEx consumer on a quiet stream" do
    setup ctx do
      start_supervised!(
        {Consumer,
         name: TestConsumer, repo: DMEx.Test.IntegrationRepo, producer: Server.producers(ctx.url)}
      )

      :ok
    end

    test "stays connected through a long quiet period", _ctx do
      {:ok, pid} = subscribe()

      {:ok, _} = Producer.publish(%OrderPlaced{order_id: 1})
      assert_receive {:handled, %OrderPlaced{order_id: 1}, _}, 5_000

      ref = Process.monitor(pid)

      # Many heartbeat intervals of no messages at all. It must not reconnect
      # and it must not die — which is the entire reason heartbeats exist.
      refute_receive {:DOWN, ^ref, :process, ^pid, _}, @heartbeat * 10

      {:ok, _} = Producer.publish(%OrderPlaced{order_id: 2})
      assert_receive {:handled, %OrderPlaced{order_id: 2}, _}, 5_000
    end

    test "counts heartbeat bytes as activity, so silence means silence", _ctx do
      {:ok, pid} = subscribe()

      eventually(fn -> :sys.get_state(pid).conn != nil end)
      before = :sys.get_state(pid).last_activity

      # Nothing is published for the whole wait, yet activity advances: a
      # heartbeat comment is bytes, and bytes are what liveness measures.
      eventually(fn -> :sys.get_state(pid).last_activity != before end, 5_000, 50)
    end

    test "adopts the producer's advertised interval as its deadline", _ctx do
      {:ok, pid} = subscribe()

      eventually(fn -> Decoder.retry(:sys.get_state(pid).decoder) != nil end)

      # Read off the wire rather than assumed: the producer's own heartbeat plus
      # its own batch interval, neither of which this consumer had any say in.
      assert Decoder.retry(:sys.get_state(pid).decoder) == @heartbeat + 200
    end
  end

  describe "a producer that opens a stream and then goes silent" do
    # Exactly the failure heartbeats exist to expose, and the one no other
    # mechanism can see: the socket stays open, nothing closes, and nothing ever
    # arrives. Dropping it costs only a re-read from the durable cursor.
    test "is dropped and reconnected once the deadline passes" do
      base = Server.start!(plug: SilentPlug)

      start_supervised!(
        {Consumer,
         name: SilentConsumer,
         repo: DMEx.Test.IntegrationRepo,
         producer: Server.producers(base <> "/stream")}
      )

      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :consumer, :reconnect]])

      {:ok, _pid} =
        Consumer.subscribe(Handlers.Echo,
          consumer: SilentConsumer,
          missed_heartbeats: 1
        )

      assert_receive {[:dmex, :consumer, :reconnect], ^ref, measurements, metadata}, 15_000
      assert {:silent_for, silent_for} = metadata.reason
      # It advertised 100ms and one missed interval was allowed.
      assert silent_for >= 100
      assert measurements.attempt == 1
    end

    test "keeps reconnecting rather than giving up", _ctx do
      base = Server.start!(plug: SilentPlug)

      start_supervised!(
        {Consumer,
         name: PersistentConsumer,
         repo: DMEx.Test.IntegrationRepo,
         producer: Server.producers(base <> "/stream")}
      )

      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :consumer, :reconnect]])

      {:ok, pid} =
        Consumer.subscribe(Handlers.Echo,
          consumer: PersistentConsumer,
          missed_heartbeats: 1,
          reconnect_backoff: 50,
          reconnect_backoff_max: 100
        )

      assert_receive {[:dmex, :consumer, :reconnect], ^ref, _, _}, 15_000
      assert_receive {[:dmex, :consumer, :reconnect], ^ref, _, _}, 15_000

      assert Process.alive?(pid)
    end
  end

  ## Helpers

  defp subscribe do
    Consumer.subscribe(Handlers.Echo, consumer: TestConsumer)
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
