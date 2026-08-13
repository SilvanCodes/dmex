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
  alias DMEx.Test.HeartbeatLog, as: Log
  alias DMEx.Test.IntegrationRepo
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Server

  @heartbeat 150

  defmodule Router do
    @moduledoc false
    use Plug.Router

    plug(:match)
    plug(:dispatch)

    forward("/stream", to: DMEx.Log.Plug, log_module: Log)

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
    # `supervisor: false`, because only the describe below wants a DMEx consumer
    # attached — the raw-client tests must be the only reader on the stream.
    start_stack!(log: Log, plug: Router, supervisor: false)
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

      # heartbeat_interval + one batch interval, because the loop's throttle runs after the
      # heartbeat branch — that sum is the real upper bound on silence, and a
      # consumer measuring against heartbeat_interval alone would disconnect a healthy
      # log.
      assert body =~ "retry: #{effective_heartbeat()}"
    end

    test "beats repeatedly, not once", ctx do
      body = collect(ctx, @heartbeat * 6)

      assert length(String.split(body, ": hb")) - 1 >= 2
    end

    test "a stream with something to send sends the frame, not a heartbeat", ctx do
      {:ok, _} = Log.append(%OrderPlaced{order_id: 1})

      body = collect(ctx, 300, [{"last-event-id", "0"}])

      # The frame's own bytes are the liveness signal.
      assert body =~ "event: order_placed"
    end
  end

  describe "a DMEx consumer on a quiet stream" do
    setup ctx do
      start_supervisor!(Server.bindings(ctx.url))

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

    # The deadline itself is the reader's, and `DMEx.Transport.SSE.ReaderTest`
    # pins that it is the *advertised* interval rather than the assumed one.
    # What is only visible from here is that heartbeats keep it from expiring.
    test "counts heartbeat bytes as activity, so silence means silence", _ctx do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :handler, :reconnect]])

      {:ok, [pid]} = subscribe()

      reader = eventually(fn -> :sys.get_state(pid).reader end)

      # Nothing is appended for many deadlines, yet the connection is never
      # dropped: a heartbeat comment is bytes, and bytes are what liveness
      # measures. A reader that measured messages would be replaced by now.
      refute_receive {[:dmex, :handler, :reconnect], ^ref, _, _}, @heartbeat * 10

      assert :sys.get_state(pid).reader == reader
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
         name: SilentHandler, otp_app: nil, repo: IntegrationRepo, bindings: Server.bindings(base <> "/stream")}
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
         name: PersistentHandler, otp_app: nil, repo: IntegrationRepo, bindings: Server.bindings(base <> "/stream")}
      )

      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :handler, :reconnect]])

      {:ok, [pid]} =
        DMEx.Supervisor.subscribe(Handlers.Echo,
          supervisor: PersistentHandler,
          missed_heartbeats: 1,
          reconnect_backoff_base: 50,
          reconnect_backoff_max: 100
        )

      assert_receive {[:dmex, :handler, :reconnect], ^ref, _, _}, 15_000
      assert_receive {[:dmex, :handler, :reconnect], ^ref, _, _}, 15_000

      assert Process.alive?(pid)
    end
  end

  ## Helpers

  # `heartbeat_interval` plus one batch interval. Derived rather than written out,
  # because the batch interval follows `max_batch_frequency`'s default: a
  # literal here states the right thing only until that default moves, and then
  # states the wrong one loudly.
  defp effective_heartbeat do
    @heartbeat + DMEx.Config.batch_interval(Log.__dmex__().config)
  end

  # The advertised interval lives in the transport's decoder now — the
  # subscription itself only asks it how long silence may last.
  # `last_seen: 0` because these tests append *after* subscribing, and the
  # default resolves to the log's tip at connect. The log is empty until they
  # append, so the two name the same rows without racing the connect.
  defp subscribe do
    DMEx.Supervisor.subscribe(Handlers.Echo, supervisor: TestSupervisor, last_seen: 0)
  end

  # A bare Mint client that reads for a while and returns whatever arrived.
  # `headers` is where a test asks for history: with none, an absent
  # `last-event-id` means "from now on" and a stream over an existing log has
  # nothing to send.
  defp collect(ctx, duration, headers \\ []) do
    {:ok, conn} = Mint.HTTP.connect(:http, ctx.host, ctx.port, mode: :active)
    {:ok, conn, ref} = Mint.HTTP.request(conn, "GET", "/stream", headers, nil)

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
