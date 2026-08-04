defmodule DMEx.Integration.StallReclaimTest do
  @moduledoc """
  A consumer that stops reading is reclaimed, with no transport configuration.

  This is the failure the deployment guide used to hand to the operator:
  `send_timeout` on the web server's listener, set correctly, or a wedged
  request process held forever. Here the listener is left alone — the only
  socket options set are the small buffers that make the stall arrive in
  milliseconds rather than megabytes — and `DMEx.Log.Watcher` is what
  reclaims the connection.
  """

  use DMEx.IntegrationCase

  alias DMEx.Log.Watcher
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.PatientLog, as: Patient
  alias DMEx.Test.StallLog, as: Log

  @stall_timeout 300
  @check_interval 50

  defmodule Router do
    @moduledoc false
    use Plug.Router

    plug(:match)
    plug(:dispatch)

    forward("/stream", to: DMEx.Log.Plug, log: Log)

    match _ do
      send_resp(conn, 404, "not found")
    end
  end

  setup do
    start_supervised!(Log)

    {:ok, server} =
      Bandit.start_link(
        plug: Router,
        scheme: :http,
        ip: {127, 0, 0, 1},
        port: 0,
        startup_log: false,
        # Small buffers only, so a consumer that stops reading fills the pipe in
        # kilobytes. Deliberately *no* `send_timeout`: that is the configuration
        # this test exists to prove unnecessary.
        thousand_island_options: [transport_options: [sndbuf: 2_048, buffer: 2_048]]
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)

    on_exit(fn ->
      ref = Process.monitor(server)
      Process.exit(server, :normal)
      receive do: ({:DOWN, ^ref, _, _, _} -> :ok), after: (1_000 -> :ok)
    end)

    %{port: port}
  end

  test "a consumer that stops reading is reclaimed, with no socket options set", ctx do
    ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :stream, :reclaim]])

    socket = connect(ctx.port)

    eventually(fn -> DMEx.Log.Registry.count(Log) == 1 end)

    # Enough bytes that the consumer's receive window closes and the log's
    # write blocks with nothing able to time it out from inside.
    append(200)

    assert_receive {[:dmex, :stream, :reclaim], ^ref, %{overdue_by: overdue_by}, %{log: Log}},
                   @stall_timeout + @check_interval + 2_000

    assert overdue_by >= 0

    # The request process died, so its registry entry went with it — and the
    # socket closed, which is the only notification a stalled consumer can get.
    eventually(fn -> DMEx.Log.Registry.count(Log) == 0 end)
    assert eventually(fn -> :gen_tcp.recv(socket, 0, 100) == {:error, :closed} end)
  end

  test "a consumer that keeps reading is left alone", ctx do
    ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :stream, :reclaim]])

    ctx.port |> connect() |> read_in_background()

    eventually(fn -> DMEx.Log.Registry.count(Log) == 1 end)

    # Reading happens in its own process, so the consumer keeps draining while
    # this one appends. A test that read only afterwards would be a stalled
    # consumer for the duration of the append loop.
    append(200)
    Process.sleep(@stall_timeout * 3)

    refute_received {[:dmex, :stream, :reclaim], ^ref, _, _}
    assert DMEx.Log.Registry.count(Log) == 1
  end

  describe "in the supervision tree" do
    test "a log with a stall timeout gets a watcher" do
      assert is_pid(Process.whereis(Watcher.name(Log)))
    end

    test "a log that opted out does not" do
      start_supervised!(Patient)

      refute Process.whereis(Watcher.name(Patient))
    end
  end

  ## Harness

  defp connect(port) do
    {:ok, socket} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, recbuf: 2_048])

    :ok = :gen_tcp.send(socket, "GET /stream HTTP/1.1\r\nhost: localhost\r\n\r\n")

    socket
  end

  defp append(count) do
    # ~1 KB per message, so the frame cap is hit repeatedly rather than once.
    note = String.duplicate("x", 1_000)

    for id <- 1..count do
      {:ok, _} = Log.append(%OrderPlaced{order_id: id, customer_id: note, total: 1})
    end
  end

  defp read_in_background(socket) do
    reader = spawn(fn -> read_forever(socket) end)
    :ok = :gen_tcp.controlling_process(socket, reader)

    on_exit(fn -> Process.exit(reader, :kill) end)

    reader
  end

  defp read_forever(socket) do
    case :gen_tcp.recv(socket, 0, :infinity) do
      {:ok, _bytes} -> read_forever(socket)
      {:error, _reason} -> :ok
    end
  end
end
