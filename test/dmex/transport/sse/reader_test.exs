defmodule DMEx.Transport.SSE.ReaderTest do
  # The reader's whole contract with `DMEx.Handler.Subscription`, driven against
  # a raw socket: it delivers rows with a call, announces itself once, and
  # reports everything else by dying. No Bandit and no DMEx log — a hand-written
  # response is exactly what a third-party producer is.
  use DMEx.Case, async: true

  alias DMEx.Config
  alias DMEx.Transport.SSE

  @moduletag :capture_log

  setup do
    # The reader links to whoever starts it, and every test here outlives its
    # reader on purpose.
    Process.flag(:trap_exit, true)

    :ok
  end

  describe "delivering" do
    test "hands a decoded frame over as a call, and waits for it" do
      reader = start_reader(fn socket -> stream(socket, [frame(2)]) end)

      assert_receive {:"$gen_call", from, {:rows, [row]}}, 2_000
      assert row["id"] == 2

      GenServer.reply(from, :ok)

      assert_receive {:EXIT, ^reader, :closed}, 2_000
    end

    # What the parked-terminal protocol used to buy, now structural: the
    # response ending is *behind* the rows in one sequential loop, so the rows
    # are handed over before it is ever reached.
    test "delivers rows that arrive in the same packet as the close" do
      reader = start_reader(fn socket -> stream(socket, [frame(2), frame(3)], close: :same_packet) end)

      # Both frames and the end of the response arrive in one packet, so the
      # loop sees all three responses in one `Mint.HTTP.stream/2` — and still
      # hands each batch over, in order, before reaching the end.
      for id <- [2, 3] do
        assert_receive {:"$gen_call", from, {:rows, [row]}}, 2_000
        assert row["id"] == id

        GenServer.reply(from, :ok)
      end

      assert_receive {:EXIT, ^reader, :closed}, 2_000
    end

    test "announces the connection once, before any row" do
      start_reader(fn socket -> stream(socket, [frame(2)]) end)

      assert_receive {:"$gen_cast", :connected}, 2_000
      assert_receive {:"$gen_call", _from, {:rows, _}}, 2_000
    end
  end

  describe "HTTP status" do
    test "a 4xx this client caused is fatal" do
      reader = start_reader(fn socket -> respond(socket, 400) end)

      assert_receive {:EXIT, ^reader, {:fatal, {:bad_request, 400, url}}}, 2_000
      assert url =~ "127.0.0.1"
    end

    # The log knows none of these types *yet* — a deploy ordering, not a bug in
    # the request, so it is an ordinary disconnect and the backoff handles it.
    test "a 404 is an ordinary disconnect" do
      reader = start_reader(fn socket -> respond(socket, 404) end)

      assert_receive {:EXIT, ^reader, {:no_matching_types, _url}}, 2_000
    end

    test "any other status reconnects" do
      reader = start_reader(fn socket -> respond(socket, 503) end)

      assert_receive {:EXIT, ^reader, {:http_status, 503}}, 2_000
    end
  end

  describe "liveness" do
    # The deadline is the log's own advertised interval, so it is read off the
    # wire rather than assumed — and silence is measured in bytes, which is why
    # a producer that says `retry:` and then stops is dropped.
    test "a producer that goes silent past its advertised interval is dropped" do
      reader =
        start_reader(
          fn socket -> stream(socket, ["retry: 50\n\n"], close: :never) end,
          Config.handler!(missed_heartbeats: 1)
        )

      assert_receive {:EXIT, ^reader, {:silent_for, 50}}, 2_000
    end

    test "a connection that never opens dies with the socket's reason" do
      # Nothing is listening on this port: the connect itself is what fails.
      reader =
        SSE.start_reader(
          owner: self(),
          origin: "http://127.0.0.1:#{closed_port()}/stream",
          subscription: "test/echo",
          handler: __MODULE__,
          config: Config.handler!([]),
          cursor: 0
        )

      assert_receive {:EXIT, ^reader, %Mint.TransportError{reason: :econnrefused}}, 2_000
    end
  end

  ## Harness

  defp start_reader(serve, config \\ Config.handler!([])) do
    SSE.start_reader(
      owner: self(),
      origin: "http://127.0.0.1:#{listen(serve)}/stream",
      subscription: "test/echo",
      handler: __MODULE__,
      config: config,
      cursor: 0
    )
  end

  # One connection, served by hand: accept, swallow the request, run `serve`.
  defp listen(serve) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listener)

    spawn_link(fn ->
      {:ok, socket} = :gen_tcp.accept(listener)
      {:ok, _request} = :gen_tcp.recv(socket, 0)

      serve.(socket)

      :gen_tcp.close(listener)
    end)

    port
  end

  defp closed_port do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(listener)
    :ok = :gen_tcp.close(listener)

    port
  end

  defp respond(socket, status) do
    :ok = :gen_tcp.send(socket, "HTTP/1.1 #{status} \r\ncontent-length: 0\r\n\r\n")

    :gen_tcp.close(socket)
  end

  # Chunked, because that is what a live stream is. `:same_packet` puts the
  # terminating chunk in the same write as the frames, which is the case a
  # sequential reader has to get right.
  defp stream(socket, frames, opts \\ []) do
    :ok =
      :gen_tcp.send(
        socket,
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n"
      )

    body = Enum.map_join(frames, &chunk/1)

    case Keyword.get(opts, :close, :after) do
      :same_packet ->
        :ok = :gen_tcp.send(socket, body <> "0\r\n\r\n")
        :gen_tcp.close(socket)

      :after ->
        :ok = :gen_tcp.send(socket, body)
        :ok = :gen_tcp.send(socket, "0\r\n\r\n")
        :gen_tcp.close(socket)

      :never ->
        :ok = :gen_tcp.send(socket, body)

        # Held open, silent: the deadline is the thing under test.
        Process.sleep(5_000)
    end
  end

  defp chunk(payload), do: [payload |> byte_size() |> Integer.to_string(16), "\r\n", payload, "\r\n"]

  defp frame(id) do
    "id: #{id}\nevent: order_placed\ndata: {\"data\":{},\"metadata\":{}}\n\n"
  end
end
