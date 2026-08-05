defmodule DMEx.Transport.SSE.ParkedTerminalTest do
  use DMEx.Case, async: true

  alias DMEx.Config
  alias DMEx.Transport.Local
  alias DMEx.Transport.SSE

  # A listening socket completes the TCP handshake on its own backlog, so Mint
  # hands back a live connection with nothing on the other end to serve it —
  # enough to build a real source, since nothing here exchanges a byte.
  setup do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    {:ok, port} = :inet.port(listener)

    {:ok, source} =
      SSE.connect(
        origin: "http://127.0.0.1:#{port}/stream",
        subscription: "test/echo",
        handler: __MODULE__,
        config: Config.handler!([]),
        cursor: 1
      )

    on_exit(fn ->
      SSE.close(source)
      :gen_tcp.close(listener)
    end)

    %{source: source}
  end

  describe "a message carrying rows and a terminal" do
    test "yields the rows ahead of the terminal, then the terminal", %{source: source} do
      events = [{:data, source.ref, frame(2)}, {:done, source.ref}]

      assert {:rows, [row], parked} = SSE.handle_message(:continue, %{source | pending: events})
      assert row["id"] == 2

      assert {:disconnect, :closed, _source} = SSE.handle_message(:continue, parked)
    end

    # Collecting the terminal hands back the terminal alone, never the rows a
    # second time.
    test "does not re-yield the rows it already handed over", %{source: source} do
      events = [{:data, source.ref, frame(2)}, {:done, source.ref}]

      assert {:rows, [_row], parked} = SSE.handle_message(:continue, %{source | pending: events})
      assert {:disconnect, :closed, _source} = SSE.handle_message(:continue, parked)
    end

    # Everything after the terminal goes with it — collecting halts on the
    # terminal before reaching any of it.
    test "drops the events behind the terminal", %{source: source} do
      events = [{:data, source.ref, frame(2)}, {:done, source.ref}, {:data, source.ref, frame(3)}]

      assert {:rows, [row], parked} = SSE.handle_message(:continue, %{source | pending: events})
      assert row["id"] == 2

      assert {:disconnect, :closed, _source} = SSE.handle_message(:continue, parked)
    end

    # A terminal with no rows ahead of it needs no parking at all.
    test "returns a terminal directly when no rows precede it", %{source: source} do
      assert {:disconnect, :closed, _source} =
               SSE.handle_message(:continue, %{source | pending: [{:done, source.ref}]})
    end
  end

  # Otherwise the stream re-arms its liveness timer once per batch for nothing.
  describe "nothing parked" do
    test "is :unknown, for both transports", %{source: source} do
      assert SSE.handle_message(:continue, source) == :unknown
      assert Local.handle_message(:continue, %{log: nil, reader: self()}) == :unknown
    end
  end

  defp frame(id) do
    "id: #{id}\nevent: order_placed\ndata: {\"data\":{},\"metadata\":{}}\n\n"
  end
end
