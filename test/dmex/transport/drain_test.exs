defmodule DMEx.Transport.DrainTest do
  use DMEx.Case, async: true

  alias DMEx.Config
  alias DMEx.Transport.Local
  alias DMEx.Transport.SSE

  # A listening socket completes the TCP handshake on its own backlog, so Mint
  # hands back a live connection with nothing on the other end to serve it —
  # which is all these tests need, since none of them exchanges a byte.
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

  describe "the two questions" do
    # The point of the split: what one message parked is asked for by name, not
    # by sending the transport a token dressed as a message.
    test "a parked terminal is answered by drain/1", %{source: source} do
      parked = %{source | pending: [{:done, source.ref}]}

      assert {:disconnect, :closed, _source} = SSE.drain(parked)
    end

    # The token used to travel through `handle_message/2`, so a log that
    # genuinely sent this atom would have been read as "give me what you
    # parked". Now it is just an atom nobody claims.
    test "the atom :continue arriving as a message is an unknown message", %{source: source} do
      parked = %{source | pending: [{:done, source.ref}]}

      assert SSE.handle_message(:continue, parked) == :unknown
      assert Local.handle_message(:continue, %{log: nil, reader: self()}) == :unknown
    end

    test "nothing parked drains to :unknown", %{source: source} do
      assert SSE.drain(source) == :unknown
      assert Local.drain(%{log: nil, reader: self()}) == :unknown
    end
  end
end
