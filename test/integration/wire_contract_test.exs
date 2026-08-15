defmodule DMEx.Integration.WireContractTest do
  @moduledoc """
  The HTTP contract, exercised by a raw client rather than by DMEx's own.

  Because the protocol is just HTTP plus SSE, any SSE-capable client can consume
  a DMEx stream. These tests speak to the log with a bare Mint connection —
  no DMEx consumer anywhere — so what they assert is what a third-party client
  receives through Bandit and a real socket.

  Only what needs the socket lives here. The header set, the `retry:` frame and
  the `Last-Event-ID` vocabulary are pinned against the plug directly, in
  `DMEx.Log.PlugTest` and `DMEx.Log.ParamsTest`.
  """

  use DMEx.IntegrationCase

  alias DMEx.Test.IntegrationLog, as: Log
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Server
  alias DMEx.Test.SSEClient
  alias DMEx.Transport.SSE.Frame

  setup do
    start_supervised!(Log)

    uri = URI.parse(Server.start!())

    %{host: uri.host, port: uri.port}
  end

  # A browser EventSource sets no request headers of its own beyond
  # Last-Event-ID, and it needs none: there is nothing DMEx-specific in either
  # direction.
  test "answers 200 with the ordinary SSE headers and nothing DMEx-specific", ctx do
    {status, headers, _body} = request(ctx, [])

    assert status == 200
    assert SSEClient.header(headers, "content-type") =~ "text/event-stream"
    assert SSEClient.header(headers, "cache-control") == "no-cache"
    assert SSEClient.header(headers, "x-accel-buffering") == "no"

    # No exemption: this is a bare `GET /stream`, so `dmex-matched-types`
    # is not set either — and exempting the one header the plug can emit would
    # blind the assertion to exactly what it is named for. Its presence on a
    # selective stream is asserted in `selection_test.exs`.
    assert Enum.all?(headers, fn {name, _} -> not String.starts_with?(name, "dmex-") end)
  end

  # The documented cost of storing payloads as jsonb, visible on the wire: the
  # bytes a consumer receives are Postgres's normalized rendering, not the
  # bytes the publisher encoded. Key order is not preserved and whitespace
  # differs, so nothing may hash or sign a payload end to end.
  test "payloads arrive as jsonb normalized them, not as the publisher encoded them", ctx do
    for n <- 1..5, do: {:ok, _} = Log.append(%OrderPlaced{order_id: n})

    {200, _, body} = request(ctx, [{"last-event-id", "0"}])

    # JSON.encode! would have produced compact, struct-ordered output.
    assert body =~ ~s("order_id": )
    refute body =~ ~s({"order_id":)

    # Still valid JSON, and still the same values — which is all the wire
    # contract promises.
    [_retry | frames] = String.split(body, "\n\n", trim: true)

    order_ids =
      for frame <- frames,
          {:ok, {:message, row}} = Frame.decode(frame),
          do: row["data"]["order_id"]

    assert order_ids == [1, 2, 3, 4, 5]
  end

  # Reads for a window and returns whatever the log sent, which is how a
  # streaming response is observed without waiting for an end that never comes.
  defp request(ctx, headers) do
    SSEClient.request(ctx.host, ctx.port, "/stream", [{"accept", "text/event-stream"}] ++ headers, timeout: 400)
  end
end
