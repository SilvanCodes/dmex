defmodule DMEx.Integration.HeaderNegotiationTest do
  @moduledoc """
  The HTTP contract, exercised by a raw client rather than by DMEx's own.

  Because the protocol is just HTTP plus SSE, any SSE-capable client can consume
  a DMEx stream. These tests speak to the producer with a bare Mint connection —
  no DMEx consumer anywhere — so what they assert is the wire contract a
  third-party client would be written against.
  """

  use DMEx.IntegrationCase

  alias DMEx.Test.IntegrationProducer, as: Producer
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Server

  @moduletag :capture_log

  setup do
    start_supervised!(Producer)

    uri = URI.parse(Server.start!())

    %{host: uri.host, port: uri.port}
  end

  describe "a well-formed request" do
    test "answers 200 with the ordinary SSE headers", ctx do
      {status, headers, _body} = request(ctx, [])

      assert status == 200
      assert header(headers, "content-type") =~ "text/event-stream"
      assert header(headers, "cache-control") == "no-cache"
      assert header(headers, "x-accel-buffering") == "no"
    end

    test "echoes the producer's defaults to a client that asks for nothing", ctx do
      # Which is exactly what a browser EventSource gets, permanently: it can set
      # no request headers of its own beyond Last-Event-ID.
      {200, headers, _} = request(ctx, [])

      assert header(headers, "dmex-batch-window") == "200"
      assert header(headers, "dmex-max-batch-size") == "500"
    end

    test "opens the stream with a retry: frame carrying the effective heartbeat", ctx do
      {200, _headers, body} = request(ctx, [{"dmex-batch-window", "300"}])

      # heartbeat_ms + batch_window, since the throttle runs after the heartbeat
      # branch and the sum is the real upper bound on silence.
      assert body =~ "retry: 15300"
    end
  end

  describe "pacing is clamped, not rejected" do
    test "a request below the floor comes back at the floor", ctx do
      {status, headers, _} = request(ctx, [{"dmex-batch-window", "1"}])

      assert status == 200
      assert header(headers, "dmex-batch-window") == "50"
    end

    test "a request above the ceiling comes back at the ceiling", ctx do
      {status, headers, _} = request(ctx, [{"dmex-max-batch-size", "999999"}])

      assert status == 200
      assert header(headers, "dmex-max-batch-size") == "5000"
    end

    test "the echo is what the producer applied, so a client can see it was capped", ctx do
      {200, headers, _} =
        request(ctx, [{"dmex-batch-window", "999999"}, {"dmex-max-batch-size", "0"}])

      assert header(headers, "dmex-batch-window") == "5000"
      assert header(headers, "dmex-max-batch-size") == "1"
    end
  end

  describe "resumption" do
    setup ctx do
      for n <- 1..5, do: {:ok, _} = Producer.publish(%OrderPlaced{order_id: n})

      ctx
    end

    test "no header streams the whole log", ctx do
      {200, _, body} = request(ctx, [])

      assert body =~ ~r/"order_id":\s*1\b/
      assert body =~ ~r/"order_id":\s*5\b/
    end

    test "an id resumes after it", ctx do
      {200, _, body} = request(ctx, [{"last-event-id", "3"}])

      refute body =~ ~r/"order_id":\s*3\b/
      assert body =~ ~r/"order_id":\s*4\b/
      assert body =~ ~r/"order_id":\s*5\b/
    end

    # The documented cost of storing payloads as jsonb, visible on the wire: the
    # bytes a consumer receives are Postgres's normalized rendering, not the
    # bytes the publisher encoded. Key order is not preserved and whitespace
    # differs, so nothing may hash or sign a payload end to end.
    test "payloads arrive as jsonb normalized them, not as the publisher encoded them", ctx do
      {200, _, body} = request(ctx, [])

      # JSON.encode! would have produced compact, struct-ordered output.
      assert body =~ ~s("order_id": )
      refute body =~ ~s({"order_id":)

      # Still valid JSON, and still the same values — which is all the wire
      # contract promises.
      [_retry, frame | _] = String.split(body, "\n\n", trim: true)
      {:ok, parsed} = DMEx.Frame.parse(frame)

      assert Enum.map(parsed.messages, & &1["data"]["order_id"]) == [1, 2, 3, 4, 5]
    end

    test "`latest` skips history entirely", ctx do
      {200, _, body} = request(ctx, [{"last-event-id", "latest"}])

      # A consumer that only cares about what happens from now on, with no
      # preflight request and no second parameter.
      refute body =~ "event: batch"
      assert body =~ "retry: "
    end
  end

  describe "a malformed request is a 400" do
    test "a Last-Event-ID outside the vocabulary", ctx do
      for value <- ["-1", "1.0", "abc", "LATEST", "", "42abc"] do
        assert {400, _headers, body} = request(ctx, [{"last-event-id", value}]),
               "expected #{inspect(value)} to be rejected"

        assert body =~ "invalid last-event-id header"
      end
    end

    test "a non-integer pacing header", ctx do
      assert {400, _, body} = request(ctx, [{"dmex-batch-window", "fast"}])
      assert body =~ "invalid dmex-batch-window header"

      assert {400, _, body} = request(ctx, [{"dmex-max-batch-size", "-1"}])
      assert body =~ "invalid dmex-max-batch-size header"
    end

    test "a 400 is text/plain and opens no stream", ctx do
      {400, headers, _} = request(ctx, [{"last-event-id", "nope"}])

      assert header(headers, "content-type") =~ "text/plain"
      refute header(headers, "content-type") =~ "event-stream"
    end
  end

  ## A minimal SSE client

  # Reads for `collect_for` ms and returns whatever the producer sent, which is
  # how a streaming response is observed without waiting for an end that never
  # comes.
  defp request(ctx, headers, collect_for \\ 400) do
    {:ok, conn} = Mint.HTTP.connect(:http, ctx.host, ctx.port, mode: :active)

    {:ok, conn, ref} =
      Mint.HTTP.request(conn, "GET", "/stream", [{"accept", "text/event-stream"}] ++ headers, nil)

    deadline = System.monotonic_time(:millisecond) + collect_for
    result = receive_loop(conn, ref, deadline, nil, [], [])

    result
  end

  defp receive_loop(conn, ref, deadline, status, headers, body) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      message ->
        case Mint.HTTP.stream(conn, message) do
          {:ok, conn, responses} ->
            {status, headers, body, done?} =
              apply_responses(responses, ref, status, headers, body)

            if done? do
              {status, headers, IO.iodata_to_binary(body)}
            else
              receive_loop(conn, ref, deadline, status, headers, body)
            end

          {:error, _conn, _reason, _} ->
            {status, headers, IO.iodata_to_binary(body)}

          :unknown ->
            receive_loop(conn, ref, deadline, status, headers, body)
        end
    after
      timeout ->
        Mint.HTTP.close(conn)
        {status, headers, IO.iodata_to_binary(body)}
    end
  end

  defp apply_responses(responses, ref, status, headers, body) do
    Enum.reduce(responses, {status, headers, body, false}, fn
      {:status, ^ref, value}, {_s, h, b, d} -> {value, h, b, d}
      {:headers, ^ref, value}, {s, _h, b, d} -> {s, value, b, d}
      {:data, ^ref, chunk}, {s, h, b, d} -> {s, h, [b, chunk], d}
      {:done, ^ref}, {s, h, b, _d} -> {s, h, b, true}
      _other, acc -> acc
    end)
  end

  defp header(headers, name) do
    case List.keyfind(headers, name, 0) do
      {^name, value} -> value
      nil -> nil
    end
  end
end
