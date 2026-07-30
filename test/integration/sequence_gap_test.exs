defmodule DMEx.Integration.SequenceGapTest do
  @moduledoc """
  A message that goes missing on the wire must not be stepped over.

  This is the one loss the cursor model cannot see by itself. The cursor is the
  only record of what a subscription processed, and it advances to the id of
  whatever row it just handled — so if the row before that never arrived, its id
  is behind the cursor from then on, it will never be re-read, and nothing
  anywhere records that it existed. Silent, permanent, and indistinguishable
  from the producer having published nothing.

  The producer therefore states each frame's predecessor and the consumer checks
  it. These tests serve a stream with a genuine hole in it — a hand-written plug
  rather than a real producer, because a correct producer cannot produce one.
  """

  use DMEx.IntegrationCase

  alias DMEx.Consumer
  alias DMEx.Storage.Postgres
  alias DMEx.Test.Handlers
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Server

  @moduletag :capture_log

  # Frame 2 is simply never written. Frame 3 chains honestly — it says its
  # predecessor was 2 — which is exactly what a producer sends and exactly what
  # the consumer must notice it never received.
  defmodule GappedStream do
    @moduledoc false
    @behaviour Plug

    @impl Plug
    def init(opts), do: opts

    @impl Plug
    def call(conn, _opts) do
      conn =
        conn
        |> Plug.Conn.put_resp_content_type("text/event-stream")
        |> Plug.Conn.send_chunked(200)

      {:ok, conn} = Plug.Conn.chunk(conn, "retry: 60000\n\n")
      {:ok, conn} = Plug.Conn.chunk(conn, frame(0, 1))
      {:ok, conn} = Plug.Conn.chunk(conn, frame(2, 3))

      # Held open so the consumer's own reaction is what ends it, not EOF.
      Process.sleep(30_000)

      conn
    end

    defp frame(previous, id) do
      "id: #{previous}/#{id}\n" <>
        "event: DMEx.Test.Messages.OrderPlaced\n" <>
        ~s(data: {"data":{"order_id":#{id}},"metadata":{},) <>
        ~s("inserted_at":"2026-07-27T10:31:04.221Z"}\n\n)
    end
  end

  defmodule GappedRouter do
    @moduledoc false
    use Plug.Router

    plug(:match)
    plug(:dispatch)

    forward("/stream", to: DMEx.Integration.SequenceGapTest.GappedStream)

    match _ do
      send_resp(conn, 404, "not found")
    end
  end

  setup do
    Handlers.report_to(self())

    url = Server.start!(plug: GappedRouter) <> "/stream"

    start_supervised!(
      {Consumer,
       name: TestConsumer, repo: DMEx.Test.IntegrationRepo, producer: Server.producers(url)}
    )

    %{url: url}
  end

  test "a frame that never arrives breaks the chain instead of being skipped", _ctx do
    ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :consumer, :reconnect]])

    {:ok, _} = Consumer.subscribe(Handlers.Echo, consumer: TestConsumer)

    # The frame before the hole is handled normally.
    assert_receive {:handled, %OrderPlaced{order_id: 1}, _}, 5_000

    # The frame after it names a predecessor this subscription never saw.
    assert_receive {[:dmex, :consumer, :reconnect], ^ref, _measurements,
                    %{reason: {:sequence_gap, 1, 2}}},
                   5_000
  end

  test "the message after the hole is not handled, so the cursor cannot step over it", _ctx do
    {:ok, _} = Consumer.subscribe(Handlers.Echo, consumer: TestConsumer)

    assert_receive {:handled, %OrderPlaced{order_id: 1}, _}, 5_000

    # Handling 3 would commit an advance to 3, putting the missing 2 permanently
    # behind the cursor. It is refused for exactly that reason.
    refute_receive {:handled, %OrderPlaced{order_id: 3}, _}, 1_000

    assert cursor() == {:ok, 1}
  end

  # Recovery is a re-read from the durable cursor rather than a crash: if the
  # gap was transport damage it repairs itself, and if the producer really does
  # skip rows the same break recurs and the subscription is a visible loop
  # making no progress — which is the correct failure, loudly.
  test "it keeps retrying rather than dying, and never advances", _ctx do
    ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :consumer, :reconnect]])

    {:ok, pid} = Consumer.subscribe(Handlers.Echo, consumer: TestConsumer)

    for _ <- 1..2 do
      assert_receive {[:dmex, :consumer, :reconnect], ^ref, _, %{reason: {:sequence_gap, _, _}}},
                     10_000
    end

    assert Process.alive?(pid)
    assert cursor() == {:ok, 1}
  end

  defp cursor do
    Postgres.last_seen(%{repo: DMEx.Test.IntegrationRepo, prefix: nil}, "test/echo")
  end
end
