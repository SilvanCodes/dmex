defmodule DMEx.Integration.UnknownTypeTest do
  @moduledoc """
  v1's filter is the consumer's own module list, applied after the bytes arrive.

  A subscription is the producer's entire log, so rows the consumer has no
  module for are the normal case rather than a fault. They advance the cursor
  and invoke nothing — which is the only thing that keeps a consumer from
  wedging on the first message type it does not know.
  """

  use DMEx.IntegrationCase

  alias DMEx.Consumer
  alias DMEx.Storage.Postgres
  alias DMEx.Test.Handlers
  alias DMEx.Test.IntegrationProducer, as: Producer
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Messages.OrderShipped
  alias DMEx.Test.Server

  @moduletag :capture_log

  setup do
    Handlers.report_to(self())

    start_supervised!(Producer)

    # The server binds port 0, so the producer map is necessarily built at
    # runtime — which is exactly why it lives on the child spec rather than in
    # application config.
    url = Server.start!() <> "/stream"

    start_supervised!(
      {Consumer,
       name: TestConsumer, repo: DMEx.Test.IntegrationRepo, producer: Server.producers(url)}
    )

    %{url: url}
  end

  test "a type the consumer cannot decode is passed over, and the rest arrive", _ctx do
    subscribe(Handlers.Echo)

    # Interleaved deliberately: an unknown type first, so a consumer that
    # wedged on it would never deliver anything at all.
    {:ok, _} = Producer.publish(%OrderShipped{order_id: 1, carrier: "dhl"})
    {:ok, _} = Producer.publish(%OrderPlaced{order_id: 2})
    {:ok, _} = Producer.publish(%OrderShipped{order_id: 3, carrier: "ups"})
    {:ok, id} = Producer.publish(%OrderPlaced{order_id: 4})

    assert_receive {:handled, %OrderPlaced{order_id: 2}, _}, 5_000
    assert_receive {:handled, %OrderPlaced{order_id: 4}, _}, 5_000
    refute_receive {:handled, %OrderShipped{}, _}, 200

    # The cursor passed over the skipped rows rather than stopping at them.
    eventually(fn -> cursor() == {:ok, id} end)
  end

  test "a stream a consumer understands none of still advances", _ctx do
    subscribe(Handlers.Echo)

    for n <- 1..5, do: {:ok, _} = Producer.publish(%OrderShipped{order_id: n, carrier: "dhl"})
    {:ok, id} = Postgres.max_id(Producer.__dmex__().storage_opts)

    eventually(fn -> cursor() == {:ok, id} end)
    refute_received {:handled, _, _}
  end

  test "skips are reported, since silence looks identical to a broken filter", _ctx do
    ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :consumer, :skip]])

    subscribe(Handlers.Echo)

    {:ok, _} = Producer.publish(%OrderShipped{order_id: 1, carrier: "dhl"})

    assert_receive {[:dmex, :consumer, :skip], ^ref, _measurements, metadata}, 5_000
    assert metadata.message_type == "DMEx.Test.Messages.OrderShipped"
    assert metadata.handler == Handlers.Echo
  end

  test "declaring both modules delivers both", _ctx do
    # The same list does double duty: it is what makes decoding possible and
    # what narrows the stream. Widening it is one edit — of the handler module,
    # which is where the list lives.
    subscribe(Handlers.EchoAll)

    {:ok, _} = Producer.publish(%OrderPlaced{order_id: 1})
    {:ok, _} = Producer.publish(%OrderShipped{order_id: 2, carrier: "dhl"})

    assert_receive {:handled, %OrderPlaced{order_id: 1}, _}, 5_000
    assert_receive {:handled, %OrderShipped{order_id: 2, carrier: "dhl"}, _}, 5_000
  end

  # The message list is part of the module now, so "the same handler with a
  # different list" is a different module.
  defp subscribe(handler) do
    {:ok, _} = Consumer.subscribe(handler, consumer: TestConsumer, batch_window: 50)
  end

  defp cursor do
    Postgres.last_seen(%{repo: DMEx.Test.IntegrationRepo, prefix: nil}, "test/echo")
  end
end
