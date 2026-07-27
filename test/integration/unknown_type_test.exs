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
    start_supervised!({Consumer, name: TestConsumer, repo: DMEx.Test.IntegrationRepo})

    %{url: Server.start!() <> "/stream"}
  end

  test "a type the consumer cannot decode is passed over, and the rest arrive", ctx do
    subscribe(ctx, [OrderPlaced])

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

  test "a stream a consumer understands none of still advances", ctx do
    subscribe(ctx, [OrderPlaced])

    for n <- 1..5, do: {:ok, _} = Producer.publish(%OrderShipped{order_id: n, carrier: "dhl"})
    {:ok, id} = Postgres.max_id(Producer.__dmex__().storage_opts)

    eventually(fn -> cursor() == {:ok, id} end)
    refute_received {:handled, _, _}
  end

  test "skips are reported, since silence looks identical to a broken filter", ctx do
    ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :consumer, :skip]])

    subscribe(ctx, [OrderPlaced])

    {:ok, _} = Producer.publish(%OrderShipped{order_id: 1, carrier: "dhl"})

    assert_receive {[:dmex, :consumer, :skip], ^ref, _measurements, metadata}, 5_000
    assert metadata.message_type == "DMEx.Test.Messages.OrderShipped"
    assert metadata.handler == Handlers.Echo
  end

  test "declaring both modules delivers both", ctx do
    # The same list does double duty: it is what makes decoding possible and
    # what narrows the stream. Widening it is one edit.
    subscribe(ctx, [OrderPlaced, OrderShipped])

    {:ok, _} = Producer.publish(%OrderPlaced{order_id: 1})
    {:ok, _} = Producer.publish(%OrderShipped{order_id: 2, carrier: "dhl"})

    assert_receive {:handled, %OrderPlaced{order_id: 1}, _}, 5_000
    assert_receive {:handled, %OrderShipped{order_id: 2, carrier: "dhl"}, _}, 5_000
  end

  defp subscribe(ctx, modules) do
    {:ok, _} =
      Consumer.subscribe(ctx.url,
        consumer: TestConsumer,
        handler: Handlers.Echo,
        message_modules: modules,
        batch_window: 50
      )
  end

  defp cursor do
    Postgres.last_seen(%{repo: DMEx.Test.IntegrationRepo, prefix: nil}, Handlers.Echo)
  end
end
