defmodule DMEx.Integration.VerticalSliceTest do
  @moduledoc """
  The whole of DMEx, end to end, over a real socket.

  Publish writes a row; `NOTIFY` wakes the request process serving the stream;
  the send loop reads and frames it; the bytes cross a real TCP connection; the
  consumer decodes, casts, and hands it to a handler; the cursor commits. If
  this passes, the vertical slice works.
  """

  use DMEx.IntegrationCase

  alias DMEx.Consumer
  alias DMEx.Storage.Postgres
  alias DMEx.Test.Handlers
  alias DMEx.Test.IntegrationProducer, as: Producer
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Server

  @moduletag :capture_log

  defmodule SecondHandler do
    @moduledoc false
    use DMEx.Consumer.Handler

    alias DMEx.Test.Handlers

    @impl true
    def handle_message(message, _metadata) do
      send(Handlers.owner(), {:second, message})

      :ok
    end
  end

  setup do
    Handlers.report_to(self())

    start_supervised!(Producer)
    start_supervised!({Consumer, name: TestConsumer, repo: DMEx.Test.IntegrationRepo})

    url = Server.start!() <> "/stream"

    %{url: url, storage_opts: Producer.__dmex__().storage_opts}
  end

  test "delivers a published message to a handler and commits the cursor", ctx do
    subscribe(ctx, Handlers.Echo)

    {:ok, id} = Producer.publish(%OrderPlaced{order_id: 7, total: 1999})

    assert_receive {:handled, message, metadata}, 5_000

    assert message == %OrderPlaced{order_id: 7, customer_id: nil, total: 1999}
    assert metadata == %{}

    # The cursor is durable, not in-memory: this is a real committed row.
    eventually(fn -> Postgres.last_seen(consumer_opts(), Handlers.Echo) == {:ok, id} end)
  end

  test "delivers a backlog published before the consumer ever connected", ctx do
    # The producer holds no consumer state, so history is not something it kept
    # for anyone — it is simply the log, read from id 0.
    for n <- 1..5, do: {:ok, _} = Producer.publish(%OrderPlaced{order_id: n})

    subscribe(ctx, Handlers.Echo)

    assert received_order_ids(5) == [1, 2, 3, 4, 5]
  end

  test "delivers new messages live on an already-open connection", ctx do
    subscribe(ctx, Handlers.Echo)

    {:ok, _} = Producer.publish(%OrderPlaced{order_id: 1})
    assert_receive {:handled, %OrderPlaced{order_id: 1}, _}, 5_000

    # No reconnect: this is the NOTIFY path waking the request process that is
    # already serving this consumer.
    for n <- 2..4, do: {:ok, _} = Producer.publish(%OrderPlaced{order_id: n})

    assert received_order_ids(3) == [2, 3, 4]
  end

  test "preserves publisher metadata verbatim", ctx do
    subscribe(ctx, Handlers.Echo)

    {:ok, _} =
      Producer.publish(%OrderPlaced{order_id: 1}, metadata: %{trace_id: "abc", retry: 2})

    assert_receive {:handled, _, metadata}, 5_000
    assert metadata == %{"trace_id" => "abc", "retry" => 2}
  end

  test "delivers in log order", ctx do
    subscribe(ctx, Handlers.Echo)

    for n <- 1..20, do: {:ok, _} = Producer.publish(%OrderPlaced{order_id: n})

    assert received_order_ids(20) == Enum.to_list(1..20)
  end

  test "two handlers on one producer each get everything, with their own cursors", ctx do
    subscribe(ctx, Handlers.Echo)
    subscribe(ctx, SecondHandler)

    {:ok, id} = Producer.publish(%OrderPlaced{order_id: 1})

    assert_receive {:handled, %OrderPlaced{order_id: 1}, _}, 5_000
    assert_receive {:second, %OrderPlaced{order_id: 1}}, 5_000

    eventually(fn ->
      Postgres.last_seen(consumer_opts(), Handlers.Echo) == {:ok, id} and
        Postgres.last_seen(consumer_opts(), SecondHandler) == {:ok, id}
    end)
  end

  ## Helpers

  defp subscribe(ctx, handler) do
    {:ok, _} =
      Consumer.subscribe(ctx.url,
        consumer: TestConsumer,
        handler: handler,
        message_modules: [OrderPlaced],
        batch_window: 50
      )

    :ok
  end

  defp consumer_opts, do: %{repo: DMEx.Test.IntegrationRepo, prefix: nil}

  defp received_order_ids(count) do
    for _ <- 1..count do
      assert_receive {:handled, %OrderPlaced{order_id: id}, _}, 5_000
      id
    end
  end
end
