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
  alias DMEx.Test.Messages.OrderShipped
  alias DMEx.Test.Server
  alias Ecto.Adapters.SQL

  @moduletag :capture_log

  defmodule SecondHandler do
    @moduledoc false
    use DMEx.Consumer.Handler, producer: :test, id: :second

    alias DMEx.Test.Handlers
    alias DMEx.Test.Messages.OrderPlaced

    @impl true
    def handle_message(%OrderPlaced{} = message, _metadata) do
      send(Handlers.owner(), {:second, message})

      :ok
    end
  end

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

    %{url: url, storage_opts: Producer.__dmex__().storage_opts}
  end

  test "delivers a published message to a handler and commits the cursor", _ctx do
    subscribe(Handlers.Echo)

    {:ok, id} = Producer.publish(%OrderPlaced{order_id: 7, total: 1999})

    assert_receive {:handled, message, metadata}, 5_000

    assert message == %OrderPlaced{order_id: 7, customer_id: nil, total: 1999}
    assert metadata == %{}

    # The cursor is durable, not in-memory: this is a real committed row.
    eventually(fn -> Postgres.last_seen(consumer_opts(), "test/echo") == {:ok, id} end)
  end

  test "delivers a backlog published before the consumer ever connected", _ctx do
    # The producer holds no consumer state, so history is not something it kept
    # for anyone — it is simply the log, read from id 0.
    for n <- 1..5, do: {:ok, _} = Producer.publish(%OrderPlaced{order_id: n})

    subscribe(Handlers.Echo)

    assert received_order_ids(5) == [1, 2, 3, 4, 5]
  end

  test "delivers new messages live on an already-open connection", _ctx do
    subscribe(Handlers.Echo)

    {:ok, _} = Producer.publish(%OrderPlaced{order_id: 1})
    assert_receive {:handled, %OrderPlaced{order_id: 1}, _}, 5_000

    # No reconnect: this is the NOTIFY path waking the request process that is
    # already serving this consumer.
    for n <- 2..4, do: {:ok, _} = Producer.publish(%OrderPlaced{order_id: n})

    assert received_order_ids(3) == [2, 3, 4]
  end

  test "preserves publisher metadata verbatim", _ctx do
    subscribe(Handlers.Echo)

    {:ok, _} =
      Producer.publish(%OrderPlaced{order_id: 1}, metadata: %{trace_id: "abc", retry: 2})

    assert_receive {:handled, _, metadata}, 5_000
    assert metadata == %{"trace_id" => "abc", "retry" => 2}
  end

  test "delivers in log order", _ctx do
    subscribe(Handlers.Echo)

    for n <- 1..20, do: {:ok, _} = Producer.publish(%OrderPlaced{order_id: n})

    assert received_order_ids(20) == Enum.to_list(1..20)
  end

  test "two handlers on one producer each get everything, with their own cursors", _ctx do
    subscribe(Handlers.Echo)
    subscribe(SecondHandler)

    {:ok, id} = Producer.publish(%OrderPlaced{order_id: 1})

    assert_receive {:handled, %OrderPlaced{order_id: 1}, _}, 5_000
    assert_receive {:second, %OrderPlaced{order_id: 1}}, 5_000

    eventually(fn ->
      Postgres.last_seen(consumer_opts(), "test/echo") == {:ok, id} and
        Postgres.last_seen(consumer_opts(), "test/second") == {:ok, id}
    end)
  end

  # What the whole `{producer, id}` refactor is for, asserted against a real
  # socket and a real cursor row rather than argued about.
  describe "the subscription's durable identity" do
    # All three declare `producer: :test, id: :slice`, so all three *are* the
    # same subscription. They differ only in the ways the design promises are
    # free: the module's name, and the message list.
    defmodule Slice do
      @moduledoc false
      use DMEx.Consumer.Handler, producer: :test, id: :slice

      alias DMEx.Test.Handlers
      alias DMEx.Test.Messages.OrderPlaced

      @impl true
      def handle_message(%OrderPlaced{} = message, _metadata) do
        send(Handlers.owner(), {:sliced, message})

        :ok
      end
    end

    defmodule SliceRenamed do
      @moduledoc "Slice after an ordinary module rename — same declaration."
      use DMEx.Consumer.Handler, producer: :test, id: :slice

      alias DMEx.Test.Handlers
      alias DMEx.Test.Messages.OrderPlaced

      @impl true
      def handle_message(%OrderPlaced{} = message, _metadata) do
        send(Handlers.owner(), {:sliced, message})

        :ok
      end
    end

    defmodule SliceWidened do
      @moduledoc "Slice with a clause added for a second type — same identity."
      use DMEx.Consumer.Handler, producer: :test, id: :slice

      alias DMEx.Test.Handlers
      alias DMEx.Test.Messages.OrderPlaced
      alias DMEx.Test.Messages.OrderShipped

      @impl true
      def handle_message(%OrderPlaced{} = message, _metadata), do: report(message)
      def handle_message(%OrderShipped{} = message, _metadata), do: report(message)

      defp report(message) do
        send(Handlers.owner(), {:sliced, message})

        :ok
      end
    end

    test "the cursor row is keyed by the declared pair, not the module name", _ctx do
      subscribe(Slice)

      {:ok, id} = Producer.publish(%OrderPlaced{order_id: 1})
      assert_receive {:sliced, %OrderPlaced{order_id: 1}}, 5_000

      eventually(fn -> cursor_rows() == [["test/slice", id]] end)
    end

    # Benefit (b), directly: a rename is what a refactoring tool does without
    # asking, and it must not replay a producer's log.
    test "a renamed handler module continues the cursor rather than replaying", _ctx do
      subscribe(Slice)
      {:ok, first} = Producer.publish(%OrderPlaced{order_id: 1})
      assert_receive {:sliced, %OrderPlaced{order_id: 1}}, 5_000

      resubscribe(Slice, SliceRenamed, after: first)

      {:ok, id} = Producer.publish(%OrderPlaced{order_id: 2})

      assert_receive {:sliced, %OrderPlaced{order_id: 2}}, 5_000
      refute_received {:sliced, %OrderPlaced{order_id: 1}}
      eventually(fn -> cursor_rows() == [["test/slice", id]] end)
    end

    # Benefit (a): the URL is provenance, never identity, so repointing a
    # producer name at a different host needs no migration.
    test "repointing the producer's URL continues the cursor", _ctx do
      subscribe(Slice)
      {:ok, first} = Producer.publish(%OrderPlaced{order_id: 1})
      assert_receive {:sliced, %OrderPlaced{order_id: 1}}, 5_000

      # A second server in front of the very same log.
      second = Server.start!() <> "/stream"

      stop(Slice, after: first)

      {:ok, _} =
        Consumer.subscribe(Slice,
          consumer: TestConsumer,
          producer: Server.producers(second),
          batch_window: 50
        )

      {:ok, id} = Producer.publish(%OrderPlaced{order_id: 2})

      assert_receive {:sliced, %OrderPlaced{order_id: 2}}, 5_000
      refute_received {:sliced, %OrderPlaced{order_id: 1}}
      eventually(fn -> cursor_rows() == [["test/slice", id]] end)
    end

    # The trade the design deliberately makes: the message list is not part of
    # the identity, so a type gained by adding a clause is handled from the tip
    # onward and its history is *not* replayed. Getting it takes Cursors.move!/3.
    test "adding a clause handles that type from the tip, without replaying", _ctx do
      subscribe(Slice)

      {:ok, _} = Producer.publish(%OrderShipped{order_id: 1, carrier: "dhl"})
      {:ok, second} = Producer.publish(%OrderPlaced{order_id: 2})
      assert_receive {:sliced, %OrderPlaced{order_id: 2}}, 5_000

      resubscribe(Slice, SliceWidened, after: second)

      {:ok, id} = Producer.publish(%OrderShipped{order_id: 3, carrier: "ups"})

      # The new one, from the tip.
      assert_receive {:sliced, %OrderShipped{order_id: 3}}, 5_000
      # Not the one published before the list was widened.
      refute_received {:sliced, %OrderShipped{order_id: 1}}
      eventually(fn -> cursor_rows() == [["test/slice", id]] end)
    end
  end

  ## Helpers

  # Stops one module's subscription and starts another declaring the same
  # identity — a module rename or a list widening, as a running system sees it.
  defp resubscribe(from, to, opts) do
    stop(from, opts)

    subscribe(to)
  end

  # Waiting for the cursor *before* stopping is not incidental. A handler sends
  # its report from inside the transaction that advances the cursor, so the
  # message arrives before the commit does — killing the stream in that window
  # rolls the advance back and the next start legitimately redelivers. That is
  # the at-least-once gap the design documents, not the identity behaviour under
  # test here.
  defp stop(handler, after: id) do
    await_cursor(id)

    :ok = Consumer.unsubscribe(handler, consumer: TestConsumer)

    eventually(fn ->
      Registry.lookup(Consumer.registry(TestConsumer), {:subscription, "test/slice"}) == []
    end)
  end

  defp await_cursor(id) do
    eventually(fn -> Postgres.last_seen(consumer_opts(), "test/slice") == {:ok, id} end)
  end

  defp cursor_rows do
    SQL.query!(
      DMEx.Test.IntegrationRepo,
      "SELECT subscription, last_seen FROM dmex_cursors ORDER BY subscription",
      []
    ).rows
  end

  defp subscribe(handler) do
    {:ok, _} = Consumer.subscribe(handler, consumer: TestConsumer, batch_window: 50)

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
