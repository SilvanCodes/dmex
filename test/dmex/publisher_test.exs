defmodule DMEx.PublisherTest do
  use DMEx.Case, async: true, database: true

  alias DMEx.Publisher
  alias DMEx.Storage.Postgres
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Messages.OrderShipped
  alias DMEx.Test.Producer

  setup do
    %{opts: Producer.__dmex__().storage_opts}
  end

  describe "publish/3" do
    test "returns the assigned id", %{opts: opts} do
      assert {:ok, id} = Publisher.publish(Producer, %OrderPlaced{order_id: 7})
      assert is_integer(id)

      assert {:ok, ^id} = Postgres.max_id(opts)
    end

    test "reads the wire type off the message's own module", %{opts: opts} do
      {:ok, id} = Publisher.publish(Producer, %OrderPlaced{order_id: 7})

      assert %{message_type: "DMEx.Test.Messages.OrderPlaced"} = fetch!(opts, id)
    end

    test "serializes the struct, not a wrapper around it", %{opts: opts} do
      {:ok, id} = Publisher.publish(Producer, %OrderPlaced{order_id: 7, total: 1999})

      assert %{data: data} = fetch!(opts, id)

      assert JSON.decode!(data) == %{"order_id" => 7, "customer_id" => nil, "total" => 1999}
    end

    test "passes metadata through untouched", %{opts: opts} do
      {:ok, id} =
        Publisher.publish(Producer, %OrderPlaced{order_id: 7}, metadata: %{trace_id: "abc"})

      assert %{metadata: metadata} = fetch!(opts, id)
      assert JSON.decode!(metadata) == %{"trace_id" => "abc"}
    end

    test "merges nothing of its own into metadata", %{opts: opts} do
      # No producer, no origin, no timestamp — so a publisher has no reserved
      # key names to avoid. The type string already identifies the publishing
      # application, which is why nothing needs recording here.
      {:ok, id} = Publisher.publish(Producer, %OrderPlaced{order_id: 7}, metadata: %{a: 1})

      assert %{metadata: metadata} = fetch!(opts, id)
      assert JSON.decode!(metadata) == %{"a" => 1}
    end

    test "defaults metadata to an empty map", %{opts: opts} do
      {:ok, id} = Publisher.publish(Producer, %OrderPlaced{order_id: 7})

      assert %{metadata: "{}"} = fetch!(opts, id)
    end

    test "distinguishes message types on the same log", %{opts: opts} do
      {:ok, _} = Publisher.publish(Producer, %OrderPlaced{order_id: 7})
      {:ok, _} = Publisher.publish(Producer, %OrderShipped{order_id: 7, carrier: "dhl"})

      assert {:ok, {rows, _}} = Postgres.read(opts, 0, 10)

      assert Enum.map(rows, & &1.message_type) == [
               "DMEx.Test.Messages.OrderPlaced",
               "DMEx.Test.Messages.OrderShipped"
             ]
    end

    test "assigns ids in call order", %{opts: opts} do
      ids = for n <- 1..5, do: elem(Publisher.publish(Producer, %OrderPlaced{order_id: n}), 1)

      assert ids == Enum.sort(ids)

      {:ok, {rows, _}} = Postgres.read(opts, 0, 10)
      assert Enum.map(rows, &JSON.decode!(&1.data)["order_id"]) == [1, 2, 3, 4, 5]
    end
  end

  describe "the producer module's shorthand" do
    test "publish/1 and publish/2 write the same row publish/3 does", %{opts: opts} do
      assert {:ok, _} = Producer.publish(%OrderPlaced{order_id: 7}, metadata: %{a: 1})
      assert {:ok, _} = Publisher.publish(Producer, %OrderPlaced{order_id: 7}, metadata: %{a: 1})

      assert {:ok, {[first, second], _}} = Postgres.read(opts, 0, 10)

      assert Map.take(first, [:message_type, :data, :metadata]) ==
               Map.take(second, [:message_type, :data, :metadata])
    end

    test "publish/1 defaults its metadata" do
      assert {:ok, _} = Producer.publish(%OrderPlaced{order_id: 7})
    end
  end

  describe "errors" do
    test "a struct that is not a DMEx message names the fix" do
      assert_raise ArgumentError, ~r/is not a DMEx message module.*use DMEx.Message/s, fn ->
        Publisher.publish(Producer, %URI{})
      end
    end

    test "an unknown option is rejected rather than ignored" do
      assert_raise ArgumentError, ~r/unknown publish option\(s\): \[:topic\]/, fn ->
        Publisher.publish(Producer, %OrderPlaced{order_id: 7}, topic: "orders")
      end
    end
  end

  describe "telemetry" do
    test "spans the append with the producer and message type" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:dmex, :publish, :start],
          [:dmex, :publish, :stop]
        ])

      {:ok, _} = Publisher.publish(Producer, %OrderPlaced{order_id: 7})

      assert_received {[:dmex, :publish, :start], ^ref, _measurements, metadata}
      assert metadata.producer == Producer
      assert metadata.message_type == "DMEx.Test.Messages.OrderPlaced"

      assert_received {[:dmex, :publish, :stop], ^ref, %{duration: duration}, _metadata}
      assert is_integer(duration)
    end
  end

  defp fetch!(opts, id) do
    {:ok, {[row], _}} = Postgres.read(opts, id - 1, 1)
    row
  end
end
