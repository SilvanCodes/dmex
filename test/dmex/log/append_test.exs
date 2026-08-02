defmodule DMEx.Log.AppendTest do
  use DMEx.Case, async: true, database: true

  alias DMEx.Storage.Postgres
  alias DMEx.Test.Log
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Messages.OrderShipped

  setup do
    %{opts: Log.__dmex__().storage_opts}
  end

  describe "append/2" do
    test "returns the assigned id", %{opts: opts} do
      assert {:ok, id} = Log.append(%OrderPlaced{order_id: 7})
      assert is_integer(id)

      assert {:ok, ^id} = Postgres.max_id(opts)
    end

    test "reads the wire type off the message's own module", %{opts: opts} do
      {:ok, id} = Log.append(%OrderPlaced{order_id: 7})

      assert %{message_type: "DMEx.Test.Messages.OrderPlaced"} = fetch!(opts, id)
    end

    test "serializes the struct, not a wrapper around it", %{opts: opts} do
      {:ok, id} = Log.append(%OrderPlaced{order_id: 7, total: 1999})

      assert %{data: data} = fetch!(opts, id)

      assert JSON.decode!(data) == %{"order_id" => 7, "customer_id" => nil, "total" => 1999}
    end

    test "passes metadata through untouched", %{opts: opts} do
      {:ok, id} = Log.append(%OrderPlaced{order_id: 7}, metadata: %{trace_id: "abc"})

      assert %{metadata: metadata} = fetch!(opts, id)
      assert JSON.decode!(metadata) == %{"trace_id" => "abc"}
    end

    test "merges nothing of its own into metadata", %{opts: opts} do
      # No log, no origin, no timestamp — so a publisher has no reserved
      # key names to avoid. The type string already identifies the appending
      # application, which is why nothing needs recording here.
      {:ok, id} = Log.append(%OrderPlaced{order_id: 7}, metadata: %{a: 1})

      assert %{metadata: metadata} = fetch!(opts, id)
      assert JSON.decode!(metadata) == %{"a" => 1}
    end

    test "defaults metadata to an empty map", %{opts: opts} do
      {:ok, id} = Log.append(%OrderPlaced{order_id: 7})

      assert %{metadata: "{}"} = fetch!(opts, id)
    end

    test "distinguishes message types on the same log", %{opts: opts} do
      {:ok, _} = Log.append(%OrderPlaced{order_id: 7})
      {:ok, _} = Log.append(%OrderShipped{order_id: 7, carrier: "dhl"})

      assert {:ok, rows} = Postgres.read(opts, 0, 10, 1_048_576, :all)

      assert Enum.map(rows, & &1.message_type) == [
               "DMEx.Test.Messages.OrderPlaced",
               "DMEx.Test.Messages.OrderShipped"
             ]
    end

    test "assigns ids in call order", %{opts: opts} do
      ids = for n <- 1..5, do: elem(Log.append(%OrderPlaced{order_id: n}), 1)

      assert ids == Enum.sort(ids)

      {:ok, rows} = Postgres.read(opts, 0, 10, 1_048_576, :all)
      assert Enum.map(rows, &JSON.decode!(&1.data)["order_id"]) == [1, 2, 3, 4, 5]
    end
  end

  describe "append/2 with a list" do
    test "returns one id per message, in the order given", %{opts: opts} do
      assert {:ok, ids} =
               Log.append([
                 %OrderPlaced{order_id: 1},
                 %OrderShipped{order_id: 1, carrier: "dhl"},
                 %OrderPlaced{order_id: 2}
               ])

      assert length(ids) == 3
      assert ids == Enum.sort(ids)

      {:ok, rows} = Postgres.read(opts, 0, 10, 1_048_576, :all)

      assert Enum.map(rows, & &1.id) == ids

      assert Enum.map(rows, & &1.message_type) == [
               "DMEx.Test.Messages.OrderPlaced",
               "DMEx.Test.Messages.OrderShipped",
               "DMEx.Test.Messages.OrderPlaced"
             ]
    end

    test "the batch is contiguous, since it holds the lock for its whole run", %{opts: opts} do
      {:ok, ids} = Log.append(for n <- 1..10, do: %OrderPlaced{order_id: n})

      assert ids == Enum.to_list(hd(ids)..List.last(ids))

      {:ok, rows} = Postgres.read(opts, 0, 20, 1_048_576, :all)
      assert Enum.map(rows, &JSON.decode!(&1.data)["order_id"]) == Enum.to_list(1..10)
    end

    test "applies the same metadata to every message", %{opts: opts} do
      {:ok, [first, second]} =
        Log.append([%OrderPlaced{order_id: 1}, %OrderPlaced{order_id: 2}],
          metadata: %{trace_id: "abc"}
        )

      for id <- [first, second] do
        assert %{metadata: metadata} = fetch!(opts, id)
        assert JSON.decode!(metadata) == %{"trace_id" => "abc"}
      end
    end

    test "a single-message list still comes back as a list", %{opts: _opts} do
      assert {:ok, [id]} = Log.append([%OrderPlaced{order_id: 7}])
      assert is_integer(id)
    end

    test "an empty list writes nothing", %{opts: opts} do
      assert {:ok, []} = Log.append([])

      assert {:ok, 0} = Postgres.max_id(opts)
    end

    test "a non-message anywhere in the list rejects the whole batch", %{opts: opts} do
      assert_raise ArgumentError, ~r/is not a DMEx message module/, fn ->
        Log.append([%OrderPlaced{order_id: 1}, %URI{}])
      end

      assert_raise ArgumentError, ~r/only DMEx message structs can be appended/, fn ->
        Log.append([%OrderPlaced{order_id: 1}, :not_a_struct])
      end

      # Encoding happens before the transaction opens, so a rejected batch
      # cannot have written its earlier messages.
      assert {:ok, 0} = Postgres.max_id(opts)
    end
  end

  describe "a log held in a variable" do
    test "writes the same row a literal call does", %{opts: opts} do
      log = Log

      assert {:ok, _} = Log.append(%OrderPlaced{order_id: 7}, metadata: %{a: 1})
      assert {:ok, _} = log.append(%OrderPlaced{order_id: 7}, metadata: %{a: 1})

      assert {:ok, [first, second]} = Postgres.read(opts, 0, 10, 1_048_576, :all)

      assert Map.take(first, [:message_type, :data, :metadata]) ==
               Map.take(second, [:message_type, :data, :metadata])
    end

    test "reaches append/1 through apply as well" do
      # The literal MFA form, since that is the shape generic code holding
      # {log, message} reaches for. Deliberately `apply/3` rather than a
      # direct call — that is the API surface under test.
      # credo:disable-for-next-line Credo.Check.Refactor.Apply
      assert {:ok, _} = apply(Log, :append, [%OrderPlaced{order_id: 7}])
    end
  end

  describe "errors" do
    test "a struct that is not a DMEx message names the fix" do
      assert_raise ArgumentError, ~r/is not a DMEx message module.*use DMEx.Message/s, fn ->
        Log.append(%URI{})
      end
    end

    test "an unknown option is rejected rather than ignored" do
      assert_raise ArgumentError, ~r/unknown append option\(s\): \[:priority\]/, fn ->
        Log.append(%OrderPlaced{order_id: 7}, priority: :high)
      end
    end
  end

  describe "telemetry" do
    setup do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:dmex, :append, :start],
          [:dmex, :append, :stop]
        ])

      %{ref: ref}
    end

    test "spans the append with the log and message type", %{ref: ref} do
      {:ok, _} = Log.append(%OrderPlaced{order_id: 7})

      assert_received {[:dmex, :append, :start], ^ref, _measurements, metadata}
      assert metadata.log == Log
      assert metadata.message_types == ["DMEx.Test.Messages.OrderPlaced"]
      assert metadata.count == 1

      assert_received {[:dmex, :append, :stop], ^ref, %{duration: duration}, _metadata}
      assert is_integer(duration)
    end

    test "a batch is one span, naming its distinct types", %{ref: ref} do
      {:ok, _} =
        Log.append([
          %OrderPlaced{order_id: 1},
          %OrderPlaced{order_id: 2},
          %OrderShipped{order_id: 1, carrier: "dhl"}
        ])

      assert_received {[:dmex, :append, :start], ^ref, _measurements, metadata}

      assert metadata.message_types == [
               "DMEx.Test.Messages.OrderPlaced",
               "DMEx.Test.Messages.OrderShipped"
             ]

      assert metadata.count == 3

      assert_received {[:dmex, :append, :stop], ^ref, _measurements, _metadata}
      refute_received {[:dmex, :append, :start], ^ref, _, _}
    end

    test "an empty batch is not an event", %{ref: ref} do
      assert {:ok, []} = Log.append([])

      refute_received {[:dmex, :append, :start], ^ref, _, _}
    end
  end

  defp fetch!(opts, id) do
    {:ok, [row]} = Postgres.read(opts, id - 1, 1, 1_048_576, :all)
    row
  end
end
