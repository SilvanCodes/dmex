defmodule DMEx.Log.AppendTest do
  use DMEx.Case, async: true, database: true

  alias DMEx.Storage.Postgres
  alias DMEx.Test.Log
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Messages.OrderShipped

  # Top level rather than inside the test, so the struct literal below can
  # expand. Exporting `type/0` without `use DMEx.Message` is exactly the shape
  # the append path must not accept.
  defmodule LooksLikeAMessage do
    @moduledoc false
    defstruct [:order_id]
    def type, do: "order_placed"
  end

  setup do
    %{opts: Log.__dmex_log__().storage}
  end

  describe "append/2" do
    test "returns the assigned id", %{opts: opts} do
      assert {:ok, id} = Log.append(%OrderPlaced{order_id: 7})
      assert is_integer(id)

      assert {:ok, ^id} = Postgres.max_id(opts)
    end

    test "reads the wire type off the message's own module", %{opts: opts} do
      {:ok, id} = Log.append(%OrderPlaced{order_id: 7})

      assert %{type: "order_placed"} = fetch!(id, opts)
    end

    test "serializes the struct, not a wrapper around it", %{opts: opts} do
      {:ok, id} = Log.append(%OrderPlaced{order_id: 7, total: 1999})

      assert %{data: data} = fetch!(id, opts)

      assert JSON.decode!(data) == %{"order_id" => 7, "customer_id" => nil, "total" => 1999}
    end

    test "merges nothing of its own into metadata", %{opts: opts} do
      # No log, no origin, no timestamp — so a publisher has no reserved
      # key names to avoid. The type string already identifies the appending
      # application, which is why nothing needs recording here.
      {:ok, id} = Log.append(%OrderPlaced{order_id: 7}, %{a: 1})

      assert %{metadata: metadata} = fetch!(id, opts)
      assert JSON.decode!(metadata) == %{"a" => 1}
    end

    test "defaults metadata to an empty map", %{opts: opts} do
      {:ok, id} = Log.append(%OrderPlaced{order_id: 7})

      assert %{metadata: "{}"} = fetch!(id, opts)
    end

    test "distinguishes message types on the same log", %{opts: opts} do
      {:ok, _} = Log.append(%OrderPlaced{order_id: 7})
      {:ok, _} = Log.append(%OrderShipped{order_id: 7, carrier: "dhl"})

      assert {:ok, rows} = Postgres.read(0, 10, 1_048_576, :all, opts)

      assert Enum.map(rows, & &1.type) == [
               "order_placed",
               "order_shipped"
             ]
    end

    test "assigns ids in call order", %{opts: opts} do
      ids = for n <- 1..5, do: elem(Log.append(%OrderPlaced{order_id: n}), 1)

      assert ids == Enum.sort(ids)

      {:ok, rows} = Postgres.read(0, 10, 1_048_576, :all, opts)
      assert Enum.map(rows, &JSON.decode!(&1.data)["order_id"]) == [1, 2, 3, 4, 5]
    end
  end

  describe "a log held in a variable" do
    test "writes the same row a literal call does", %{opts: opts} do
      log = Log

      assert {:ok, _} = Log.append(%OrderPlaced{order_id: 7}, %{a: 1})
      assert {:ok, _} = log.append(%OrderPlaced{order_id: 7}, %{a: 1})

      assert {:ok, [first, second]} = Postgres.read(0, 10, 1_048_576, :all, opts)

      assert Map.take(first, [:type, :data, :metadata]) ==
               Map.take(second, [:type, :data, :metadata])
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

    # Exporting `type/0` is a shape a struct can have by coincidence, so the
    # check has to be `DMEx.Message.message?/1` and not `function_exported?/3`.
    # Under the weaker test this got past validation and died further in, inside
    # `JSON.encode!/1`, naming a protocol rather than the missing `use`.
    test "a look-alike exporting type/0 is still not a message" do
      assert_raise ArgumentError, ~r/is not a DMEx message module/, fn ->
        Log.append(%LooksLikeAMessage{order_id: 1})
      end
    end

    test "something that is not a struct at all raises the documented error" do
      assert_raise ArgumentError, ~r/is not a struct built with `use DMEx.Message`/, fn ->
        Log.append(%{order_id: 1})
      end
    end

    # The keyword form this replaced would encode to valid JSON and then read as
    # a malformed row on the wire, where an object is required.
    test "metadata that is not a map is refused at the door" do
      assert_raise ArgumentError, ~r/metadata must be a map, got: \[priority: :high\]/, fn ->
        Log.append(%OrderPlaced{order_id: 7}, priority: :high)
      end
    end
  end

  describe "telemetry" do
    test "spans the append with the log and message type" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:dmex, :append, :start],
          [:dmex, :append, :stop]
        ])

      {:ok, _} = Log.append(%OrderPlaced{order_id: 7})

      assert_received {[:dmex, :append, :start], ^ref, _measurements, metadata}
      assert metadata.log == Log
      assert metadata.type == "order_placed"

      assert_received {[:dmex, :append, :stop], ^ref, %{duration: duration}, _metadata}
      assert is_integer(duration)
    end
  end

  defp fetch!(id, opts) do
    {:ok, [row]} = Postgres.read(id - 1, 1, 1_048_576, :all, opts)
    row
  end
end
