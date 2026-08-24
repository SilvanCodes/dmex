defmodule DMEx.QueryTest do
  use DMEx.Case, async: true, database: true

  alias DMEx.Query
  alias DMEx.Storage.Postgres
  alias DMEx.Test.Log
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Messages.OrderShipped
  alias DMEx.Test.Messages.Renamed
  alias DMEx.Test.Messages.WithCustomCast

  # An adapter with no `c:DMEx.Storage.select/2` — legal for the behaviour, and
  # the one optional callback whose absence stops the caller.
  defmodule NoSelect do
    @moduledoc false
    def validate!(_opts), do: :ok
  end

  setup do
    %{opts: [repo: DMEx.Test.Repo], storage: Log.__dmex_log__().storage}
  end

  describe "all/2" do
    test "casts rows into the module's struct", ctx do
      {:ok, _} = Log.append(%OrderPlaced{order_id: 7, total: 1999})

      assert {:ok, [%OrderPlaced{order_id: 7, total: 1999, customer_id: nil}]} =
               Query.all(OrderPlaced, ctx.opts)
    end

    test "returns only the module's own type", ctx do
      {:ok, _} = Log.append(%OrderPlaced{order_id: 1})
      {:ok, _} = Log.append(%OrderShipped{order_id: 2, carrier: "dhl"})

      assert {:ok, [%OrderPlaced{order_id: 1}]} = Query.all(OrderPlaced, ctx.opts)
      assert {:ok, [%OrderShipped{order_id: 2}]} = Query.all(OrderShipped, ctx.opts)
    end

    # The reason `:types` is a list: a module answers for every string it ever
    # appended under, so a rename does not hide the rows written before it.
    test "includes the types the module retired", ctx do
      {:ok, _} = append_raw(ctx.storage, "order_placed_legacy", %{order_id: 1})
      {:ok, _} = Log.append(%Renamed{order_id: 2})

      assert {:ok, [%Renamed{order_id: 1}, %Renamed{order_id: 2}]} = Query.all(Renamed, ctx.opts)
    end

    # Not a second decode path: whatever a handler would receive is what this
    # returns, including an overridden `cast/1`.
    test "casts through the module's own cast/1", ctx do
      {:ok, _} = append_raw(ctx.storage, WithCustomCast.type(), %{"order_id" => 1, "placed_at" => "2026-08-24"})

      assert {:ok, [%WithCustomCast{order_id: 1, placed_at: placed_at}]} =
               Query.all(WithCustomCast, ctx.opts)

      refute is_binary(placed_at)
    end

    test "returns oldest first", ctx do
      for n <- 1..5, do: {:ok, _} = Log.append(%OrderPlaced{order_id: n})

      assert {:ok, rows} = Query.all(OrderPlaced, ctx.opts)
      assert Enum.map(rows, & &1.order_id) == [1, 2, 3, 4, 5]
    end

    test "an empty log reads empty", ctx do
      assert {:ok, []} = Query.all(OrderPlaced, ctx.opts)
    end
  end

  describe "filters" do
    test "limit caps the rows", ctx do
      for n <- 1..3, do: {:ok, _} = Log.append(%OrderPlaced{order_id: n})

      assert {:ok, rows} = Query.all(OrderPlaced, ctx.opts ++ [limit: 2])
      assert Enum.map(rows, & &1.order_id) == [1, 2]
    end

    # The documented default, pinned against data rather than restated: an
    # unbounded read of an append-only log is the failure this exists to prevent.
    test "an unnamed limit is default_limit/0", ctx do
      for n <- 1..(Query.default_limit() + 1), do: {:ok, _} = Log.append(%OrderPlaced{order_id: n})

      assert {:ok, rows} = Query.all(OrderPlaced, ctx.opts)
      assert length(rows) == Query.default_limit()
    end

    test "after_id pages exclusively", ctx do
      {:ok, first} = Log.append(%OrderPlaced{order_id: 1})
      {:ok, _} = Log.append(%OrderPlaced{order_id: 2})

      assert {:ok, [%OrderPlaced{order_id: 2}]} = Query.all(OrderPlaced, ctx.opts ++ [after_id: first])
    end

    test "order: :desc reads newest first", ctx do
      for n <- 1..3, do: {:ok, _} = Log.append(%OrderPlaced{order_id: n})

      assert {:ok, rows} = Query.all(OrderPlaced, ctx.opts ++ [order: :desc])
      assert Enum.map(rows, & &1.order_id) == [3, 2, 1]
    end

    # What an operator actually asks for: the latest handful.
    test "order and limit compose into the newest N", ctx do
      for n <- 1..5, do: {:ok, _} = Log.append(%OrderPlaced{order_id: n})

      assert {:ok, rows} = Query.all(OrderPlaced, ctx.opts ++ [order: :desc, limit: 2])
      assert Enum.map(rows, & &1.order_id) == [5, 4]
    end
  end

  describe "errors" do
    test "a module that is not a DMEx message names the fix", ctx do
      assert_raise ArgumentError, ~r/is not a DMEx message module.*use DMEx.Message/s, fn ->
        Query.all(URI, ctx.opts)
      end
    end

    test "an unknown option is rejected rather than ignored", ctx do
      assert_raise ArgumentError, ~r/unknown option \[:type\]/, fn ->
        Query.all(OrderPlaced, ctx.opts ++ [type: "order_placed"])
      end
    end

    test "a repo is required", _ctx do
      assert_raise ArgumentError, ~r/a `repo:` is required/, fn ->
        Query.all(OrderPlaced, [])
      end
    end

    # `:order` reaches the statement as SQL rather than as a parameter, so an
    # unrecognised value must not get that far.
    test "a bound of the wrong shape names the option", ctx do
      assert_raise ArgumentError, ~r/`order:` must be either :asc or :desc/, fn ->
        Query.all(OrderPlaced, ctx.opts ++ [order: "DESC; DROP TABLE dmex_messages"])
      end

      assert_raise ArgumentError, ~r/`after_id:` must be a non-negative integer/, fn ->
        Query.all(OrderPlaced, ctx.opts ++ [after_id: -1])
      end
    end

    test "an adapter with no select/2 says so and names itself", _ctx do
      assert_raise ArgumentError, ~r/DMEx.QueryTest.NoSelect implements no .*select\/2/, fn ->
        Query.all(OrderPlaced, repo: NoSelect, storage: NoSelect)
      end
    end
  end

  ## Helpers

  # A row under a type no message module currently declares — the only way to
  # write one, since `c:DMEx.Message.type/0` is always the current type.
  defp append_raw(storage, type, data) do
    Postgres.append(type, JSON.encode!(data), "{}", storage)
  end
end
