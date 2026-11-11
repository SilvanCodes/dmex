defmodule DMEx.QueryTest do
  use DMEx.Case, async: true, database: true

  alias DMEx.Query
  alias DMEx.Storage.Postgres
  alias DMEx.Test.Log
  alias DMEx.Test.Messages.EntityPlaced
  alias DMEx.Test.Messages.EntityShipped
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

  describe "the first argument" do
    test "a list of modules unions their types", ctx do
      {:ok, _} = Log.append(%OrderPlaced{order_id: 1})
      {:ok, _} = Log.append(%OrderShipped{order_id: 2, carrier: "dhl"})
      {:ok, _} = Log.append(%WithCustomCast{order_id: 3, placed_at: DateTime.utc_now()})

      assert {:ok, [%OrderPlaced{}, %OrderShipped{}]} =
               Query.all([OrderPlaced, OrderShipped], ctx.opts)
    end

    test ":all reads every type, resolving each row's module from its own type", ctx do
      {:ok, _} = Log.append(%OrderPlaced{order_id: 1})
      {:ok, _} = Log.append(%OrderShipped{order_id: 2, carrier: "dhl"})

      assert {:ok, [%OrderPlaced{order_id: 1}, %OrderShipped{order_id: 2}]} =
               Query.all(:all, ctx.opts)
    end

    # The stance `DMEx.Message.decode/1` already takes: a reader legitimately sees
    # a log wider than its own message modules.
    test ":all skips a type this node has no module for", ctx do
      {:ok, _} = append_raw(ctx.storage, "nobody.declares.this", %{n: 1})
      {:ok, _} = Log.append(%OrderPlaced{order_id: 7})

      assert {:ok, [%OrderPlaced{order_id: 7}]} = Query.all(:all, ctx.opts)
    end

    test "an empty module list is refused rather than answered with nothing", ctx do
      assert_raise ArgumentError, ~r/empty list of message modules/, fn ->
        Query.all([], ctx.opts)
      end
    end

    test "a module that is not a message names itself", ctx do
      assert_raise ArgumentError, ~r/is not a DMEx message module/, fn ->
        Query.all([OrderPlaced, URI], ctx.opts)
      end
    end
  end

  describe "entities" do
    test "one entity's history crosses message types", ctx do
      order = entity()

      {:ok, _} = Log.append(%EntityPlaced{order_id: order, customer_id: entity()})
      {:ok, _} = Log.append(%EntityShipped{order_id: order, line_item_ids: [entity()]})
      {:ok, _} = Log.append(%EntityPlaced{order_id: entity()})

      assert {:ok, [%EntityPlaced{order_id: ^order}, %EntityShipped{order_id: ^order}]} =
               Query.all(:all, ctx.opts ++ [entities: [order]])
    end

    test "the entity and type axes narrow together", ctx do
      order = entity()

      {:ok, _} = Log.append(%EntityPlaced{order_id: order})
      {:ok, _} = Log.append(%EntityShipped{order_id: order})

      assert {:ok, [%EntityShipped{}]} = Query.all(EntityShipped, ctx.opts ++ [entities: [order]])
    end

    test "a list field is indexed per element", ctx do
      [one, two] = [entity(), entity()]

      {:ok, _} = Log.append(%EntityShipped{order_id: entity(), line_item_ids: [one, two]})

      assert {:ok, [%EntityShipped{}]} = Query.all(:all, ctx.opts ++ [entities: [two]])
    end

    test "a list of entities admits a row carrying any of them, once", ctx do
      [a, b] = [entity(), entity()]

      {:ok, _} = Log.append(%EntityPlaced{order_id: a, customer_id: b})

      assert {:ok, [%EntityPlaced{}]} = Query.all(:all, ctx.opts ++ [entities: [a, b]])
    end

    test "an unset entity field indexes nothing", ctx do
      customer = entity()

      {:ok, _} = Log.append(%EntityPlaced{order_id: entity(), customer_id: nil})

      assert {:ok, []} = Query.all(:all, ctx.opts ++ [entities: [customer]])
    end

    test "a single entity needs no list", ctx do
      order = entity()
      {:ok, _} = Log.append(%EntityPlaced{order_id: order})

      assert {:ok, [%EntityPlaced{}]} = Query.all(:all, ctx.opts ++ [entities: order])
    end

    # The opposite of an unnarrowed axis, and answered without a statement —
    # see `DMEx.Storage.select/2`.
    test "an empty entity list admits nothing", ctx do
      {:ok, _} = Log.append(%EntityPlaced{order_id: entity()})

      assert {:ok, []} = Query.all(:all, ctx.opts ++ [entities: []])
    end

    test "an identifier that is not a non-empty string names itself", ctx do
      assert_raise ArgumentError, ~r/as non-empty strings/, fn ->
        Query.all(:all, ctx.opts ++ [entities: [7]])
      end

      assert_raise ArgumentError, ~r/as non-empty strings/, fn ->
        Query.all(:all, ctx.opts ++ [entities: %{}])
      end
    end

    # The bare and the listed form go through one check, so neither shape can
    # carry an identifier the other would refuse.
    test "one identifier is checked exactly as a list of them is", ctx do
      for bad <- ["", 7] do
        assert_raise ArgumentError, ~r/as non-empty strings/, fn ->
          Query.all(:all, ctx.opts ++ [entities: bad])
        end

        assert_raise ArgumentError, ~r/as non-empty strings/, fn ->
          Query.all(:all, ctx.opts ++ [entities: [bad]])
        end
      end
    end
  end

  describe "stream/2" do
    test "pages past the limit, which is one page rather than a total", ctx do
      order = entity()
      for _ <- 1..5, do: {:ok, _} = Log.append(%EntityPlaced{order_id: order})

      streamed = :all |> Query.stream(ctx.opts ++ [entities: [order], limit: 2]) |> Enum.to_list()

      assert length(streamed) == 5
      assert Enum.all?(streamed, &match?(%EntityPlaced{order_id: ^order}, &1))
    end

    test "is lazy: taking one row does not read the whole history", ctx do
      order = entity()
      for _ <- 1..5, do: {:ok, _} = Log.append(%EntityPlaced{order_id: order})

      assert [%EntityPlaced{}] = :all |> Query.stream(ctx.opts ++ [entities: [order], limit: 2]) |> Enum.take(1)
    end

    test "an empty log streams nothing", ctx do
      assert [] = :all |> Query.stream(ctx.opts) |> Enum.to_list()
    end

    # A skipped unknown type must still advance the page, or the stream reads the
    # same rows forever.
    test "a page of nothing but unknown types still terminates", ctx do
      for n <- 1..3, do: {:ok, _} = append_raw(ctx.storage, "nobody.declares.this", %{n: n})

      assert [] = :all |> Query.stream(ctx.opts ++ [limit: 1]) |> Enum.to_list()
    end
  end

  describe "before_id" do
    test "is an exclusive upper bound", ctx do
      [_first, _second, third] = for n <- 1..3, do: elem(Log.append(%OrderPlaced{order_id: n}), 1)

      assert {:ok, [%OrderPlaced{order_id: 1}, %OrderPlaced{order_id: 2}]} =
               Query.all(OrderPlaced, ctx.opts ++ [before_id: third])
    end

    test "composes with after_id into a window", ctx do
      [first, _second, _third, fourth] =
        for n <- 1..4, do: elem(Log.append(%OrderPlaced{order_id: n}), 1)

      assert {:ok, [%OrderPlaced{order_id: 2}, %OrderPlaced{order_id: 3}]} =
               Query.all(OrderPlaced, ctx.opts ++ [after_id: first, before_id: fourth])
    end

    # The one value a caller could plausibly mean the opposite by.
    test "0 is refused rather than read as the tip", ctx do
      assert_raise ArgumentError, ~r/`before_id: 0` admits nothing/, fn ->
        Query.all(OrderPlaced, ctx.opts ++ [before_id: 0])
      end
    end

    test "anything else out of shape names the option", ctx do
      assert_raise ArgumentError, ~r/`before_id:` must be a positive integer/, fn ->
        Query.all(OrderPlaced, ctx.opts ++ [before_id: -1])
      end

      assert_raise ArgumentError, ~r/`before_id:` must be a positive integer/, fn ->
        Query.all(OrderPlaced, ctx.opts ++ [before_id: "7"])
      end
    end
  end

  describe "stream/2 backwards" do
    test "walks back from the tip, paging past the limit", ctx do
      for n <- 1..5, do: {:ok, _} = Log.append(%OrderPlaced{order_id: n})

      streamed = :all |> Query.stream(ctx.opts ++ [order: :desc, limit: 2]) |> Enum.to_list()

      assert Enum.map(streamed, & &1.order_id) == [5, 4, 3, 2, 1]
    end

    test "starts below before_id", ctx do
      ids = for n <- 1..5, do: elem(Log.append(%OrderPlaced{order_id: n}), 1)
      third = Enum.at(ids, 2)

      streamed =
        :all
        |> Query.stream(ctx.opts ++ [order: :desc, before_id: third, limit: 2])
        |> Enum.to_list()

      assert Enum.map(streamed, & &1.order_id) == [2, 1]
    end

    # The other bound is the stopping line, which is what makes a window
    # streamable from either end.
    test "stops at after_id", ctx do
      ids = for n <- 1..5, do: elem(Log.append(%OrderPlaced{order_id: n}), 1)
      second = Enum.at(ids, 1)

      streamed =
        :all
        |> Query.stream(ctx.opts ++ [order: :desc, after_id: second, limit: 2])
        |> Enum.to_list()

      assert Enum.map(streamed, & &1.order_id) == [5, 4, 3]
    end

    test "is lazy: the newest row costs one page", ctx do
      for n <- 1..5, do: {:ok, _} = Log.append(%OrderPlaced{order_id: n})

      assert [%OrderPlaced{order_id: 5}] =
               :all |> Query.stream(ctx.opts ++ [order: :desc, limit: 2]) |> Enum.take(1)
    end

    test "the two directions see exactly the same rows", ctx do
      for n <- 1..7, do: {:ok, _} = Log.append(%OrderPlaced{order_id: n})

      forwards = :all |> Query.stream(ctx.opts ++ [limit: 3]) |> Enum.to_list()
      backwards = :all |> Query.stream(ctx.opts ++ [order: :desc, limit: 3]) |> Enum.to_list()

      assert forwards == Enum.reverse(backwards)
    end

    test "an entity's history reads backwards too", ctx do
      order = entity()
      for _ <- 1..4, do: {:ok, _} = Log.append(%EntityPlaced{order_id: order})
      {:ok, _} = Log.append(%EntityPlaced{order_id: entity()})

      streamed =
        :all
        |> Query.stream(ctx.opts ++ [entities: [order], order: :desc, limit: 2])
        |> Enum.to_list()

      assert length(streamed) == 4
      assert Enum.all?(streamed, &match?(%EntityPlaced{order_id: ^order}, &1))
    end

    # Follows from the log being append-only: every new row takes an id above the
    # ceiling a backwards stream has already dropped below, so it reads a window
    # that cannot grow. A forwards stream chases a tip that moves, and the
    # contrast is the whole reason the property is worth having.
    test "does not see rows appended after it started, where a forwards one does", ctx do
      for n <- 1..4, do: {:ok, _} = Log.append(%OrderPlaced{order_id: n})

      # The append fires while the first page is being consumed, so the next page
      # is read after the log has grown.
      drain = fn stream ->
        stream
        |> Enum.reduce([], fn message, seen ->
          if seen == [], do: {:ok, _} = Log.append(%OrderPlaced{order_id: 99})

          [message.order_id | seen]
        end)
        |> Enum.reverse()
      end

      backwards = drain.(Query.stream(:all, ctx.opts ++ [order: :desc, limit: 2]))

      assert backwards == [4, 3, 2, 1]
      refute 99 in backwards

      # The forwards stream reaches the row appended during its own run, which it
      # can only do because the tip moved under it.
      assert List.last(drain.(Query.stream(:all, ctx.opts ++ [limit: 2]))) == 99
    end

    test "an empty log streams nothing backwards either", ctx do
      assert [] = :all |> Query.stream(ctx.opts ++ [order: :desc]) |> Enum.to_list()
    end
  end

  ## Helpers

  # Entity identifiers are compared as strings and never coerced, so a test that
  # needs a fresh one needs it to be a string.
  defp entity, do: "e-#{System.unique_integer([:positive])}"

  # A row under a type no message module currently declares — the only way to
  # write one, since `c:DMEx.Message.type/0` is always the current type.
  defp append_raw(storage, type, data) do
    Postgres.append(type, JSON.encode!(data), "{}", [], storage)
  end
end
