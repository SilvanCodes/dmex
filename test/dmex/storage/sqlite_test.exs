defmodule DMEx.Storage.SQLiteTest do
  # The contract itself is pinned once for every adapter, in
  # `conformance_test.exs`. What is left here is SQLite and nothing else: the
  # storage format's fidelity, the query plan, and the options this adapter
  # refuses.
  use DMEx.Case, async: false, database: true, repo: DMEx.Test.SQLiteRepo

  alias DMEx.Storage.SQLite
  alias Ecto.Adapters.SQL

  @megabyte 1_048_576

  setup do
    %{opts: %{repo: Repo, lock: {1_701, 4_242}, prefix: nil}}
  end

  describe "payload fidelity" do
    # The inverse of the Postgres adapter's "normalization drops duplicate keys".
    # A TEXT column stores the publisher's bytes, so what a consumer decodes is
    # what a publisher encoded — the trade that buys no indexed access into
    # payloads.
    test "duplicate keys and key order survive, byte for byte", %{opts: opts} do
      written = ~s({"z":1,"a":2,"z":3})

      {:ok, id} = SQLite.append(opts, "A", written, "{}")

      assert {:ok, [row]} = SQLite.read(opts, id - 1, 1, @megabyte, :all)
      assert row.data == written
    end

    test "a payload that is not JSON is refused by the column", %{opts: opts} do
      assert {:error, _reason} = SQLite.append(opts, "A", "not json", "{}")
    end
  end

  describe "the read plan" do
    setup %{opts: opts} do
      for n <- 1..20, do: SQLite.append(opts, "A", ~s({"n":#{n}}), "{}")

      :ok
    end

    test "a whole-log read walks the primary key", %{opts: opts} do
      plan = explain(opts, "")

      # `id > ?` on the rowid, which is the table itself — no index to consult
      # and no sort to do.
      assert plan =~ ~r/SEARCH .*dmex_messages USING INTEGER PRIMARY KEY/
    end

    test "a selective read takes the (type, id) index", %{opts: opts} do
      plan = explain(opts, "AND type IN (?4)")

      assert plan =~ "dmex_messages_type_id_index"
    end

    # The window function has to run over the *limited* batch. SQLite also
    # computes windows before LIMIT within one SELECT, and it does not flatten a
    # subquery containing one — which is what keeps `page` a separate level.
    test "the running sum runs over a bounded subquery", %{opts: opts} do
      plan = explain(opts, "")

      assert plan =~ "CO-ROUTINE"
      assert plan =~ ~r/SCAN .*page|CO-ROUTINE page/
    end
  end

  describe "read/5" do
    test "refuses a selection larger than SQLite can bind", %{opts: opts} do
      types = for n <- 1..1_000, do: "T#{n}"

      assert_raise ArgumentError, ~r/bound-parameter limit/, fn ->
        SQLite.read(opts, 0, 10, @megabyte, types)
      end
    end
  end

  describe "validate!/1" do
    test "refuses a :prefix rather than ignoring it", %{opts: opts} do
      assert_raise ArgumentError, ~r/SQLite has no schemas/, fn ->
        SQLite.validate!(%{opts | prefix: "reporting"})
      end
    end

    test "refuses a repo that is not in immediate transaction mode", %{opts: opts} do
      assert_raise ArgumentError, ~r/requires :immediate/, fn ->
        SQLite.validate!(%{opts | repo: DMEx.Test.DeferredRepo})
      end
    end

    test "accepts the configured repo", %{opts: opts} do
      assert :ok = SQLite.validate!(opts)
    end
  end

  describe "default_notifier/0" do
    # SQLite cannot notify through the database, so it names the mechanism that
    # needs none.
    test "is the in-BEAM notifier" do
      assert SQLite.default_notifier() == DMEx.Notifier.Beam
    end
  end

  ## Helpers

  # EXPLAIN QUERY PLAN over the adapter's real statement. Reaches for the
  # private builder deliberately: what is under test is that statement, and no
  # restatement of it would prove anything.
  defp explain(%{repo: repo}, predicate) do
    sql = :erlang.apply(SQLite, :__read_sql__, [predicate])
    params = if predicate == "", do: [0, 3, 10_000_000], else: [0, 3, 10_000_000, "A"]

    repo
    |> SQL.query!("EXPLAIN QUERY PLAN " <> sql, params)
    |> Map.fetch!(:rows)
    |> Enum.map_join("\n", &List.last/1)
  end
end
