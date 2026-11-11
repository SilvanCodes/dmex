defmodule DMEx.Storage.PostgresTest do
  # The contract itself is pinned once for every adapter, in
  # `conformance_test.exs`. What is left here is Postgres and nothing else: the
  # plan its read depends on, the jsonb behaviour that plan is built around, and
  # the driver's own error shape.
  use DMEx.Case, async: true, database: true

  alias DMEx.Storage.Postgres
  alias Ecto.Adapters.SQL

  @adapter Postgres
  setup do
    %{opts: %{repo: Repo, lock: {1_701, 4_242}, prefix: nil}}
  end

  describe "the read plan" do
    setup %{opts: opts} do
      big = String.duplicate("x", 1_000)

      for n <- 1..20, do: append("A", %{n: n, note: big}, opts)

      :ok
    end

    # The window function has to run over the *limited* batch, not over every
    # row after the cursor — in one SELECT, Postgres computes window functions
    # before LIMIT, so the naive shape would scan the whole log to answer a
    # bounded read. Asserted through the plan rather than through a stopwatch.
    test "the running sum is computed over the limited batch, not the whole log",
         %{opts: opts} do
      plan = explain(0, 3, 10_000_000, opts)

      assert plan =~ ~r/Limit/
      assert plan =~ ~r/WindowAgg/

      # And the limit is below the window, not above it: the scan stops at three
      # rows rather than the window running over all twenty.
      [_above, below] = String.split(plan, "WindowAgg", parts: 2)
      assert below =~ ~r/Limit/
    end
  end

  describe "jsonb" do
    # Regression guard. Writing the parameter as `$n::jsonb` rather than
    # `$n::text::jsonb` makes Postgres describe it as jsonb, at which point
    # Postgrex encodes the already-encoded binary through its own JSON codec and
    # stores the *string* "{\"order_id\":7}" instead of the object. The insert
    # succeeds, the row is valid jsonb, and only a consumer decoding the payload
    # ever notices — so it has to be asserted structurally.
    test "payloads are stored as JSON objects, not as JSON strings", %{opts: opts} do
      {:ok, _} = append("A", %{order_id: 7}, %{trace_id: "abc"}, opts)

      assert [["object", "object"]] =
               raw("SELECT jsonb_typeof(data), jsonb_typeof(metadata) FROM dmex_messages", opts)
    end

    test "normalization drops duplicate keys but still yields valid JSON", %{opts: opts} do
      # Written as raw text rather than through JSON.encode!/1, which cannot
      # produce a duplicate key. This is the documented cost of the jsonb
      # column: stored bytes are not the publisher's bytes. `DMEx.Storage.SQLite`
      # stores text and does not pay it — see its inverse of this test.
      {:ok, id} = Postgres.append("A", ~s({"a":1,"a":2}), "{}", [], opts)

      assert {:ok, [row]} = Postgres.read(id - 1, 1, 1_048_576, :all, opts)
      assert JSON.decode!(row.data) == %{"a" => 2}
    end
  end

  # At boot rather than at the first query, so an option that will never be
  # honoured fails where it was written — `c:DMEx.Storage.validate!/1`.
  describe "validate!/1" do
    test "accepts a log's options and a subscription's", %{opts: opts} do
      assert :ok = Postgres.validate!(opts)

      # A subscription's half carries no `:lock` at all, and that is not a fault.
      assert :ok = Postgres.validate!(%{repo: Repo, prefix: nil})
      assert :ok = Postgres.validate!(%{repo: Repo, prefix: "reporting"})
    end

    test "refuses a prefix it cannot splice into a quoted identifier", %{opts: opts} do
      for prefix <- [~s(rep"orting), "reporting; DROP TABLE dmex_messages", "has space", "9lives"] do
        assert_raise ArgumentError, ~r/not a usable Postgres schema name/, fn ->
          Postgres.validate!(%{opts | prefix: prefix})
        end
      end

      assert_raise ArgumentError, ~r/must be a schema name as a string or nil/, fn ->
        Postgres.validate!(%{opts | prefix: :reporting})
      end
    end
  end

  ## Helpers

  # The fourth contract on `c:DMEx.Storage.append/5`. The ordering that matters is
  # message-first: a NULL entity is refused by the column, so the *second*
  # statement fails after the first succeeded — which is the only way a row could
  # ever commit without its entities. In an append-only log that row would be
  # invisibly absent from every entity query, forever, with no later write to
  # notice.
  describe "a message and its entities commit together" do
    test "a refused entity row takes the message with it", %{opts: opts} do
      assert {:error, _reason} = @adapter.append("A", ~s({"n":1}), "{}", [nil], opts)

      assert count("dmex_messages") == 0
      assert count("dmex_message_entities") == 0
    end

    test "entity rows land under the id the append assigned", %{opts: opts} do
      {:ok, id} = @adapter.append("A", ~s({"n":1}), "{}", ["e1", "e2"], opts)

      assert entities(id) == ["e1", "e2"]
    end
  end

  defp count(table) do
    %{rows: [[count]]} = SQL.query!(Repo, "SELECT count(*) FROM #{table}", [])

    count
  end

  defp entities(id) do
    %{rows: rows} =
      SQL.query!(Repo, "SELECT entity FROM dmex_message_entities WHERE id = #{id} ORDER BY entity", [])

    List.flatten(rows)
  end

  defp append(type, data, metadata \\ %{}, opts) do
    Postgres.append(type, JSON.encode!(data), JSON.encode!(metadata), [], opts)
  end

  defp raw(sql, %{repo: repo}), do: SQL.query!(repo, sql, []).rows

  # The read's own SQL, run through EXPLAIN, so the plan shape is asserted
  # rather than assumed. Reaches into the adapter's private query builder
  # deliberately: what is under test is that statement, and no restatement of it
  # would prove anything.
  defp explain(cursor, limit, max_bytes, %{repo: repo} = opts) do
    sql = :erlang.apply(Postgres, :__read_sql__, ["", opts])

    repo
    |> SQL.query!("EXPLAIN " <> sql, [cursor, limit, max_bytes])
    |> Map.fetch!(:rows)
    |> List.flatten()
    |> Enum.join("\n")
  end
end
