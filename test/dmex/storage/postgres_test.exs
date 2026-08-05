defmodule DMEx.Storage.PostgresTest do
  # The contract itself is pinned once for every adapter, in
  # `conformance_test.exs`. What is left here is Postgres and nothing else: the
  # plan its read depends on, the jsonb behaviour that plan is built around, and
  # the driver's own error shape.
  use DMEx.Case, async: true, database: true

  alias Ecto.Adapters.SQL

  alias DMEx.Storage.Postgres

  @some "orders/some"

  setup do
    %{opts: %{repo: Repo, lock: {1_701, 4_242}, prefix: nil}}
  end

  describe "the read plan" do
    setup %{opts: opts} do
      big = String.duplicate("x", 1_000)

      for n <- 1..20, do: append(opts, "A", %{n: n, note: big})

      :ok
    end

    # The window function has to run over the *limited* batch, not over every
    # row after the cursor — in one SELECT, Postgres computes window functions
    # before LIMIT, so the naive shape would scan the whole log to answer a
    # bounded read. Asserted through the plan rather than through a stopwatch.
    test "the running sum is computed over the limited batch, not the whole log",
         %{opts: opts} do
      plan = explain(opts, 0, 3, 10_000_000)

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
      {:ok, _} = append(opts, "A", %{order_id: 7}, %{trace_id: "abc"})

      assert [["object", "object"]] =
               raw(opts, "SELECT jsonb_typeof(data), jsonb_typeof(metadata) FROM dmex_messages")
    end

    test "normalization drops duplicate keys but still yields valid JSON", %{opts: opts} do
      # Written as raw text rather than through JSON.encode!/1, which cannot
      # produce a duplicate key. This is the documented cost of the jsonb
      # column: stored bytes are not the publisher's bytes. `DMEx.Storage.SQLite`
      # stores text and does not pay it — see its inverse of this test.
      {:ok, id} = Postgres.append(opts, "A", ~s({"a":1,"a":2}), "{}")

      assert {:ok, [row]} = Postgres.read(opts, id - 1, 1, 1_048_576, :all)
      assert JSON.decode!(row.data) == %{"a" => 2}
    end
  end

  describe "dead_message/2" do
    # The error term is the driver's, not DMEx's — the behaviour promises
    # `{:error, term()}` and nothing about its shape.
    test "one subscription cannot record the same message twice", %{opts: opts} do
      assert :ok = Postgres.dead_message(opts, dead_attrs())

      assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
               Postgres.dead_message(opts, dead_attrs())
    end
  end

  ## Helpers

  defp append(opts, type, data, metadata \\ %{}) do
    Postgres.append(opts, type, JSON.encode!(data), JSON.encode!(metadata))
  end

  defp raw(%{repo: repo}, sql), do: SQL.query!(repo, sql, []).rows

  # The read's own SQL, run through EXPLAIN, so the plan shape is asserted
  # rather than assumed. Reaches into the adapter's private query builder
  # deliberately: what is under test is that statement, and no restatement of it
  # would prove anything.
  defp explain(%{repo: repo} = opts, cursor, limit, max_bytes) do
    sql = :erlang.apply(Postgres, :__read_sql__, [opts, ""])

    repo
    |> SQL.query!("EXPLAIN " <> sql, [cursor, limit, max_bytes])
    |> Map.fetch!(:rows)
    |> List.flatten()
    |> Enum.join("\n")
  end

  defp dead_attrs do
    %{
      subscription: @some,
      id: 1042,
      message_type: "MyApp.Messages.OrderPlaced",
      data: ~s({"order_id":7}),
      metadata: ~s({"trace_id":"abc"})
    }
  end
end
