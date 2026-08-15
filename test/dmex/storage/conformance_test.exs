# One suite, run against every shipped `DMEx.Storage`. What is asserted here is
# the behaviour's contract and nothing about how an engine keeps it — so an
# adapter is finished when this file passes, and a change to the contract is one
# edit rather than one per adapter.
#
# A `for` comprehension rather than a `__using__` macro: every test below stays
# an ordinary top-level `test`, with line numbers and compile errors that point
# at the thing you are reading.
#
# Engine-specific behaviour lives in `postgres_test.exs` and `sqlite_test.exs` —
# query plans, driver error shapes, and the two places the storage formats
# genuinely differ.
for {storage, repo, tag} <- [
      {DMEx.Storage.Postgres, DMEx.Test.Repo, :database},
      {DMEx.Storage.SQLite, DMEx.Test.SQLiteRepo, :sqlite}
    ] do
  defmodule Module.concat(storage, ConformanceTest) do
    # Not async: ecto_sqlite3's sandbox does not support it, and one shape for
    # both adapters beats two.
    use DMEx.Case, async: false

    alias Ecto.Adapters.SQL

    @storage storage
    @repo repo

    @moduletag tag
    @moduletag repo: repo

    # Declared subscription identifiers, the shape DMEx.Handler renders.
    @some "orders/some"
    @one "orders/one"
    @two "billing/two"

    @megabyte 1_048_576

    setup do
      %{opts: %{repo: @repo, lock: {1_701, 4_242}, prefix: nil}}
    end

    describe "append/4" do
      test "returns the assigned id, increasing", %{opts: opts} do
        assert {:ok, first} = append("A", %{n: 1}, opts)
        assert {:ok, second} = append("A", %{n: 2}, opts)

        assert is_integer(first)
        assert second > first
      end

      test "passes metadata through untouched", %{opts: opts} do
        {:ok, id} = append("A", %{n: 1}, %{trace_id: "abc", nested: %{k: [1, 2]}}, opts)

        assert %{metadata: metadata} = fetch!(id, opts)
        assert JSON.decode!(metadata) == %{"trace_id" => "abc", "nested" => %{"k" => [1, 2]}}
      end

      test "defaults metadata to an empty object rather than NULL", %{opts: opts} do
        {:ok, id} = append("A", %{n: 1}, opts)

        assert %{metadata: "{}"} = fetch!(id, opts)
      end

      # The one write that answers with a tuple rather than raising — see *How a
      # write fails* in `DMEx.Storage`. Invalid JSON is the rejection both engines
      # share: Postgres's `jsonb` cast refuses it, SQLite's `json_valid` CHECK does.
      test "a payload the database refuses comes back as an error", %{opts: opts} do
        assert {:error, _reason} = @storage.append("A", "not json", "{}", opts)
      end
    end

    describe "read/5" do
      test "returns rows in ascending id order", %{opts: opts} do
        ids = for n <- 1..5, do: elem(append("A", %{n: n}, opts), 1)

        assert {:ok, rows} = @storage.read(0, 100, @megabyte, :all, opts)
        assert Enum.map(rows, & &1.id) == Enum.sort(ids)
      end

      test "is exclusive on the cursor", %{opts: opts} do
        [_first, second, third] = for n <- 1..3, do: elem(append("A", %{n: n}, opts), 1)

        assert {:ok, rows} = @storage.read(second, 100, @megabyte, :all, opts)
        assert Enum.map(rows, & &1.id) == [third]
      end

      test "honours the limit", %{opts: opts} do
        for n <- 1..10, do: append("A", %{n: n}, opts)

        assert {:ok, rows} = @storage.read(0, 4, @megabyte, :all, opts)
        assert length(rows) == 4
      end

      test "returns every field DMEx.Transport.SSE.Frame needs, and nothing else",
           %{opts: opts} do
        {:ok, id} = append("MyApp.Messages.OrderPlaced", %{order_id: 7}, opts)

        assert {:ok, [row]} = @storage.read(id - 1, 1, @megabyte, :all, opts)

        assert row |> Map.keys() |> Enum.sort() ==
                 [:data, :id, :inserted_at, :metadata, :type]

        assert row.id == id
        assert row.type == "MyApp.Messages.OrderPlaced"
      end

      # This is the splice invariant. DMEx.Transport.SSE.Frame.encode_row/1 copies
      # these two columns into the wire frame byte-for-byte without parsing them,
      # so an adapter that let its driver decode a JSON column into a map would
      # hand Frame a map where it expects text and build a wrong frame.
      test "returns data and metadata as JSON text, never as decoded maps", %{opts: opts} do
        {:ok, id} = append("A", %{order_id: 7, total: 1999}, %{trace_id: "abc"}, opts)

        assert {:ok, [row]} = @storage.read(id - 1, 1, @megabyte, :all, opts)

        assert is_binary(row.data)
        assert is_binary(row.metadata)
        assert JSON.decode!(row.data) == %{"order_id" => 7, "total" => 1999}
        assert JSON.decode!(row.metadata) == %{"trace_id" => "abc"}
      end

      test "returns inserted_at as a DateTime, so the wire keeps its UTC suffix",
           %{opts: opts} do
        {:ok, id} = append("A", %{n: 1}, opts)

        assert {:ok, [row]} = @storage.read(id - 1, 1, @megabyte, :all, opts)

        # A naive timestamp would silently drop the trailing Z from every frame.
        assert %DateTime{} = row.inserted_at
        assert row.inserted_at |> DateTime.to_iso8601() |> String.ends_with?("Z")
      end

      # The wire carries no `max_id`, so a read returns rows and nothing else: no
      # correlated subquery folded in, and no tuple to unwrap.
      test "an empty read is an empty list, not a tuple with a nil tip", %{opts: opts} do
        assert {:ok, []} = @storage.read(0, 10, @megabyte, :all, opts)
      end

      test "a selective read returns only the named types", %{opts: opts} do
        {:ok, _} = append("A", %{n: 1}, opts)
        {:ok, b} = append("B", %{n: 2}, opts)
        {:ok, c} = append("C", %{n: 3}, opts)

        assert {:ok, rows} = @storage.read(0, 100, @megabyte, ["B", "C"], opts)
        assert Enum.map(rows, & &1.id) == [b, c]
      end

      test "a selection nothing matches reads empty", %{opts: opts} do
        {:ok, _} = append("A", %{n: 1}, opts)

        assert {:ok, []} = @storage.read(0, 100, @megabyte, ["Z"], opts)
      end

      # An empty list is a selection that admits nothing, which is a different
      # question from `:all`. No shipped caller passes one — the plug 404s an
      # empty expansion first — so nothing but this pins the two adapters to the
      # same answer.
      test "an empty selection reads empty rather than failing", %{opts: opts} do
        {:ok, _} = append("A", %{n: 1}, opts)

        assert {:ok, []} = @storage.read(0, 100, @megabyte, [], opts)
      end
    end

    describe "read/5 across successive reads" do
      test "walks the log without repeating or skipping", %{opts: opts} do
        ids = for n <- 1..10, do: elem(append("A", %{n: n}, opts), 1)

        {:ok, first} = @storage.read(0, 4, @megabyte, :all, opts)
        last_id = List.last(first).id
        {:ok, second} = @storage.read(last_id, 4, @megabyte, :all, opts)

        assert Enum.map(first, & &1.id) == Enum.take(ids, 4)
        assert Enum.map(second, & &1.id) == ids |> Enum.drop(4) |> Enum.take(4)
      end

      # A selective read is sparse by design, so "walks the log" is about the
      # delivered subsequence rather than the id space.
      test "a selective read resumes within its own subsequence", %{opts: opts} do
        ids =
          for n <- 1..6 do
            type = if rem(n, 2) == 1, do: "A", else: "B"
            {:ok, id} = append(type, %{n: n}, opts)
            id
          end

        [first, second, third] = Enum.take_every(ids, 2)

        assert {:ok, [row]} = @storage.read(first, 1, @megabyte, ["A"], opts)
        assert row.id == second

        assert {:ok, [row]} = @storage.read(second, 1, @megabyte, ["A"], opts)
        assert row.id == third
      end
    end

    # The bound that makes the log's "memory per connection is one batch" claim
    # true in bytes and not only in rows. It has to hold *in the query*: a read
    # that returns `limit` rows and lets the caller trim them has already spent
    # the memory the bound exists to cap.
    describe "read/5 byte bound" do
      setup %{opts: opts} do
        # 1 KB of payload per row, so the arithmetic is easy to state exactly.
        big = String.duplicate("x", 1_000)

        ids = for n <- 1..20, do: elem(append("A", %{n: n, note: big}, opts), 1)

        %{ids: ids, row_bytes: row_bytes(hd(ids), opts)}
      end

      test "stops before the budget is exceeded", %{opts: opts, row_bytes: row_bytes} do
        # Room for four rows and not a fifth.
        budget = row_bytes * 4 + 1

        assert {:ok, rows} = @storage.read(0, 100, budget, :all, opts)

        assert length(rows) == 4
        assert payload_bytes(rows) <= budget
      end

      test "the row limit still applies, whichever binds first", %{opts: opts} do
        assert {:ok, rows} = @storage.read(0, 2, 10_000_000, :all, opts)

        assert length(rows) == 2
      end

      test "returns rows in id order, contiguous from the cursor", %{opts: opts, ids: ids} do
        {:ok, rows} = @storage.read(0, 100, 4_000, :all, opts)

        assert Enum.map(rows, & &1.id) == Enum.take(ids, length(rows))
      end

      # Refusing it would wedge the subscription on that row permanently, and a
      # stuck stream is a worse failure than one oversized read. This falls out
      # of comparing each row against the bytes *before* it, zero for the first.
      test "always returns one row, even one larger than the whole budget", %{opts: opts} do
        assert {:ok, [row]} = @storage.read(0, 100, 1, :all, opts)

        assert byte_size(row.data) > 1_000
      end

      test "the bound applies to a selective read too", %{opts: opts, row_bytes: row_bytes} do
        {:ok, _} = append("B", %{n: 1}, opts)

        assert {:ok, rows} = @storage.read(0, 100, row_bytes * 3 + 1, ["A"], opts)

        assert length(rows) == 3
        assert Enum.all?(rows, &(&1.type == "A"))
      end

      # The budget is counted in bytes, not characters — which only shows up on a
      # payload where the two differ.
      test "counts bytes rather than characters", %{opts: opts} do
        {:ok, id} = append("A", %{note: String.duplicate("é", 100)}, opts)

        assert {:ok, [row]} = @storage.read(id - 1, 1, @megabyte, :all, opts)

        # The adapter counts what `byte_size/1` counts, so a two-byte character
        # costs two against the budget. 100 of them plus the JSON scaffolding.
        assert payload_bytes([row]) == byte_size(row.data) + byte_size(row.metadata) + 1
        assert byte_size(row.data) > 200
      end
    end

    describe "JSON payload fidelity" do
      # A literal newline inside `data` would terminate the SSE `data:` field and
      # split the frame around it. JSON's grammar forbids raw control characters
      # in strings, so this holds by construction of the format — but only if the
      # column really is JSON, which is what this asserts.
      test "control characters stay escaped, so a frame cannot be split", %{opts: opts} do
        {:ok, id} = append("A", %{note: "line one\nline two\r\ttabbed"}, opts)

        assert %{data: data} = fetch!(id, opts)

        refute String.contains?(data, "\n")
        refute String.contains?(data, "\r")
        assert JSON.decode!(data) == %{"note" => "line one\nline two\r\ttabbed"}
      end

      test "a unicode payload round-trips", %{opts: opts} do
        {:ok, id} = append("A", %{note: "naïve 日本語 🎉"}, opts)

        assert %{data: data} = fetch!(id, opts)
        assert JSON.decode!(data) == %{"note" => "naïve 日本語 🎉"}
      end
    end

    describe "max_id/1" do
      test "is 0 on an empty log", %{opts: opts} do
        assert {:ok, 0} = @storage.max_id(opts)
      end

      test "tracks the highest id", %{opts: opts} do
        {:ok, id} = append("A", %{n: 1}, opts)

        assert {:ok, ^id} = @storage.max_id(opts)
      end

      # What an absent `Last-Event-ID` resolves to: the tip is an *exclusive*
      # cursor, so a read issued against it delivers nothing already appended.
      test "is the cursor that delivers nothing yet", %{opts: opts} do
        for n <- 1..3, do: append("A", %{n: n}, opts)

        {:ok, tip} = @storage.max_id(opts)

        assert {:ok, []} = @storage.read(tip, 100, @megabyte, :all, opts)
      end
    end

    describe "appended_since/2" do
      # The common case on an idle log, and the one the dispatcher runs once per
      # poll interval per app.
      test "is {nil, []} when nothing was appended", %{opts: opts} do
        assert {:ok, {nil, []}} = @storage.appended_since(0, opts)
      end

      test "reports the highest id and the distinct types above the cursor", %{opts: opts} do
        {:ok, _} = append("A", %{n: 1}, opts)
        {:ok, _} = append("A", %{n: 2}, opts)
        {:ok, last} = append("B", %{n: 3}, opts)

        assert {:ok, {^last, types}} = @storage.appended_since(0, opts)
        assert Enum.sort(types) == ["A", "B"]
      end

      test "sees nothing at or below the cursor", %{opts: opts} do
        {:ok, id} = append("A", %{n: 1}, opts)

        assert {:ok, {nil, []}} = @storage.appended_since(id, opts)
      end
    end

    describe "last_seen/2 and advance/3" do
      # nil, not 0. That distinction is what makes `last_seen:` apply exactly
      # once: a subscription with no cursor resolves it, and one with a cursor —
      # even at 0 — ignores it entirely.
      test "is nil before anything has committed", %{opts: opts} do
        assert {:ok, nil} = @storage.last_seen(@some, opts)
      end

      test "advance/3 sets the cursor and last_seen/2 reads it back", %{opts: opts} do
        assert :ok = @storage.advance_cursor(@some, 42, opts)

        assert {:ok, 42} = @storage.last_seen(@some, opts)
      end

      test "advance/3 upserts rather than duplicating", %{opts: opts} do
        :ok = @storage.advance_cursor(@some, 42, opts)
        :ok = @storage.advance_cursor(@some, 43, opts)

        assert {:ok, 43} = @storage.last_seen(@some, opts)
        assert count("dmex_cursors", opts) == 1
      end

      test "cursors are keyed per subscription", %{opts: opts} do
        :ok = @storage.advance_cursor(@one, 10, opts)
        :ok = @storage.advance_cursor(@two, 20, opts)

        assert {:ok, 10} = @storage.last_seen(@one, opts)
        assert {:ok, 20} = @storage.last_seen(@two, opts)
      end

      # Duplicate suppression happens in the pipeline, which is the authority, and
      # an operator resetting a cursor deliberately needs this to go both ways.
      test "advance/3 does not refuse to move backwards", %{opts: opts} do
        :ok = @storage.advance_cursor(@some, 100, opts)
        :ok = @storage.advance_cursor(@some, 5, opts)

        assert {:ok, 5} = @storage.last_seen(@some, opts)
      end

      test "stores the identifier verbatim, with no inspect/1 in the way", %{opts: opts} do
        # The id arrives as a string and goes straight into the query — there is
        # nothing left to render, and nothing that could ever be atomized back.
        :ok = @storage.advance_cursor(@some, 1, opts)

        assert [[@some]] = raw("SELECT subscription FROM dmex_cursors", opts)
      end
    end

    describe "cursor/2 and delete_cursor/2" do
      test "cursor/2 is nil for a subscription that has committed nothing", %{opts: opts} do
        assert {:ok, nil} = @storage.cursor(@some, opts)
      end

      # `last_seen/2` answers the read path's question; this answers an
      # operator's — when did this last move? — so the timestamp is a DateTime
      # on every adapter rather than whatever the driver happened to hand back.
      test "cursor/2 reports the position and when it last moved", %{opts: opts} do
        :ok = @storage.advance_cursor(@some, 42, opts)

        assert {:ok, %{last_seen: 42, updated_at: %DateTime{}}} = @storage.cursor(@some, opts)
      end

      # Distinct from advancing to 0: with no row at all, `last_seen:` applies
      # again on the subscription's next start.
      test "delete_cursor/2 removes the row entirely", %{opts: opts} do
        :ok = @storage.advance_cursor(@some, 42, opts)

        assert :ok = @storage.delete_cursor(@some, opts)
        assert {:ok, nil} = @storage.last_seen(@some, opts)
      end

      test "deleting a cursor that is not there is fine", %{opts: opts} do
        assert :ok = @storage.delete_cursor(@some, opts)
      end
    end

    describe "transaction/2" do
      test "returns the function's value", %{opts: opts} do
        assert {:ok, :value} = @storage.transaction(fn -> :value end, opts)
      end

      # What makes a handler's side effects and the cursor advance atomic.
      test "a raise rolls back and propagates", %{opts: opts} do
        assert_raise RuntimeError, "boom", fn ->
          @storage.transaction(
            fn ->
              :ok = @storage.advance_cursor(@some, 42, opts)

              raise "boom"
            end,
            opts
          )
        end

        assert {:ok, nil} = @storage.last_seen(@some, opts)
      end
    end

    describe "dead_message/2" do
      test "stores the log row verbatim, plus whose row it is", %{opts: opts} do
        assert :ok = @storage.record_dead_message(dead_row(), opts)

        assert {:ok, [row]} = @storage.list_dead_messages(@some, opts)

        assert row.id == 1042
        assert row.type == "MyApp.Messages.OrderPlaced"
        assert JSON.decode!(row.data) == %{"order_id" => 7}
        assert JSON.decode!(row.metadata) == %{"trace_id" => "abc"}

        # The adapter stamps this one: it records when the row landed here, not
        # when the message was appended.
        assert %DateTime{} = row.inserted_at
      end

      test "two subscriptions may give up on the same log row", %{opts: opts} do
        assert :ok = @storage.record_dead_message(dead_row(subscription: @one), opts)
        assert :ok = @storage.record_dead_message(dead_row(subscription: @two), opts)

        assert count("dmex_dead_messages", opts) == 2
      end

      # `DMEx.Handler.Cursors.reset!/2` moves a cursor backwards, so the same
      # subscription can reach the same message twice. A plain INSERT made the
      # second failure unrecordable, and the subscription crash-looped on a row
      # it had already given up on.
      test "one subscription may give up on the same log row twice", %{opts: opts} do
        assert :ok = @storage.record_dead_message(dead_row(), opts)
        assert :ok = @storage.record_dead_message(dead_row(type: "later.attempt"), opts)

        assert count("dmex_dead_messages", opts) == 1
        assert {:ok, [%{type: "later.attempt"}]} = @storage.list_dead_messages(@some, opts)
      end

      # Nothing is defaulted here: every slot is mandatory on a message, so a
      # transport rejects one short of them and a row that reaches storage is
      # whole — which is why the adapter matches the shape in its head.
      test "requires every key", %{opts: opts} do
        for key <- ~w(subscription id type data metadata) do
          row = Map.delete(dead_row(), key)

          assert_raise FunctionClauseError, fn -> @storage.record_dead_message(row, opts) end
        end
      end
    end

    describe "dead_messages/2 and delete_dead_message/3" do
      # The point of the table's shape: what comes back is a log row, in exactly
      # the shape read/5 returns and the pipeline consumes.
      test "reads back rows in the same shape read/5 returns", %{opts: opts} do
        :ok = @storage.record_dead_message(dead_row(), opts)

        assert {:ok, [row]} = @storage.list_dead_messages(@some, opts)

        assert row |> Map.keys() |> Enum.sort() ==
                 [:data, :id, :inserted_at, :metadata, :type]

        assert is_binary(row.data)
        assert is_binary(row.metadata)
      end

      test "returns only the given subscription's messages, oldest first", %{opts: opts} do
        :ok = @storage.record_dead_message(dead_row(subscription: @one, id: 3), opts)
        :ok = @storage.record_dead_message(dead_row(subscription: @one, id: 1), opts)
        :ok = @storage.record_dead_message(dead_row(subscription: @two, id: 2), opts)

        assert {:ok, rows} = @storage.list_dead_messages(@one, opts)
        assert Enum.map(rows, & &1.id) == [1, 3]
      end

      test "is empty for a subscription that has given up on nothing", %{opts: opts} do
        assert {:ok, []} = @storage.list_dead_messages(@some, opts)
      end

      test "delete removes one message by subscription and log id", %{opts: opts} do
        :ok = @storage.record_dead_message(dead_row(id: 1), opts)
        :ok = @storage.record_dead_message(dead_row(id: 2), opts)

        assert :ok = @storage.delete_dead_message(@some, 1, opts)

        assert {:ok, [row]} = @storage.list_dead_messages(@some, opts)
        assert row.id == 2
      end

      test "deleting something absent is fine", %{opts: opts} do
        assert :ok = @storage.delete_dead_message(@some, 999, opts)
      end
    end

    ## Helpers

    defp append(type, data, metadata \\ %{}, opts) do
      @storage.append(type, JSON.encode!(data), JSON.encode!(metadata), opts)
    end

    defp fetch!(id, opts) do
      {:ok, [row]} = @storage.read(id - 1, 1, @megabyte, :all, opts)
      row
    end

    defp raw(sql, %{repo: repo}), do: SQL.query!(repo, sql, []).rows

    # What the byte bound counts: payload plus the type.
    defp payload_bytes(rows) do
      Enum.reduce(rows, 0, fn row, total ->
        total + byte_size(row.data) + byte_size(row.metadata) + byte_size(row.type)
      end)
    end

    defp row_bytes(id, opts) do
      {:ok, [row]} = @storage.read(id - 1, 1, 10_000_000, :all, opts)

      payload_bytes([row])
    end

    defp count(table, opts) do
      [[count]] = raw("SELECT count(*) FROM #{table}", opts)
      count
    end

    # String-keyed, with `data` and `metadata` as terms: a dead row is the row the
    # pipeline was holding, and encoding it is the adapter's job.
    defp dead_row(overrides \\ []) do
      %{
        "subscription" => Keyword.get(overrides, :subscription, @some),
        "id" => Keyword.get(overrides, :id, 1042),
        "type" => Keyword.get(overrides, :type, "MyApp.Messages.OrderPlaced"),
        "data" => %{"order_id" => 7},
        "metadata" => %{"trace_id" => "abc"}
      }
    end
  end
end
