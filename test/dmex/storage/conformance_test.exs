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
        assert {:ok, first} = append(opts, "A", %{n: 1})
        assert {:ok, second} = append(opts, "A", %{n: 2})

        assert is_integer(first)
        assert second > first
      end

      test "passes metadata through untouched", %{opts: opts} do
        {:ok, id} = append(opts, "A", %{n: 1}, %{trace_id: "abc", nested: %{k: [1, 2]}})

        assert %{metadata: metadata} = fetch!(opts, id)
        assert JSON.decode!(metadata) == %{"trace_id" => "abc", "nested" => %{"k" => [1, 2]}}
      end

      test "defaults metadata to an empty object rather than NULL", %{opts: opts} do
        {:ok, id} = append(opts, "A", %{n: 1})

        assert %{metadata: "{}"} = fetch!(opts, id)
      end
    end

    describe "read/5" do
      test "returns rows in ascending id order", %{opts: opts} do
        ids = for n <- 1..5, do: elem(append(opts, "A", %{n: n}), 1)

        assert {:ok, rows} = @storage.read(opts, 0, 100, @megabyte, :all)
        assert Enum.map(rows, & &1.id) == Enum.sort(ids)
      end

      test "is exclusive on the cursor", %{opts: opts} do
        [_first, second, third] = for n <- 1..3, do: elem(append(opts, "A", %{n: n}), 1)

        assert {:ok, rows} = @storage.read(opts, second, 100, @megabyte, :all)
        assert Enum.map(rows, & &1.id) == [third]
      end

      test "honours the limit", %{opts: opts} do
        for n <- 1..10, do: append(opts, "A", %{n: n})

        assert {:ok, rows} = @storage.read(opts, 0, 4, @megabyte, :all)
        assert length(rows) == 4
      end

      test "returns every field DMEx.Transport.SSE.Frame needs, and nothing else",
           %{opts: opts} do
        {:ok, id} = append(opts, "MyApp.Messages.OrderPlaced", %{order_id: 7})

        assert {:ok, [row]} = @storage.read(opts, id - 1, 1, @megabyte, :all)

        assert Map.keys(row) |> Enum.sort() ==
                 [:data, :id, :inserted_at, :message_type, :metadata, :previous_id]

        assert row.id == id
        assert row.message_type == "MyApp.Messages.OrderPlaced"
      end

      # This is the splice invariant. DMEx.Transport.SSE.Frame.encode_row/1 copies
      # these two columns into the wire frame byte-for-byte without parsing them,
      # so an adapter that let its driver decode a JSON column into a map would
      # hand Frame a map where it expects text and build a wrong frame.
      test "returns data and metadata as JSON text, never as decoded maps", %{opts: opts} do
        {:ok, id} = append(opts, "A", %{order_id: 7, total: 1999}, %{trace_id: "abc"})

        assert {:ok, [row]} = @storage.read(opts, id - 1, 1, @megabyte, :all)

        assert is_binary(row.data)
        assert is_binary(row.metadata)
        assert JSON.decode!(row.data) == %{"order_id" => 7, "total" => 1999}
        assert JSON.decode!(row.metadata) == %{"trace_id" => "abc"}
      end

      test "returns inserted_at as a DateTime, so the wire keeps its UTC suffix",
           %{opts: opts} do
        {:ok, id} = append(opts, "A", %{n: 1})

        assert {:ok, [row]} = @storage.read(opts, id - 1, 1, @megabyte, :all)

        # A naive timestamp would silently drop the trailing Z from every frame.
        assert %DateTime{} = row.inserted_at
        assert row.inserted_at |> DateTime.to_iso8601() |> String.ends_with?("Z")
      end

      # The wire carries no `max_id`, so a read returns rows and nothing else: no
      # correlated subquery folded in, and no tuple to unwrap.
      test "an empty read is an empty list, not a tuple with a nil tip", %{opts: opts} do
        assert {:ok, []} = @storage.read(opts, 0, 10, @megabyte, :all)
      end

      test "a selective read returns only the named types", %{opts: opts} do
        {:ok, _} = append(opts, "A", %{n: 1})
        {:ok, b} = append(opts, "B", %{n: 2})
        {:ok, c} = append(opts, "C", %{n: 3})

        assert {:ok, rows} = @storage.read(opts, 0, 100, @megabyte, ["B", "C"])
        assert Enum.map(rows, & &1.id) == [b, c]
      end

      test "a selection nothing matches reads empty", %{opts: opts} do
        {:ok, _} = append(opts, "A", %{n: 1})

        assert {:ok, []} = @storage.read(opts, 0, 100, @megabyte, ["Z"])
      end
    end

    # Every row states the id of the row before it *in what this read returned*.
    # It is what lets a consumer prove nothing went missing between two frames,
    # and it cannot be `id - 1` — a selective read is sparse by design.
    describe "read/5 predecessor chain" do
      test "links each row to the one before it, back to the cursor", %{opts: opts} do
        ids = for n <- 1..5, do: elem(append(opts, "A", %{n: n}), 1)

        assert {:ok, rows} = @storage.read(opts, 0, 100, @megabyte, :all)

        assert Enum.map(rows, & &1.previous_id) == [0 | Enum.take(ids, 4)]
        assert chained?(rows, 0)
      end

      test "the first row of a resumed read links back to the cursor it was given",
           %{opts: opts} do
        [_first, second, _third] = for n <- 1..3, do: elem(append(opts, "A", %{n: n}), 1)

        assert {:ok, [row | _]} = @storage.read(opts, second, 100, @megabyte, :all)

        # Not `row.id - 1`, and not the log's first id: exactly what was asked
        # for, which is what proves the log resumed where the consumer said.
        assert row.previous_id == second
      end

      # The case that makes a stated link necessary rather than merely convenient:
      # the delivered rows are every other one, and every gap is deliberate.
      test "a selective read chains over the delivered subsequence, not the id space",
           %{opts: opts} do
        ids =
          for n <- 1..6 do
            type = if rem(n, 2) == 1, do: "A", else: "B"
            {:ok, id} = append(opts, type, %{n: n})
            id
          end

        # The ids of the "A" rows, taken as assigned rather than assumed to be
        # `1, 3, 5`: the sequence is shared with every other test in this file.
        wanted = Enum.take_every(ids, 2)

        assert {:ok, rows} = @storage.read(opts, 0, 100, @megabyte, ["A"])

        assert Enum.map(rows, & &1.id) == wanted
        assert chained?(rows, 0)
        # Each link skips the interleaved "B" row rather than pointing at it.
        assert Enum.zip(rows, tl(rows)) |> Enum.all?(fn {a, b} -> b.id - a.id > 1 end)
      end

      test "a batch cut short by bytes is still contiguous", %{opts: opts} do
        big = String.duplicate("x", 1_000)
        for n <- 1..20, do: append(opts, "A", %{n: n, note: big})

        assert {:ok, rows} = @storage.read(opts, 0, 100, 4_000, :all)

        # Truncation only ever removes the tail, so what came back still chains.
        assert length(rows) < 20
        assert chained?(rows, 0)
      end

      test "successive reads chain across the boundary between them", %{opts: opts} do
        for n <- 1..10, do: append(opts, "A", %{n: n})

        {:ok, first} = @storage.read(opts, 0, 4, @megabyte, :all)
        last_id = List.last(first).id
        {:ok, second} = @storage.read(opts, last_id, 4, @megabyte, :all)

        assert chained?(first, 0)
        assert chained?(second, last_id)
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

        ids = for n <- 1..20, do: elem(append(opts, "A", %{n: n, note: big}), 1)

        %{ids: ids, row_bytes: row_bytes(opts, hd(ids))}
      end

      test "stops before the budget is exceeded", %{opts: opts, row_bytes: row_bytes} do
        # Room for four rows and not a fifth.
        budget = row_bytes * 4 + 1

        assert {:ok, rows} = @storage.read(opts, 0, 100, budget, :all)

        assert length(rows) == 4
        assert payload_bytes(rows) <= budget
      end

      test "the row limit still applies, whichever binds first", %{opts: opts} do
        assert {:ok, rows} = @storage.read(opts, 0, 2, 10_000_000, :all)

        assert length(rows) == 2
      end

      test "returns rows in id order, contiguous from the cursor", %{opts: opts, ids: ids} do
        {:ok, rows} = @storage.read(opts, 0, 100, 4_000, :all)

        assert Enum.map(rows, & &1.id) == Enum.take(ids, length(rows))
      end

      # Refusing it would wedge the subscription on that row permanently, and a
      # stuck stream is a worse failure than one oversized read. This falls out
      # of comparing each row against the bytes *before* it, zero for the first.
      test "always returns one row, even one larger than the whole budget", %{opts: opts} do
        assert {:ok, [row]} = @storage.read(opts, 0, 100, 1, :all)

        assert byte_size(row.data) > 1_000
      end

      test "the bound applies to a selective read too", %{opts: opts, row_bytes: row_bytes} do
        {:ok, _} = append(opts, "B", %{n: 1})

        assert {:ok, rows} = @storage.read(opts, 0, 100, row_bytes * 3 + 1, ["A"])

        assert length(rows) == 3
        assert Enum.all?(rows, &(&1.message_type == "A"))
      end

      # The budget is counted in bytes, not characters — which only shows up on a
      # payload where the two differ.
      test "counts bytes rather than characters", %{opts: opts} do
        {:ok, id} = append(opts, "A", %{note: String.duplicate("é", 100)})

        assert {:ok, [row]} = @storage.read(opts, id - 1, 1, @megabyte, :all)

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
        {:ok, id} = append(opts, "A", %{note: "line one\nline two\r\ttabbed"})

        assert %{data: data} = fetch!(opts, id)

        refute String.contains?(data, "\n")
        refute String.contains?(data, "\r")
        assert JSON.decode!(data) == %{"note" => "line one\nline two\r\ttabbed"}
      end

      test "a unicode payload round-trips", %{opts: opts} do
        {:ok, id} = append(opts, "A", %{note: "naïve 日本語 🎉"})

        assert %{data: data} = fetch!(opts, id)
        assert JSON.decode!(data) == %{"note" => "naïve 日本語 🎉"}
      end
    end

    describe "max_id/1" do
      test "is 0 on an empty log", %{opts: opts} do
        assert {:ok, 0} = @storage.max_id(opts)
      end

      test "tracks the highest id", %{opts: opts} do
        {:ok, id} = append(opts, "A", %{n: 1})

        assert {:ok, ^id} = @storage.max_id(opts)
      end
    end

    describe "appended_since/2" do
      # The common case on an idle log, and the one the dispatcher runs once per
      # poll interval per app.
      test "is {nil, []} when nothing was appended", %{opts: opts} do
        assert {:ok, {nil, []}} = @storage.appended_since(opts, 0)
      end

      test "reports the highest id and the distinct types above the cursor", %{opts: opts} do
        {:ok, _} = append(opts, "A", %{n: 1})
        {:ok, _} = append(opts, "A", %{n: 2})
        {:ok, last} = append(opts, "B", %{n: 3})

        assert {:ok, {^last, types}} = @storage.appended_since(opts, 0)
        assert Enum.sort(types) == ["A", "B"]
      end

      test "sees nothing at or below the cursor", %{opts: opts} do
        {:ok, id} = append(opts, "A", %{n: 1})

        assert {:ok, {nil, []}} = @storage.appended_since(opts, id)
      end
    end

    # An instant names the *gap between* two messages, which is already what an
    # exclusive cursor is — so every case here comes out of one expression, with
    # no arithmetic and no branch.
    describe "cursor_at/2" do
      test "is 0 on an empty log", %{opts: opts} do
        assert {:ok, 0} = @storage.cursor_at(opts, ~U[2026-08-03 10:00:00Z])
      end

      test "is 0 before the first row", %{opts: opts} do
        {:ok, _} = append(opts, "A", %{n: 1})

        assert {:ok, 0} = @storage.cursor_at(opts, ~U[2000-01-01 00:00:00Z])
      end

      # What `stream_from: :current` resolves to, and why the vocabulary needs no
      # tip token: every row is before "now". A second out rather than
      # `utc_now/0` because adapters stamp at different resolutions — SQLite to
      # the millisecond — and "after the tip" should not mean "within the clock's
      # granularity of it".
      test "after the tip is the tip", %{opts: opts} do
        {:ok, _} = append(opts, "A", %{n: 1})
        {:ok, last} = append(opts, "A", %{n: 2})

        assert {:ok, ^last} = @storage.cursor_at(opts, DateTime.add(DateTime.utc_now(), 1))
      end

      # The cursor is exclusive, so landing exactly on a row's `inserted_at` puts
      # the cursor *below* it and that row is still delivered. This is what makes
      # `stream_from:` inclusive of the instant it names.
      #
      # *Below*, not *immediately* below: where the stamp resolution cannot
      # separate two appends they tie, and the cursor falls below both. Erring
      # low re-delivers; erring high would skip, which a message log may never
      # do. The exact predecessor is pinned per adapter, where the resolution is
      # known.
      test "exactly on a row's inserted_at still delivers that row", %{opts: opts} do
        {:ok, _first} = append(opts, "A", %{n: 1})
        {:ok, second} = append(opts, "A", %{n: 2})

        assert {:ok, cursor} = @storage.cursor_at(opts, inserted_at(opts, second))
        assert cursor < second
      end

      test "inserted_at ascends with id, which is what makes the cut sound",
           %{opts: opts} do
        ids = for n <- 1..5, do: elem(append(opts, "A", %{n: n}), 1)

        stamps = Enum.map(ids, &inserted_at(opts, &1))

        assert stamps == Enum.sort(stamps, &(DateTime.compare(&1, &2) != :gt))
      end
    end

    describe "last_seen/2 and advance/3" do
      # nil, not 0. That distinction is what makes `stream_from:` apply exactly
      # once: a subscription with no cursor resolves it, and one with a cursor —
      # even at 0 — ignores it entirely.
      test "is nil before anything has committed", %{opts: opts} do
        assert {:ok, nil} = @storage.last_seen(opts, @some)
      end

      test "advance/3 sets the cursor and last_seen/2 reads it back", %{opts: opts} do
        assert :ok = @storage.advance(opts, @some, 42)

        assert {:ok, 42} = @storage.last_seen(opts, @some)
      end

      test "advance/3 upserts rather than duplicating", %{opts: opts} do
        :ok = @storage.advance(opts, @some, 42)
        :ok = @storage.advance(opts, @some, 43)

        assert {:ok, 43} = @storage.last_seen(opts, @some)
        assert count(opts, "dmex_cursors") == 1
      end

      test "cursors are keyed per subscription", %{opts: opts} do
        :ok = @storage.advance(opts, @one, 10)
        :ok = @storage.advance(opts, @two, 20)

        assert {:ok, 10} = @storage.last_seen(opts, @one)
        assert {:ok, 20} = @storage.last_seen(opts, @two)
      end

      # Duplicate suppression happens in the pipeline, which is the authority, and
      # an operator resetting a cursor deliberately needs this to go both ways.
      test "advance/3 does not refuse to move backwards", %{opts: opts} do
        :ok = @storage.advance(opts, @some, 100)
        :ok = @storage.advance(opts, @some, 5)

        assert {:ok, 5} = @storage.last_seen(opts, @some)
      end

      test "stores the identifier verbatim, with no inspect/1 in the way", %{opts: opts} do
        # The id arrives as a string and goes straight into the query — there is
        # nothing left to render, and nothing that could ever be atomized back.
        :ok = @storage.advance(opts, @some, 1)

        assert [[@some]] = raw(opts, "SELECT subscription FROM dmex_cursors")
      end
    end

    describe "cursor/2 and delete_cursor/2" do
      test "cursor/2 is nil for a subscription that has committed nothing", %{opts: opts} do
        assert {:ok, nil} = @storage.cursor(opts, @some)
      end

      # `last_seen/2` answers the read path's question; this answers an
      # operator's — when did this last move? — so the timestamp is a DateTime
      # on every adapter rather than whatever the driver happened to hand back.
      test "cursor/2 reports the position and when it last moved", %{opts: opts} do
        :ok = @storage.advance(opts, @some, 42)

        assert {:ok, %{last_seen: 42, updated_at: %DateTime{}}} = @storage.cursor(opts, @some)
      end

      # Distinct from advancing to 0: with no row at all, `stream_from:` applies
      # again on the subscription's next start.
      test "delete_cursor/2 removes the row entirely", %{opts: opts} do
        :ok = @storage.advance(opts, @some, 42)

        assert :ok = @storage.delete_cursor(opts, @some)
        assert {:ok, nil} = @storage.last_seen(opts, @some)
      end

      test "deleting a cursor that is not there is fine", %{opts: opts} do
        assert :ok = @storage.delete_cursor(opts, @some)
      end
    end

    describe "transaction/2" do
      test "returns the function's value", %{opts: opts} do
        assert {:ok, :value} = @storage.transaction(opts, fn -> :value end)
      end

      # What makes a handler's side effects and the cursor advance atomic.
      test "a raise rolls back and propagates", %{opts: opts} do
        assert_raise RuntimeError, "boom", fn ->
          @storage.transaction(opts, fn ->
            :ok = @storage.advance(opts, @some, 42)

            raise "boom"
          end)
        end

        assert {:ok, nil} = @storage.last_seen(opts, @some)
      end
    end

    describe "dead_message/2" do
      test "stores the log row verbatim, plus whose row it is", %{opts: opts} do
        assert :ok = @storage.dead_message(opts, dead_attrs())

        assert {:ok, [row]} = @storage.dead_messages(opts, @some)

        assert row.id == 1042
        assert row.message_type == "MyApp.Messages.OrderPlaced"
        assert JSON.decode!(row.data) == %{"order_id" => 7}
        assert JSON.decode!(row.metadata) == %{"trace_id" => "abc"}

        # The adapter stamps this one: it records when the row landed here, not
        # when the message was appended.
        assert %DateTime{} = row.inserted_at
      end

      test "two subscriptions may give up on the same log row", %{opts: opts} do
        assert :ok = @storage.dead_message(opts, dead_attrs(subscription: @one))
        assert :ok = @storage.dead_message(opts, dead_attrs(subscription: @two))

        assert count(opts, "dmex_dead_messages") == 2
      end

      test "defaults metadata to an empty object", %{opts: opts} do
        attrs = Map.put(dead_attrs(), :metadata, nil)

        assert :ok = @storage.dead_message(opts, attrs)
        assert {:ok, [%{metadata: "{}"}]} = @storage.dead_messages(opts, @some)
      end
    end

    describe "dead_messages/2 and delete_dead_message/3" do
      # The point of the table's shape: what comes back is a log row, in exactly
      # the shape read/5 returns and the pipeline consumes.
      test "reads back rows in the same shape read/5 returns", %{opts: opts} do
        :ok = @storage.dead_message(opts, dead_attrs())

        assert {:ok, [row]} = @storage.dead_messages(opts, @some)

        assert Map.keys(row) |> Enum.sort() ==
                 [:data, :id, :inserted_at, :message_type, :metadata, :previous_id]

        assert row.previous_id == nil
        assert is_binary(row.data)
        assert is_binary(row.metadata)
      end

      test "returns only the given subscription's messages, oldest first", %{opts: opts} do
        :ok = @storage.dead_message(opts, dead_attrs(subscription: @one, id: 3))
        :ok = @storage.dead_message(opts, dead_attrs(subscription: @one, id: 1))
        :ok = @storage.dead_message(opts, dead_attrs(subscription: @two, id: 2))

        assert {:ok, rows} = @storage.dead_messages(opts, @one)
        assert Enum.map(rows, & &1.id) == [1, 3]
      end

      test "is empty for a subscription that has given up on nothing", %{opts: opts} do
        assert {:ok, []} = @storage.dead_messages(opts, @some)
      end

      test "delete removes one message by subscription and log id", %{opts: opts} do
        :ok = @storage.dead_message(opts, dead_attrs(id: 1))
        :ok = @storage.dead_message(opts, dead_attrs(id: 2))

        assert :ok = @storage.delete_dead_message(opts, @some, 1)

        assert {:ok, [row]} = @storage.dead_messages(opts, @some)
        assert row.id == 2
      end

      test "deleting something absent is fine", %{opts: opts} do
        assert :ok = @storage.delete_dead_message(opts, @some, 999)
      end
    end

    ## Helpers

    defp append(opts, type, data, metadata \\ %{}) do
      @storage.append(opts, type, JSON.encode!(data), JSON.encode!(metadata))
    end

    defp fetch!(opts, id) do
      {:ok, [row]} = @storage.read(opts, id - 1, 1, @megabyte, :all)
      row
    end

    defp raw(%{repo: repo}, sql), do: SQL.query!(repo, sql, []).rows

    defp inserted_at(opts, id), do: opts |> fetch!(id) |> Map.fetch!(:inserted_at)

    # Every row claims the one before it, and the first claims `from`.
    defp chained?(rows, from) do
      rows
      |> Enum.reduce_while(from, fn row, previous ->
        if row.previous_id == previous, do: {:cont, row.id}, else: {:halt, :broken}
      end)
      |> Kernel.!=(:broken)
    end

    # What the byte bound counts: payload plus the type.
    defp payload_bytes(rows) do
      Enum.reduce(rows, 0, fn row, total ->
        total + byte_size(row.data) + byte_size(row.metadata) + byte_size(row.message_type)
      end)
    end

    defp row_bytes(opts, id) do
      {:ok, [row]} = @storage.read(opts, id - 1, 1, 10_000_000, :all)

      payload_bytes([row])
    end

    defp count(opts, table) do
      [[count]] = raw(opts, "SELECT count(*) FROM #{table}")
      count
    end

    defp dead_attrs(overrides \\ []) do
      %{
        subscription: Keyword.get(overrides, :subscription, @some),
        id: Keyword.get(overrides, :id, 1042),
        message_type: "MyApp.Messages.OrderPlaced",
        data: ~s({"order_id":7}),
        metadata: ~s({"trace_id":"abc"})
      }
    end
  end
end
