defmodule DMEx.Storage.PostgresTest do
  use DMEx.Case, async: true, database: true

  alias Ecto.Adapters.SQL

  alias DMEx.Storage.Postgres

  # Declared subscription identifiers, the shape DMEx.Handler renders.
  @some "orders/some"
  @one "orders/one"
  @two "billing/two"

  setup do
    %{opts: %{repo: Repo, lock: {1_701, 4_242}, channel: "dmex_storage_test", prefix: nil}}
  end

  describe "append/2" do
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

    test "writes a batch in entry order, with contiguous ascending ids", %{opts: opts} do
      entries = for n <- 1..5, do: {"A", ~s({"n":#{n}}), "{}"}

      assert {:ok, ids} = Postgres.append(opts, entries)

      assert length(ids) == 5
      assert ids == Enum.sort(ids)
      assert ids == Enum.to_list(hd(ids)..List.last(ids))

      {:ok, rows} = Postgres.read(opts, 0, 10, 1_048_576, :all)

      assert Enum.map(rows, &JSON.decode!(&1.data)["n"]) == [1, 2, 3, 4, 5]
    end

    test "keeps each entry's own type and metadata", %{opts: opts} do
      entries = [
        {"A", ~s({"n":1}), ~s({"trace_id":"one"})},
        {"B", ~s({"n":2}), ~s({"trace_id":"two"})}
      ]

      assert {:ok, [_, _]} = Postgres.append(opts, entries)

      {:ok, rows} = Postgres.read(opts, 0, 10, 1_048_576, :all)

      assert Enum.map(rows, &{&1.message_type, JSON.decode!(&1.metadata)["trace_id"]}) ==
               [{"A", "one"}, {"B", "two"}]
    end

    test "stores a batch's payloads as JSON objects too", %{opts: opts} do
      # The `text[]` parameter is the array-shaped version of the `$n::text::jsonb`
      # trap the JSON fidelity block guards — same failure, same structural check.
      {:ok, _} = Postgres.append(opts, [{"A", ~s({"n":1}), ~s({"t":"x"})}])

      assert [["object", "object"]] =
               raw(opts, "SELECT jsonb_typeof(data), jsonb_typeof(metadata) FROM dmex_messages")
    end

    test "an empty batch writes nothing", %{opts: opts} do
      assert {:ok, []} = Postgres.append(opts, [])

      assert {:ok, 0} = Postgres.max_id(opts)
    end
  end

  describe "read/5" do
    test "returns rows in ascending id order", %{opts: opts} do
      ids = for n <- 1..5, do: elem(append(opts, "A", %{n: n}), 1)

      assert {:ok, rows} = Postgres.read(opts, 0, 100, 1_048_576, :all)
      assert Enum.map(rows, & &1.id) == Enum.sort(ids)
    end

    test "is exclusive on the cursor", %{opts: opts} do
      [_first, second, third] = for n <- 1..3, do: elem(append(opts, "A", %{n: n}), 1)

      assert {:ok, rows} = Postgres.read(opts, second, 100, 1_048_576, :all)
      assert Enum.map(rows, & &1.id) == [third]
    end

    test "honours the limit", %{opts: opts} do
      for n <- 1..10, do: append(opts, "A", %{n: n})

      assert {:ok, rows} = Postgres.read(opts, 0, 4, 1_048_576, :all)
      assert length(rows) == 4
    end

    test "returns every field DMEx.Transport.SSE.Frame needs, and nothing else", %{opts: opts} do
      {:ok, id} = append(opts, "MyApp.Messages.OrderPlaced", %{order_id: 7})

      assert {:ok, [row]} = Postgres.read(opts, id - 1, 1, 1_048_576, :all)

      assert Map.keys(row) |> Enum.sort() ==
               [:data, :id, :inserted_at, :message_type, :metadata, :previous_id]

      assert row.id == id
      assert row.message_type == "MyApp.Messages.OrderPlaced"
    end

    # This is the splice invariant. DMEx.Transport.SSE.Frame.encode_row/1 copies these two
    # columns into the wire frame byte-for-byte without parsing them, so an
    # adapter that let its driver decode jsonb into a map would hand Frame a map
    # where it expects text and build a wrong frame.
    test "returns data and metadata as JSON text, never as decoded maps", %{opts: opts} do
      {:ok, id} = append(opts, "A", %{order_id: 7, total: 1999}, %{trace_id: "abc"})

      assert {:ok, [row]} = Postgres.read(opts, id - 1, 1, 1_048_576, :all)

      assert is_binary(row.data)
      assert is_binary(row.metadata)
      assert JSON.decode!(row.data) == %{"order_id" => 7, "total" => 1999}
      assert JSON.decode!(row.metadata) == %{"trace_id" => "abc"}
    end

    test "returns inserted_at as a DateTime, so the wire keeps its UTC suffix", %{opts: opts} do
      {:ok, id} = append(opts, "A", %{n: 1})

      assert {:ok, [row]} = Postgres.read(opts, id - 1, 1, 1_048_576, :all)

      # timestamptz -> %DateTime{}; a plain `timestamp` column would come back
      # as %NaiveDateTime{} and silently drop the trailing Z from every frame.
      assert %DateTime{} = row.inserted_at
      assert row.inserted_at |> DateTime.to_iso8601() |> String.ends_with?("Z")
    end

    # The wire carries no `max_id`, so a read returns rows and nothing else: no
    # correlated subquery folded in, and no tuple to unwrap.
    test "an empty read is an empty list, not a tuple with a nil tip", %{opts: opts} do
      assert {:ok, []} = Postgres.read(opts, 0, 10, 1_048_576, :all)
    end
  end

  # Every row states the id of the row before it *in what this read returned*.
  # It is what lets a consumer prove nothing went missing between two frames,
  # and it cannot be `id - 1` — a selective read is sparse by design.
  describe "read/5 predecessor chain" do
    test "links each row to the one before it, back to the cursor", %{opts: opts} do
      ids = for n <- 1..5, do: elem(append(opts, "A", %{n: n}), 1)

      assert {:ok, rows} = Postgres.read(opts, 0, 100, 1_048_576, :all)

      assert Enum.map(rows, & &1.previous_id) == [0 | Enum.take(ids, 4)]
      assert chained?(rows, 0)
    end

    test "the first row of a resumed read links back to the cursor it was given",
         %{opts: opts} do
      [_first, second, _third] = for n <- 1..3, do: elem(append(opts, "A", %{n: n}), 1)

      assert {:ok, [row | _]} = Postgres.read(opts, second, 100, 1_048_576, :all)

      # Not `row.id - 1`, and not the log's first id: exactly what was asked for,
      # which is what proves the log resumed where the consumer said.
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
      # `1, 3, 5`: the sequence is shared with every other async test, and it
      # does not roll back, so only their relative order is ours to rely on.
      wanted = Enum.take_every(ids, 2)

      assert {:ok, rows} = Postgres.read(opts, 0, 100, 1_048_576, ["A"])

      assert Enum.map(rows, & &1.id) == wanted
      assert chained?(rows, 0)
      # Each link skips the interleaved "B" row rather than pointing at it.
      assert Enum.zip(rows, tl(rows)) |> Enum.all?(fn {a, b} -> b.id - a.id > 1 end)
    end

    test "a batch cut short by bytes is still contiguous", %{opts: opts} do
      big = String.duplicate("x", 1_000)
      for n <- 1..20, do: append(opts, "A", %{n: n, note: big})

      assert {:ok, rows} = Postgres.read(opts, 0, 100, 4_000, :all)

      # Truncation only ever removes the tail, so what came back still chains.
      assert length(rows) < 20
      assert chained?(rows, 0)
    end

    test "successive reads chain across the boundary between them", %{opts: opts} do
      for n <- 1..10, do: append(opts, "A", %{n: n})

      {:ok, first} = Postgres.read(opts, 0, 4, 1_048_576, :all)
      last_id = List.last(first).id
      {:ok, second} = Postgres.read(opts, last_id, 4, 1_048_576, :all)

      assert chained?(first, 0)
      assert chained?(second, last_id)
    end
  end

  # The bound that makes the log's "memory per connection is one batch"
  # claim true in bytes and not only in rows. It has to hold *in the query*: a
  # read that returns `limit` rows and lets the caller trim them has already
  # spent the memory the bound exists to cap.
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

      assert {:ok, rows} = Postgres.read(opts, 0, 100, budget, :all)

      assert length(rows) == 4
      assert payload_bytes(rows) <= budget
    end

    test "the row limit still applies, whichever binds first", %{opts: opts} do
      assert {:ok, rows} = Postgres.read(opts, 0, 2, 10_000_000, :all)

      assert length(rows) == 2
    end

    test "returns rows in id order, contiguous from the cursor", %{opts: opts, ids: ids} do
      {:ok, rows} = Postgres.read(opts, 0, 100, 4_000, :all)

      assert Enum.map(rows, & &1.id) == Enum.take(ids, length(rows))
    end

    # Refusing it would wedge the subscription on that row permanently, and a
    # stuck stream is a worse failure than one oversized read. This falls out of
    # comparing each row against the bytes *before* it, which is zero for the
    # first.
    test "always returns one row, even one larger than the whole budget", %{opts: opts} do
      assert {:ok, [row]} = Postgres.read(opts, 0, 100, 1, :all)

      assert byte_size(row.data) > 1_000
    end

    test "the bound applies to a selective read too", %{opts: opts, row_bytes: row_bytes} do
      {:ok, _} = append(opts, "B", %{n: 1})

      assert {:ok, rows} = Postgres.read(opts, 0, 100, row_bytes * 3 + 1, ["A"])

      assert length(rows) == 3
      assert Enum.all?(rows, &(&1.message_type == "A"))
    end

    # The window function has to run over the *limited* batch, not over every
    # row after the cursor — in one SELECT, Postgres computes window functions
    # before LIMIT, so the naive shape would scan the whole log to answer a
    # bounded read. Asserted through the plan rather than through a stopwatch.
    test "the running sum is computed over the limited batch, not the whole log",
         %{opts: opts} do
      plan = explain(opts, 0, 3, 10_000_000, :all)

      # The index scan feeding the window carries the row limit with it.
      assert plan =~ ~r/Limit/
      assert plan =~ ~r/WindowAgg/

      # And the limit is below the window, not above it: the scan stops at three
      # rows rather than the window running over all twenty.
      [_above, below] = String.split(plan, "WindowAgg", parts: 2)
      assert below =~ ~r/Limit/
    end
  end

  describe "JSON payload fidelity" do
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

    test "jsonb normalization drops duplicate keys but still yields valid JSON", %{opts: opts} do
      # Written as raw text rather than through JSON.encode!/1, which cannot
      # produce a duplicate key. This is the documented cost of the jsonb
      # column: stored bytes are not the publisher's bytes.
      {:ok, [id]} = Postgres.append(opts, [{"A", ~s({"a":1,"a":2}), "{}"}])

      assert %{data: data} = fetch!(opts, id)
      assert JSON.decode!(data) == %{"a" => 2}
    end

    test "a unicode payload round-trips", %{opts: opts} do
      {:ok, id} = append(opts, "A", %{note: "naïve 日本語 🎉"})

      assert %{data: data} = fetch!(opts, id)
      assert JSON.decode!(data) == %{"note" => "naïve 日本語 🎉"}
    end
  end

  describe "max_id/1" do
    test "is 0 on an empty log", %{opts: opts} do
      assert {:ok, 0} = Postgres.max_id(opts)
    end

    test "tracks the highest id", %{opts: opts} do
      {:ok, id} = append(opts, "A", %{n: 1})

      assert {:ok, ^id} = Postgres.max_id(opts)
    end
  end

  describe "last_seen/2 and advance/4" do
    # nil, not 0. That distinction is what makes `start: :latest` meaningful
    # exactly once: a subscription with no cursor resolves against the tip, and
    # one with a cursor — even at 0 — ignores :start entirely.
    test "is nil before anything has committed", %{opts: opts} do
      assert {:ok, nil} = Postgres.last_seen(opts, @some)
    end

    test "advance/4 sets the cursor and last_seen/2 reads it back", %{opts: opts} do
      assert :ok = Postgres.advance(opts, @some, 42)

      assert {:ok, 42} = Postgres.last_seen(opts, @some)
    end

    test "advance/4 upserts rather than duplicating", %{opts: opts} do
      :ok = Postgres.advance(opts, @some, 42)
      :ok = Postgres.advance(opts, @some, 43)

      assert {:ok, 43} = Postgres.last_seen(opts, @some)
      assert count(opts, "dmex_cursors") == 1
    end

    test "cursors are keyed per subscription", %{opts: opts} do
      :ok = Postgres.advance(opts, @one, 10)
      :ok = Postgres.advance(opts, @two, 20)

      assert {:ok, 10} = Postgres.last_seen(opts, @one)
      assert {:ok, 20} = Postgres.last_seen(opts, @two)
    end

    # Duplicate suppression happens in the pipeline, which is the authority, and
    # an operator resetting a cursor deliberately needs this to go both ways.
    test "advance/4 does not refuse to move backwards", %{opts: opts} do
      :ok = Postgres.advance(opts, @some, 100)
      :ok = Postgres.advance(opts, @some, 5)

      assert {:ok, 5} = Postgres.last_seen(opts, @some)
    end

    test "stores the identifier verbatim, with no inspect/1 in the way", %{opts: opts} do
      # The id arrives as a string and goes straight into the query — there is
      # nothing left to render, and nothing that could ever be atomized back.
      :ok = Postgres.advance(opts, @some, 1)

      # inspect/1, not Atom.to_string/1: the stored key is the readable module
      # name an operator would recognise, and nothing ever converts it back.
      assert [[@some]] = raw(opts, "SELECT subscription FROM dmex_cursors")
    end
  end

  # These two are why nothing outside this module knows how Postgres delivers a
  # wakeup. Before them, DMEx.Log compared the storage module against this
  # one by name and carried a list of Postgrex connection options.
  describe "child_specs/1" do
    test "supplies a notification connection named after the log", %{opts: opts} do
      opts = Map.put(opts, :log, DMEx.Test.Log)

      assert [%{id: id, start: {Postgrex.Notifications, :start_link, [conn_opts]}}] =
               Postgres.child_specs(opts)

      assert id == DMEx.Test.Log.Notifications
      assert conn_opts[:name] == DMEx.Test.Log.Notifications
      assert conn_opts[:auto_reconnect] == true
      assert conn_opts[:database] == "dmex_test"
    end

    test "passes Postgrex only keys it understands", %{opts: opts} do
      # repo.config/0 carries Ecto's own settings — a pool module, a telemetry
      # prefix — and handing those to Postgrex would be a connection that fails
      # to start rather than one that runs without notifications.
      [%{start: {_, _, [conn_opts]}}] =
        Postgres.child_specs(Map.put(opts, :log, DMEx.Test.Log))

      for key <- [:pool, :pool_size, :telemetry_prefix, :otp_app, :log, :scheme, :url] do
        refute Keyword.has_key?(conn_opts, key), "#{inspect(key)} leaked into Postgrex opts"
      end
    end
  end

  describe "listen/1" do
    # The behaviour promises best-effort: polling is what makes dispatch robust,
    # so a missing notifier must be an error value rather than an exit that
    # takes the dispatcher down with it.
    test "reports a notifier that is not running instead of exiting", %{opts: opts} do
      opts = Map.put(opts, :log, DMEx.Test.Log)

      assert {:error, :not_running} = Postgres.listen(opts)
    end
  end

  describe "dead_message/2" do
    test "stores the log row verbatim, plus whose row it is", %{opts: opts} do
      assert :ok = Postgres.dead_message(opts, dead_attrs())

      assert [[subscription, id, type, data, metadata, inserted_at]] =
               raw(opts, """
               SELECT subscription, id, message_type,
                      data::text, metadata::text, inserted_at
                 FROM dmex_dead_messages
               """)

      assert subscription == @some
      assert id == 1042
      assert type == "MyApp.Messages.OrderPlaced"
      assert JSON.decode!(data) == %{"order_id" => 7}
      assert JSON.decode!(metadata) == %{"trace_id" => "abc"}
      assert %DateTime{} = inserted_at
    end

    test "two subscriptions may give up on the same log row", %{opts: opts} do
      assert :ok = Postgres.dead_message(opts, dead_attrs(subscription: @one))
      assert :ok = Postgres.dead_message(opts, dead_attrs(subscription: @two))

      assert count(opts, "dmex_dead_messages") == 2
    end

    test "one subscription cannot record the same message twice", %{opts: opts} do
      assert :ok = Postgres.dead_message(opts, dead_attrs())

      assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
               Postgres.dead_message(opts, dead_attrs())
    end
  end

  describe "dead_messages/2 and delete_dead_message/3" do
    # The point of the table's shape: what comes back is a log row, in exactly
    # the shape read/5 returns and the pipeline consumes.
    test "reads back rows in the same shape read/5 returns", %{opts: opts} do
      :ok = Postgres.dead_message(opts, dead_attrs())

      assert {:ok, [row]} = Postgres.dead_messages(opts, @some)

      assert Map.keys(row) |> Enum.sort() ==
               [:data, :id, :inserted_at, :message_type, :metadata, :previous_id]

      assert row.id == 1042
      assert is_binary(row.data)
      assert is_binary(row.metadata)
    end

    test "returns only the given subscription's messages, oldest first", %{opts: opts} do
      :ok = Postgres.dead_message(opts, dead_attrs(subscription: @one, id: 3))
      :ok = Postgres.dead_message(opts, dead_attrs(subscription: @one, id: 1))
      :ok = Postgres.dead_message(opts, dead_attrs(subscription: @two, id: 2))

      assert {:ok, rows} = Postgres.dead_messages(opts, @one)
      assert Enum.map(rows, & &1.id) == [1, 3]
    end

    test "is empty for a subscription that has given up on nothing", %{opts: opts} do
      assert {:ok, []} = Postgres.dead_messages(opts, @some)
    end

    test "delete removes one message by subscription and log id", %{opts: opts} do
      :ok = Postgres.dead_message(opts, dead_attrs(id: 1))
      :ok = Postgres.dead_message(opts, dead_attrs(id: 2))

      assert :ok = Postgres.delete_dead_message(opts, @some, 1)

      assert {:ok, [row]} = Postgres.dead_messages(opts, @some)
      assert row.id == 2
    end

    test "deleting something absent is fine", %{opts: opts} do
      assert :ok = Postgres.delete_dead_message(opts, @some, 999)
    end
  end

  ## Helpers

  defp append(opts, type, data, metadata \\ %{}) do
    {:ok, [id]} = Postgres.append(opts, [{type, JSON.encode!(data), JSON.encode!(metadata)}])

    {:ok, id}
  end

  defp fetch!(opts, id) do
    {:ok, [row]} = Postgres.read(opts, id - 1, 1, 1_048_576, :all)
    row
  end

  defp raw(%{repo: repo}, sql), do: SQL.query!(repo, sql, []).rows

  # Every row claims the one before it, and the first claims `from`.
  defp chained?(rows, from) do
    rows
    |> Enum.reduce_while(from, fn row, previous ->
      if row.previous_id == previous, do: {:cont, row.id}, else: {:halt, :broken}
    end)
    |> Kernel.!=(:broken)
  end

  # What the byte bound counts: payload plus the type, matching the adapter's
  # `octet_length(data) + octet_length(metadata) + octet_length(message_type)`.
  defp payload_bytes(rows) do
    Enum.reduce(rows, 0, fn row, total ->
      total + byte_size(row.data) + byte_size(row.metadata) + byte_size(row.message_type)
    end)
  end

  defp row_bytes(opts, id) do
    {:ok, [row]} = Postgres.read(opts, id - 1, 1, 10_000_000, :all)

    payload_bytes([row])
  end

  # The read's own SQL, run through EXPLAIN, so the plan shape is asserted
  # rather than assumed. Reaches into the adapter's private query builder
  # deliberately: what is under test is that statement and no restatement of it
  # would prove anything.
  defp explain(%{repo: repo} = opts, cursor, limit, max_bytes, :all) do
    sql = :erlang.apply(Postgres, :__read_sql__, [opts, ""])

    repo
    |> SQL.query!("EXPLAIN " <> sql, [cursor, limit, max_bytes])
    |> Map.fetch!(:rows)
    |> List.flatten()
    |> Enum.join("\n")
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
      metadata: ~s({"trace_id":"abc"}),
      inserted_at: "2026-07-27T10:31:04.221Z"
    }
  end
end
