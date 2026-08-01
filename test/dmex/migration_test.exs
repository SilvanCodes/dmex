defmodule DMEx.MigrationTest do
  use DMEx.Case, async: true, database: true

  alias Ecto.Adapters.SQL

  alias DMEx.Migration

  describe "option validation" do
    test "rejects an unknown table" do
      assert_raise ArgumentError, ~r/:tables must be a non-empty list/, fn ->
        Migration.up(tables: [:log, :nonsense])
      end
    end

    test "rejects an empty table list" do
      assert_raise ArgumentError, ~r/:tables must be a non-empty list/, fn ->
        Migration.up(tables: [])
      end
    end

    test "rejects a non-list" do
      assert_raise ArgumentError, ~r/:tables must be a non-empty list/, fn ->
        Migration.down(tables: :log)
      end
    end
  end

  # The suite's own repos are migrated through DMEx.Migration in test_helper.exs,
  # so introspecting them tests the real DDL rather than a restatement of it.
  describe "dmex_messages" do
    test "id is a bigint from a sequence" do
      # bigint, not int: a log nothing deletes from outgrows 2^31, and every
      # consumer cursor in the network is denominated in this column.
      assert %{"data_type" => "bigint", "column_default" => default} =
               column("dmex_messages", "id")

      assert default =~ "nextval"
    end

    test "payload columns are jsonb and metadata defaults to an empty object" do
      assert %{"data_type" => "jsonb", "is_nullable" => "NO"} = column("dmex_messages", "data")

      assert %{"data_type" => "jsonb", "is_nullable" => "NO", "column_default" => "'{}'::jsonb"} =
               column("dmex_messages", "metadata")
    end

    test "inserted_at carries a time zone" do
      # A plain `timestamp` would come back from Postgrex as %NaiveDateTime{},
      # which renders without the trailing Z and would silently change the wire
      # format without failing anything.
      assert %{"data_type" => "timestamp with time zone"} =
               column("dmex_messages", "inserted_at")
    end

    test "two indexes, one per read shape" do
      # The primary key serves a whole-log read — a bare `id > cursor` range
      # scan — directly.
      #
      # `(message_type, id)` serves a selective one. Leading on `message_type`
      # is what lets the planner seek per selected type and merge, so a consumer
      # selecting a slice of a busy log reads that slice rather than scanning
      # everything and discarding; without it, resolving a filter server-side
      # would save bandwidth and no database work at all.
      #
      # It is not free: appends are serialized per app, so this index's
      # maintenance sits on the one write path that cannot be parallelized away,
      # and every log pays it whether or not anyone selects against it.
      assert indexes("dmex_messages") == [
               "dmex_messages_message_type_id_index",
               "dmex_messages_pkey"
             ]

      assert index_columns("dmex_messages_message_type_id_index") == ["message_type", "id"]
    end

    # `message_type` goes on the wire as an SSE `event:` line, unescaped, because
    # SSE field lines have no escaping. Everything else on the wire is spliced
    # JSON, whose grammar already forbids raw control characters inside strings;
    # this column is not, so the log enforces the charset instead. A row that
    # could split a frame cannot be written in the first place.
    test "message_type is constrained to what an SSE event: line can carry" do
      assert "dmex_messages_type_charset" in constraints("dmex_messages")

      for bad <- ["With\nNewline", "With\rReturn", "With Space", "With:Colon", ""] do
        assert_raise Postgrex.Error, ~r/dmex_messages_type_charset/, fn ->
          insert_type(bad)
        end
      end
    end

    test "the charset is exactly what an Elixir module name uses" do
      assert :ok = insert_type("MyApp.Messages.OrderPlaced")
      assert :ok = insert_type("Elixir.A_b.C9")
    end
  end

  describe "dmex_cursors" do
    test "the declared subscription identifier is the primary key" do
      # The `log/id` pair a handler declares *is* the subscription's
      # identity, so the resulting unique index also enforces "one cursor per
      # subscription" in storage, not only in the process registry — and keying
      # on the declaration rather than the module name is what makes renaming
      # the module free.
      assert primary_key("dmex_cursors") == ["subscription"]
    end

    test "carries a durable position and when it last moved" do
      assert %{"data_type" => "bigint", "is_nullable" => "NO"} =
               column("dmex_cursors", "last_seen")

      assert %{"is_nullable" => "NO"} = column("dmex_cursors", "updated_at")
    end

    # A cursor is a subscription and a position, and nothing else: which host
    # that subscription's log name resolves to is the consumer's runtime
    # configuration, so repointing it is a config change rather than a data
    # migration.
    test "records nothing about the log it read from" do
      assert column_missing?("dmex_cursors", "producer_url")
    end
  end

  describe "dmex_dead_messages" do
    # The table is shared across every subscription, so a row has to say which
    # one owns it — and a subscription reads exactly one log, so its
    # own log ids cannot collide.
    test "is keyed by subscription and the log's own id" do
      assert primary_key("dmex_dead_messages") == ["subscription", "id"]
    end

    # This is the property the whole shape exists for: a dead row *is* a log
    # row, so replaying one needs no reshaping and no second decode path.
    # `inserted_at` is the same column with a different meaning — when the row
    # landed here rather than when the message was appended — so it is asserted
    # separately below.
    test "carries the message log's columns, with the same types" do
      for name <- ~w(id message_type data metadata inserted_at) do
        dead = column("dmex_dead_messages", name)
        log = column("dmex_messages", name)

        assert dead["data_type"] == log["data_type"],
               "#{name}: #{dead["data_type"]} in dead_messages, #{log["data_type"]} in the log"
      end
    end

    # Append time is not recoverable from this table, deliberately: pruning
    # needs to know how long a row has sat here, and a message appended a year
    # ago but abandoned this morning would look ancient by append time.
    test "stamps its own insertion time rather than requiring one" do
      assert %{"column_default" => default} = column("dmex_dead_messages", "inserted_at")

      assert default =~ "now()"
    end

    # A failure is an event, not a property of the message.
    test "stores nothing about the failure" do
      for name <- ~w(reason attempts error stacktrace failed_at) do
        assert column_missing?("dmex_dead_messages", name), "#{name} should not be stored"
      end
    end
  end

  ## Introspection helpers

  defp column(table, name) do
    %{rows: [row], columns: columns} =
      query(
        """
        SELECT data_type, is_nullable, column_default
          FROM information_schema.columns
         WHERE table_name = $1 AND column_name = $2
        """,
        [table, name]
      )

    columns |> Enum.zip(row) |> Map.new()
  end

  defp indexes(table) do
    %{rows: rows} =
      query("SELECT indexname FROM pg_indexes WHERE tablename = $1 ORDER BY indexname", [table])

    List.flatten(rows)
  end

  defp primary_key(table) do
    %{rows: rows} =
      query(
        """
        SELECT a.attname
          FROM pg_index i
          JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
         WHERE i.indrelid = $1::text::regclass AND i.indisprimary
         ORDER BY array_position(i.indkey, a.attnum)
        """,
        [table]
      )

    List.flatten(rows)
  end

  defp column_missing?(table, name) do
    %{rows: rows} =
      query(
        """
        SELECT 1 FROM information_schema.columns
         WHERE table_name = $1 AND column_name = $2
        """,
        [table, name]
      )

    rows == []
  end

  # Column *order* is the whole point for `(message_type, id)` — the same two
  # columns the other way round would not serve a selective read — so this
  # unnests `indkey` with ordinality rather than taking whatever order the
  # catalog join happens to produce.
  defp index_columns(index) do
    %{rows: rows} =
      query(
        """
        SELECT a.attname
          FROM pg_index i
          JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) ON true
          JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
         WHERE i.indexrelid = $1::text::regclass
         ORDER BY k.ord
        """,
        [index]
      )

    List.flatten(rows)
  end

  defp constraints(table) do
    %{rows: rows} =
      query(
        """
        SELECT conname FROM pg_constraint
         WHERE conrelid = $1::text::regclass AND contype = 'c'
        """,
        [table]
      )

    List.flatten(rows)
  end

  defp insert_type(type) do
    query(
      "INSERT INTO dmex_messages (message_type, data) VALUES ($1, '{}'::jsonb)",
      [type]
    )

    :ok
  end

  defp query(sql, params), do: SQL.query!(Repo, sql, params)
end
