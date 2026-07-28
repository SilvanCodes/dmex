defmodule DMEx.MigrationTest do
  use DMEx.Case, async: true, database: true

  alias Ecto.Adapters.SQL

  alias DMEx.Migration

  describe "option validation" do
    test "rejects an unknown table" do
      assert_raise ArgumentError, ~r/:tables must be a non-empty list/, fn ->
        Migration.up(tables: [:producer, :nonsense])
      end
    end

    test "rejects an empty table list" do
      assert_raise ArgumentError, ~r/:tables must be a non-empty list/, fn ->
        Migration.up(tables: [])
      end
    end

    test "rejects a non-list" do
      assert_raise ArgumentError, ~r/:tables must be a non-empty list/, fn ->
        Migration.down(tables: :producer)
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

    test "the primary key is the only index" do
      # Every producer read is a bare `id > cursor` range scan, which the
      # primary key serves directly. An index on message_type would exist purely
      # for ad-hoc operator queries, and it is not free: appends are serialized
      # per app, so its maintenance sits on the one write path that cannot be
      # parallelized away.
      assert indexes("dmex_messages") == ["dmex_messages_pkey"]
    end
  end

  describe "dmex_cursors" do
    test "the declared subscription identifier is the primary key" do
      # The `producer/id` pair a handler declares *is* the subscription's
      # identity, so the resulting unique index also enforces "one cursor per
      # subscription" in storage, not only in the process registry — and keying
      # on the declaration rather than the module name is what makes renaming
      # the module free.
      assert primary_key("dmex_cursors") == ["subscription"]
    end

    test "carries a durable position and the producer it last read from" do
      assert %{"data_type" => "bigint", "is_nullable" => "NO"} =
               column("dmex_cursors", "last_seen")

      assert %{"data_type" => "text", "is_nullable" => "NO"} =
               column("dmex_cursors", "producer_url")
    end
  end

  describe "dmex_dead_messages" do
    # The table is shared across every subscription, so a row has to say which
    # one owns it — and a subscription reads exactly one producer's log, so its
    # own log ids cannot collide.
    test "is keyed by subscription and the producer's own log id" do
      assert primary_key("dmex_dead_messages") == ["subscription", "id"]
    end

    # This is the property the whole shape exists for: a dead row *is* a log
    # row, so replaying one needs no reshaping and no second decode path.
    test "carries the message log's columns, with the same types" do
      for name <- ~w(id message_type data metadata inserted_at) do
        dead = column("dmex_dead_messages", name)
        log = column("dmex_messages", name)

        assert dead["data_type"] == log["data_type"],
               "#{name}: #{dead["data_type"]} in dead_messages, #{log["data_type"]} in the log"
      end
    end

    test "records where the message came from, since messages carry no origin" do
      assert %{"data_type" => "text", "is_nullable" => "NO"} =
               column("dmex_dead_messages", "producer_url")
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

  defp unique_index_columns(index) do
    %{rows: rows} =
      query(
        """
        SELECT a.attname
          FROM pg_index i
          JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
         WHERE i.indexrelid = $1::text::regclass AND i.indisunique
         ORDER BY array_position(i.indkey, a.attnum)
        """,
        [index]
      )

    List.flatten(rows)
  end

  defp query(sql, params), do: SQL.query!(Repo, sql, params)
end
