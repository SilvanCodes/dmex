defmodule DMEx.Storage.SQLiteMigrationTest do
  # The tables come from test_helper.exs, which migrates this repo through
  # `DMEx.Storage.Migration` — the same path a host application takes, and the
  # thing under test here.
  use DMEx.Case, async: false, database: true, repo: DMEx.Test.SQLiteRepo

  alias DMEx.Storage.SQLite
  alias DMEx.Storage.SQLite.Migration
  alias Ecto.Adapters.SQL

  @some "orders/some"

  # Every column stamped with the adapter's timestamp expression, and the two
  # writers of each.
  @timestamps [
    {"dmex_messages", "inserted_at"},
    {"dmex_cursors", "updated_at"},
    {"dmex_dead_messages", "inserted_at"}
  ]

  describe "dmex_messages" do
    test "ids are AUTOINCREMENT, so a deleted tail cannot be reissued" do
      {:ok, _} = insert("A", ~s({"n":1}))
      [[id]] = query("SELECT max(id) FROM dmex_messages")

      # SQLite tracks a high-water mark here only for a table declared
      # AUTOINCREMENT — the observable proof, rather than a match on DDL text.
      assert [["dmex_messages"]] =
               query("SELECT name FROM sqlite_sequence WHERE name = 'dmex_messages'")

      {:ok, _} = SQL.query(Repo, "DELETE FROM dmex_messages WHERE id = ?1", [id])
      {:ok, _} = insert("A", ~s({"n":2}))

      assert [[next]] = query("SELECT max(id) FROM dmex_messages")
      assert next > id
    end

    test "columns are the types the read path expects" do
      columns = table_info("dmex_messages")

      assert columns["data"].type == "TEXT"
      assert columns["metadata"].type == "TEXT"
      assert columns["inserted_at"].type == "TEXT"
      assert columns["type"].notnull == 1
      assert columns["metadata"].dflt_value == "'{}'"
      assert columns["inserted_at"].dflt_value =~ "strftime"
    end

    test "carries the selective read's index, on the columns that query needs" do
      indexes = index_columns("dmex_messages")

      assert indexes["dmex_messages_type_id_index"] == ["type", "id"]
      assert indexes["dmex_messages_inserted_at_id_index"] == nil
    end
  end

  describe "the type CHECK" do
    # It goes on the wire as an unescaped SSE `event:` line, and the comma case
    # is load-bearing a second time: `appended_since/2` splits `group_concat` on
    # one, which is only lossless because no type can contain one.
    test "rejects anything outside [A-Za-z0-9_.]" do
      for type <- ["has space", "has,comma", "has\nnewline", "has-dash", ""] do
        assert {:error, _} = insert(type, ~s({"n":1})), "#{inspect(type)} was accepted"
      end
    end

    test "accepts a fully qualified module name" do
      assert {:ok, _} = insert("MyApp.Messages.OrderPlaced", ~s({"n":1}))
    end
  end

  describe "the json_valid CHECKs" do
    # What `jsonb` gave for free on Postgres, and for the same reason: it has to
    # hold for every writer of the table, including a sqlite3 shell.
    test "reject a payload that is not JSON" do
      assert {:error, _} = insert("A", "not json")
      assert {:error, _} = SQL.query(Repo, insert_sql(), ["A", ~s({"n":1}), "nope"])
    end
  end

  describe "dmex_message_entities" do
    test "the key is the whole table and the table is WITHOUT ROWID" do
      [[sql]] =
        query("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'dmex_message_entities'")

      # `WITHOUT ROWID` because this is an index and nothing else: a rowid table
      # would store every pair twice, in the hidden heap and in the index
      # enforcing the key.
      assert sql =~ "WITHOUT ROWID"
      assert sql =~ "PRIMARY KEY (entity, id)"
    end

    test "a rowid really is absent, not merely undeclared" do
      # The observable proof rather than a match on DDL text: a WITHOUT ROWID
      # table has no rowid column to select.
      assert catch_error(query("SELECT rowid FROM dmex_message_entities"))
    end

    test "both columns are required" do
      # Spelled out in the DDL rather than implied by the key, because a rowid
      # table lets a declared PRIMARY KEY hold NULL — see the module docs.
      for column <- ~w(entity id) do
        assert [[1]] =
                 query(
                   "SELECT \"notnull\" FROM pragma_table_info('dmex_message_entities') " <>
                     "WHERE name = '#{column}'"
                 )
      end
    end
  end

  describe "dmex_cursors and dmex_dead_messages" do
    test "the declared subscription identifier is the primary key" do
      assert %{notnull: 1, pk: 1} = table_info("dmex_cursors")["subscription"]
    end

    test "a dead message is keyed by subscription and the log's own id" do
      columns = table_info("dmex_dead_messages")

      assert columns["subscription"].pk == 1
      assert columns["id"].pk == 2

      # From `id` down this is dmex_messages, so replay needs no reshaping.
      assert columns["data"].type == "TEXT"
      assert columns["metadata"].dflt_value == "'{}'"
    end
  end

  # Fixed width is what makes byte comparison equal chronological comparison,
  # which is the only thing making these columns orderable at all — SQLite has no
  # timestamp type. See `DMEx.Storage.SQLite.Migration`. All three tables, because the
  # format is one invariant and holding it for `dmex_messages` alone would be an
  # accident rather than a property.
  describe "the stored timestamp format" do
    test "every default renders the adapter's one expression rather than copying it" do
      for {table, column} <- @timestamps do
        default = table_info(table)[column].dflt_value

        assert String.contains?(default, SQLite.__now__()),
               "#{table}.#{column} defaults to #{inspect(default)}, which is not " <>
                 "DMEx.Storage.SQLite.__now__/0"
      end
    end

    test "every stamped value is 24 bytes of millisecond ISO-8601" do
      {:ok, _} = insert("A", ~s({"n":1}))

      # Not a raw INSERT: `dmex_cursors.updated_at` has a second writer, and it
      # is the adapter rather than the column default.
      :ok = SQLite.advance_cursor(@some, 7, %{repo: Repo})

      {:ok, _} =
        SQL.query(
          Repo,
          """
          INSERT INTO dmex_dead_messages (subscription, id, type, data, metadata)
               VALUES (?1, ?2, ?3, ?4, ?5)
          """,
          [@some, 7, "A", ~s({"n":1}), "{}"]
        )

      for {table, column} <- @timestamps do
        assert [[at]] = query("SELECT #{column} FROM #{table} LIMIT 1")

        assert byte_size(at) == 24, "#{table}.#{column} stamped #{inspect(at)}, not 24 bytes"
        assert {:ok, _datetime, 0} = DateTime.from_iso8601(at)
      end
    end
  end

  describe ":prefix" do
    test "is refused rather than ignored" do
      # SQLite has no schemas. Silently dropping it would put the tables
      # somewhere other than where the operator wrote them.
      assert_raise ArgumentError, ~r/SQLite has no schemas/, fn ->
        Migration.up([:messages], "reporting")
      end

      assert_raise ArgumentError, ~r/SQLite has no schemas/, fn ->
        Migration.down([:messages], "reporting")
      end
    end
  end

  ## Helpers

  defp insert(type, data) do
    SQL.query(Repo, insert_sql(), [type, data, "{}"])
  end

  defp insert_sql do
    "INSERT INTO dmex_messages (type, data, metadata) VALUES (?1, ?2, ?3)"
  end

  defp query(sql, params \\ []) do
    {:ok, %{rows: rows}} = SQL.query(Repo, sql, params)

    rows
  end

  defp table_info(table) do
    "SELECT name, type, `notnull`, dflt_value, pk FROM pragma_table_info(?1)"
    |> query([table])
    |> Map.new(fn [name, type, notnull, default, pk] ->
      {name, %{type: type, notnull: notnull, dflt_value: default, pk: pk}}
    end)
  end

  defp index_columns(table) do
    "SELECT name FROM pragma_index_list(?1) WHERE origin = 'c'"
    |> query([table])
    |> Map.new(fn [index] ->
      columns =
        "SELECT name FROM pragma_index_info(?1) ORDER BY seqno"
        |> query([index])
        |> Enum.map(&hd/1)

      {index, columns}
    end)
  end
end
