defmodule DMEx.Storage.SQLite.Migration do
  @moduledoc """
  The SQLite DDL behind `DMEx.Storage.Migration`.

  Reached through that module, which runs the DDL the configured storage adapter
  names — `DMEx.Storage.SQLite.migration/0` is what points here. A host
  application writes `DMEx.Storage.Migration.up()` and never names this one.

  **Raw SQL rather than the migration DSL**, because the CHECK constraints have
  to be inline: SQLite's `ALTER TABLE` cannot add one, and ecto_sqlite3 raises
  on `constraint/3` for exactly that reason. Written out, it is also honest —
  `add(:id, :bigserial, primary_key: true)` renders `INTEGER PRIMARY KEY
  AUTOINCREMENT` through two indirections nobody reading the migration can see.

  Every statement is `IF NOT EXISTS`, so the whole migration is re-runnable.

  ## AUTOINCREMENT, not a bare rowid

  A plain `INTEGER PRIMARY KEY` may **reuse** the ids of deleted rows, and a
  reused id is the one failure a consumer cannot survive: every message under it
  is behind cursors that have already passed, so they are skipped permanently and
  silently. DMEx never deletes from `dmex_messages`, but an operator pruning the
  tail would. `AUTOINCREMENT` costs one `sqlite_sequence` update per append and
  makes that unrepresentable.

  ## Why `data` and `metadata` are TEXT

  Not a compromise for a database without `jsonb` — a better fit. The bytes
  stored are the publisher's bytes, so key order and duplicate keys survive
  where `jsonb` normalises both away, and `c:DMEx.Storage.read/5`'s
  "return them as text" contract needs no cast at all. `json_valid/1` restores
  the one thing `jsonb` did give: nothing that is not JSON can land in the
  column, whatever wrote it. What is given up is indexed access into payloads.

  ## Why `inserted_at` is fixed-width text

  `strftime('%f')` is documented as `SS.SSS` — always three digits — so every
  value is exactly 24 bytes and lexicographic comparison *is* chronological
  comparison. The default is evaluated during the INSERT, inside the write
  transaction that draws the rowid, so the column ascends with `id` — SQLite has
  no transaction-start clock to reach for by mistake.

  All three tables' defaults render `DMEx.Storage.SQLite.__now__/0` rather than
  spelling the expression out, because the adapter is a fourth writer of the
  format: `advance_cursor/3` stamps `dmex_cursors.updated_at` itself. One statement, so
  the invariant cannot hold for one table and not another.

  ## The `type` CHECK, in GLOB

  SQLite has no regex operator, so Postgres's `~ '^[A-Za-z0-9_.]+$'` becomes
  "contains no character outside the set", plus a non-empty test the negated glob
  does not imply. It is load-bearing twice over: for the unescaped SSE `event:`
  line, and because it guarantees no comma appears in a type — which is what
  makes `DMEx.Storage.SQLite`'s `group_concat` split lossless.
  """

  use Ecto.Migration

  alias DMEx.Storage.Migration
  alias DMEx.Storage.SQLite

  # From the behaviour, not restated: see `DMEx.Storage.__table__/1`. The same
  # three names `DMEx.Storage.SQLite` writes its queries against.
  @messages DMEx.Storage.__table__(:messages)
  @cursors DMEx.Storage.__table__(:cursors)
  @dead_messages DMEx.Storage.__table__(:dead_messages)

  @now SQLite.__now__()

  @doc false
  @spec up([Migration.table()], String.t() | nil) :: :ok
  def up(tables, prefix) do
    SQLite.__refuse_prefix__(prefix)

    if :messages in tables, do: up_messages()
    if :cursors in tables, do: up_cursors()
    if :dead_messages in tables, do: up_dead_messages()

    :ok
  end

  @doc false
  @spec down([Migration.table()], String.t() | nil) :: :ok
  def down(tables, prefix) do
    SQLite.__refuse_prefix__(prefix)

    if :dead_messages in tables, do: execute("DROP TABLE IF EXISTS #{@dead_messages}")
    if :cursors in tables, do: execute("DROP TABLE IF EXISTS #{@cursors}")
    if :messages in tables, do: execute("DROP TABLE IF EXISTS #{@messages}")

    :ok
  end

  # The `type` CHECK is a backstop and deliberately looser than the real rule —
  # `DMEx.Storage.Postgres.Migration` says why, and this is the same rule as a
  # negated GLOB, SQLite having no regex.
  defp up_messages do
    execute("""
    CREATE TABLE IF NOT EXISTS #{@messages} (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      type         TEXT NOT NULL CHECK (type <> '' AND type NOT GLOB '*[^A-Za-z0-9_.]*'),
      data         TEXT NOT NULL CHECK (json_valid(data)),
      metadata     TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(metadata)),
      inserted_at  TEXT NOT NULL DEFAULT (#{@now})
    )
    """)

    # The same one the Postgres migration creates, for the same query — see
    # `DMEx.Storage.Postgres.Migration` for when it earns its per-append cost.
    execute("""
    CREATE INDEX IF NOT EXISTS #{@messages}_type_id_index
        ON #{@messages} (type, id)
    """)
  end

  defp up_cursors do
    # `NOT NULL` spelled out: SQLite lets a rowid table's declared PRIMARY KEY
    # hold NULL, for backwards compatibility with a long-ago bug.
    execute("""
    CREATE TABLE IF NOT EXISTS #{@cursors} (
      subscription TEXT NOT NULL PRIMARY KEY,
      last_seen    INTEGER NOT NULL,
      updated_at   TEXT NOT NULL DEFAULT (#{@now})
    )
    """)
  end

  # From `id` down this is `dmex_messages`, so a dead row *is* a log row and
  # replay needs no reshaping. `inserted_at` is the exception: here it records
  # when the row landed, not when the message was appended.
  defp up_dead_messages do
    execute("""
    CREATE TABLE IF NOT EXISTS #{@dead_messages} (
      subscription TEXT NOT NULL,
      id           INTEGER NOT NULL,
      type         TEXT NOT NULL,
      data         TEXT NOT NULL CHECK (json_valid(data)),
      metadata     TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(metadata)),
      inserted_at  TEXT NOT NULL DEFAULT (#{@now}),
      PRIMARY KEY (subscription, id)
    )
    """)
  end
end
