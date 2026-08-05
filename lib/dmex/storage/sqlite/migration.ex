defmodule DMEx.Storage.SQLite.Migration do
  @moduledoc """
  The SQLite DDL behind `DMEx.Storage.Migration`.

  Reached through that module, which picks a migration by the repo's adapter —
  a host application writes `DMEx.Storage.Migration.up()` and never names this
  one.

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

  ## The `message_type` CHECK, in GLOB

  SQLite has no regex operator, so Postgres's `~ '^[A-Za-z0-9_.]+$'` becomes
  "contains no character outside the set", plus a non-empty test the negated glob
  does not imply. It is load-bearing twice over: for the unescaped SSE `event:`
  line, and because it guarantees no comma appears in a type — which is what
  makes `DMEx.Storage.SQLite`'s `group_concat` split lossless.

  ## No `:prefix`

  SQLite has no schemas. A `:prefix` raises here rather than being ignored,
  because tables silently landing somewhere other than where an operator wrote
  them is worse than a failed migration.
  """

  use Ecto.Migration

  @doc false
  @spec up([:log | :subscriptions], String.t() | nil) :: :ok
  def up(tables, prefix) do
    refuse_prefix!(prefix)

    if :log in tables, do: up_log()
    if :subscriptions in tables, do: up_subscriptions()

    :ok
  end

  @doc false
  @spec down([:log | :subscriptions], String.t() | nil) :: :ok
  def down(tables, prefix) do
    refuse_prefix!(prefix)

    if :subscriptions in tables do
      execute("DROP TABLE IF EXISTS dmex_dead_messages")
      execute("DROP TABLE IF EXISTS dmex_cursors")
    end

    if :log in tables do
      execute("DROP TABLE IF EXISTS dmex_messages")
    end

    :ok
  end

  defp refuse_prefix!(nil), do: :ok

  defp refuse_prefix!(prefix) do
    raise ArgumentError,
          "SQLite has no schemas, so DMEx cannot create its tables in " <>
            "#{inspect(prefix)}. Drop `:prefix`, or give this database a file of " <>
            "its own — which is what a schema would have bought."
  end

  ## The DDL

  defp up_log do
    execute("""
    CREATE TABLE IF NOT EXISTS dmex_messages (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      message_type TEXT NOT NULL
                   CHECK (message_type <> '' AND message_type NOT GLOB '*[^A-Za-z0-9_.]*'),
      data         TEXT NOT NULL CHECK (json_valid(data)),
      metadata     TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(metadata)),
      inserted_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    )
    """)

    # The same one the Postgres migration creates, for the same query — see
    # `DMEx.Storage.Migration` for when it earns its per-append cost.
    execute("""
    CREATE INDEX IF NOT EXISTS dmex_messages_message_type_id_index
        ON dmex_messages (message_type, id)
    """)

    :ok
  end

  defp up_subscriptions do
    # `NOT NULL` spelled out: SQLite lets a rowid table's declared PRIMARY KEY
    # hold NULL, for backwards compatibility with a long-ago bug.
    execute("""
    CREATE TABLE IF NOT EXISTS dmex_cursors (
      subscription TEXT NOT NULL PRIMARY KEY,
      last_seen    INTEGER NOT NULL,
      updated_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    )
    """)

    # From `id` down this is `dmex_messages`, so a dead row *is* a log row and
    # replay needs no reshaping. `inserted_at` is the exception: here it records
    # when the row landed, not when the message was appended.
    execute("""
    CREATE TABLE IF NOT EXISTS dmex_dead_messages (
      subscription TEXT NOT NULL,
      id           INTEGER NOT NULL,
      message_type TEXT NOT NULL,
      data         TEXT NOT NULL CHECK (json_valid(data)),
      metadata     TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(metadata)),
      inserted_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      PRIMARY KEY (subscription, id)
    )
    """)

    :ok
  end
end
