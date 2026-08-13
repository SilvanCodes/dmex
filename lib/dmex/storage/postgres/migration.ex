defmodule DMEx.Storage.Postgres.Migration do
  @moduledoc """
  The Postgres DDL behind `DMEx.Storage.Migration`.

  Reached through that module, which runs the DDL the configured storage adapter
  names — `DMEx.Storage.Postgres.migration/0` is what points here. A host
  application writes `DMEx.Storage.Migration.up()` and never names this one.
  """

  use Ecto.Migration

  alias DMEx.Storage.Migration
  alias DMEx.Storage.Postgres

  # From the behaviour, not restated: see `DMEx.Storage.__table__/1`.
  @messages DMEx.Storage.__table__(:messages)
  @cursors DMEx.Storage.__table__(:cursors)
  @dead_messages DMEx.Storage.__table__(:dead_messages)

  @doc false
  @spec up([Migration.table()], String.t() | nil) :: :ok
  def up(tables, prefix) do
    if prefix, do: execute("CREATE SCHEMA IF NOT EXISTS \"#{prefix}\"")

    if :messages in tables, do: up_messages(prefix)
    if :cursors in tables, do: up_cursors(prefix)
    if :dead_messages in tables, do: up_dead_messages(prefix)

    :ok
  end

  @doc false
  @spec down([Migration.table()], String.t() | nil) :: :ok
  def down(tables, prefix) do
    if :dead_messages in tables, do: drop_if_exists(table(@dead_messages, prefix: prefix))
    if :cursors in tables, do: drop_if_exists(table(@cursors, prefix: prefix))
    if :messages in tables, do: drop_if_exists(table(@messages, prefix: prefix))

    :ok
  end

  defp up_messages(prefix) do
    create_if_not_exists table(@messages, primary_key: false, prefix: prefix) do
      # bigint, not int: a log nothing deletes from outgrows 2^31, and every
      # consumer cursor in the network is denominated in this column.
      add(:id, :bigserial, primary_key: true)

      # The only classification a row carries, and the only one it ever will: it
      # describes what the message *is*, never who might want to read it. Who
      # wants what is resolved at read time, by the reader. Constrained below,
      # because it goes on the wire as an unescaped SSE `event:` line.
      add(:type, :text, null: false)

      # jsonb so operators can query inside payloads directly. The accepted cost
      # is normalization: key order is not preserved and duplicate keys are
      # dropped, so stored bytes are not what the publisher encoded.
      add(:data, :jsonb, null: false)

      # Publisher-owned: nothing DMEx knows is ever written here. Defaulted
      # rather than nullable so no read path has to distinguish "no metadata"
      # from NULL.
      add(:metadata, :jsonb, null: false, default: fragment("'{}'::jsonb"))

      # timestamptz specifically. Postgrex returns %DateTime{} for this type,
      # which is what renders the trailing Z on the wire; a plain `timestamp`
      # comes back as %NaiveDateTime{} and silently drops it.
      #
      # `clock_timestamp()`, not `now()`: `now()` is the *transaction's* start
      # time, frozen before `pg_advisory_xact_lock` was ever acquired — and
      # arbitrarily long before, when a caller wraps `append/2` in a transaction
      # of its own. `clock_timestamp()` is evaluated during the INSERT, inside
      # the lock's serialised region, by the same statement that draws `nextval`,
      # so the column ascends with `id` rather than with transaction starts.
      #
      # Never an ordering key. `id` is. This column is observability: nothing in
      # DMEx reads it and it never reaches a handler.
      add(:inserted_at, :timestamptz, null: false, default: fragment("clock_timestamp()"))
    end

    # For exactly one query shape — `DMEx.Storage.Migration` covers when the
    # planner takes it, and `c:DMEx.Storage.read/5` why that makes a selective
    # read a *separate statement* rather than one nullable predicate.
    #
    # Measured once, on a 500k-row skewed log (one type at 99%, one at 0.9%, one
    # at 0.1%): the 0.1% type gets `Index Scan using dmex_messages_type_id_index`,
    # the 0.9% type and larger get `Index Scan using dmex_messages_pkey` and
    # filter. Exactly the crossover the two-index design assumes, and the reason
    # `type = ANY($1)` is left plain for the planner to choose. A hand-written
    # per-type `LATERAL` seek — the usual loose-index-scan workaround — measured
    # *worse* everywhere except the most extreme selectivity, because each branch
    # fetches a whole `LIMIT` per type and discards the surplus.
    create_if_not_exists(index(@messages, [:type, :id], prefix: prefix))

    # Written out rather than through `constraint/3`, because Ecto has no
    # `create_if_not_exists` for constraints and Postgres has no
    # `ADD CONSTRAINT IF NOT EXISTS` — and every other statement here is
    # re-runnable, which is worth keeping for the whole migration.
    execute("""
    DO $$ BEGIN
      ALTER TABLE #{Postgres.__table__(@messages, prefix)}
        ADD CONSTRAINT #{@messages}_type_charset
        CHECK (type ~ '^[A-Za-z0-9_.]+$');
    EXCEPTION WHEN duplicate_object THEN NULL;
    END $$;
    """)
  end

  defp up_cursors(prefix) do
    create_if_not_exists table(@cursors, primary_key: false, prefix: prefix) do
      # The `log/id` pair a handler declares, as the key rather than a surrogate
      # id: the resulting unique index enforces "one cursor per subscription" in
      # storage, not only in the process registry. Keying on the declaration
      # rather than the module name is what makes renaming that module free.
      #
      # Both halves are `[a-z][a-z0-9_]*`, so "/" separates them unambiguously
      # and two distinct pairs can never render to one string.
      add(:subscription, :text, primary_key: true)
      add(:last_seen, :bigint, null: false)
      add(:updated_at, :timestamptz, null: false, default: fragment("now()"))
    end
  end

  # **Structurally a message log.** Everything below `subscription` is
  # `dmex_messages` — the same column names, types and meanings — so a dead
  # row *is* a log row: replaying one needs no reshaping and no second decode
  # path. Equal as *values*, not as bytes: `data` carries the jsonb caveat
  # above.
  #
  # Deliberately **not** here: the exception and the attempt count. A failure
  # is an event, not a property of the message, so it goes to logs and
  # telemetry — persisting it would stop a row from simply being a message.
  defp up_dead_messages(prefix) do
    create_if_not_exists table(@dead_messages, primary_key: false, prefix: prefix) do
      # The table is shared across every subscription in the application, so a
      # row has to say which owns it, by the same identifier `dmex_cursors` is
      # keyed on. That plus the log id is the identity: a subscription reads
      # exactly one producer's log, so its ids cannot collide.
      add(:subscription, :text, primary_key: true)

      # From here down, this is dmex_messages.
      add(:id, :bigint, primary_key: true)
      add(:type, :text, null: false)
      add(:data, :jsonb, null: false)
      add(:metadata, :jsonb, null: false, default: fragment("'{}'::jsonb"))

      # The one column that does *not* mean what it means in the log: when this
      # row landed here, not when the message was appended.
      #
      # The producer's append time is genuinely lost, and that is the trade:
      # this table grows forever, and pruning it needs an axis answering "how
      # long has this been sitting here" — which append time cannot, since a
      # message appended a year ago and abandoned this morning would look
      # ancient. Nothing downstream misses it: a handler is given its struct and
      # the publisher's metadata, never the row's timestamps.
      add(:inserted_at, :timestamptz, null: false, default: fragment("now()"))
    end
  end
end
