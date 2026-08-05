defmodule DMEx.Storage.Postgres.Migration do
  @moduledoc """
  The Postgres DDL behind `DMEx.Storage.Migration`.

  Reached through that module, which picks a migration by the repo's adapter —
  a host application writes `DMEx.Storage.Migration.up()` and never names this
  one.

  `:prefix` here is a Postgres schema, created if absent.
  """

  use Ecto.Migration

  alias DMEx.Storage.Postgres

  @doc false
  @spec up([:log | :subscriptions], String.t() | nil) :: :ok
  def up(tables, prefix) do
    if prefix, do: execute("CREATE SCHEMA IF NOT EXISTS \"#{prefix}\"")

    if :log in tables, do: up_log(prefix)
    if :subscriptions in tables, do: up_subscriptions(prefix)

    :ok
  end

  @doc false
  @spec down([:log | :subscriptions], String.t() | nil) :: :ok
  def down(tables, prefix) do
    if :subscriptions in tables do
      drop_if_exists(table(:dmex_dead_messages, prefix: prefix))
      drop_if_exists(table(:dmex_cursors, prefix: prefix))
    end

    if :log in tables do
      drop_if_exists(table(:dmex_messages, prefix: prefix))
    end

    :ok
  end

  defp up_log(prefix) do
    create_if_not_exists table(:dmex_messages, primary_key: false, prefix: prefix) do
      # bigint, not int: a log nothing deletes from outgrows 2^31, and every
      # consumer cursor in the network is denominated in this column.
      add(:id, :bigserial, primary_key: true)

      # The only classification a row carries, and the only one it ever will: it
      # describes what the message *is*, never who might want to read it. Who
      # wants what is resolved at read time, by the reader. Constrained below,
      # because it goes on the wire as an unescaped SSE `event:` line.
      add(:message_type, :text, null: false)

      # jsonb so operators can query inside payloads directly. The accepted cost
      # is normalization: key order is not preserved and duplicate keys are
      # dropped, so stored bytes are not what the publisher encoded.
      add(:data, :jsonb, null: false)

      # Publisher-owned as stored: nothing DMEx knows is ever written here. A log
      # whose `metadata:` lifts columns merges them on the way out instead, so
      # the reserved names that creates cost this column nothing. Defaulted
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
      # Never an ordering key. `id` is. This column is observability: a log
      # lifts it into delivered metadata with `metadata: [:inserted_at]`, and
      # nothing in DMEx reads it.
      add(:inserted_at, :timestamptz, null: false, default: fragment("clock_timestamp()"))
    end

    # For exactly one query shape — see the moduledoc for when the planner takes
    # it. It is also the reason a whole-log read is a *separate statement* (see
    # `DMEx.Storage.Postgres.read/5`): folding both into one query with a
    # nullable predicate would defeat this index and the primary key at once.
    create_if_not_exists(index(:dmex_messages, [:message_type, :id], prefix: prefix))

    # Written out rather than through `constraint/3`, because Ecto has no
    # `create_if_not_exists` for constraints and Postgres has no
    # `ADD CONSTRAINT IF NOT EXISTS` — and every other statement here is
    # re-runnable, which is worth keeping for the whole migration.
    execute("""
    DO $$ BEGIN
      ALTER TABLE #{qualified(:dmex_messages, prefix)}
        ADD CONSTRAINT dmex_messages_type_charset
        CHECK (message_type ~ '^[A-Za-z0-9_.]+$');
    EXCEPTION WHEN duplicate_object THEN NULL;
    END $$;
    """)

    :ok
  end

  # One renderer for both halves of the adapter, so a prefixed name in DDL and a
  # prefixed name in a query cannot disagree.
  defp qualified(name, prefix), do: Postgres.__table__(name, prefix)

  defp up_subscriptions(prefix) do
    create_if_not_exists table(:dmex_cursors, primary_key: false, prefix: prefix) do
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

    # **Structurally a message log.** Everything below `subscription` is
    # `dmex_messages` — the same column names, types and meanings — so a dead
    # row *is* a log row: replaying one needs no reshaping and no second decode
    # path. Equal as *values*, not as bytes: the payload was decoded out of a
    # frame and re-encoded, the same caveat that already applies to anything
    # reading `data` back out of the log.
    #
    # Deliberately **not** here: the exception and the attempt count. A failure
    # is an event, not a property of the message, so it goes to logs and
    # telemetry — persisting it would stop a row from simply being a message.
    create_if_not_exists table(:dmex_dead_messages, primary_key: false, prefix: prefix) do
      # The table is shared across every subscription in the application, so a
      # row has to say which owns it, by the same identifier `dmex_cursors` is
      # keyed on. That plus the log id is the identity: a subscription reads
      # exactly one producer's log, so its ids cannot collide.
      add(:subscription, :text, primary_key: true)

      # From here down, this is dmex_messages.
      add(:id, :bigint, primary_key: true)
      add(:message_type, :text, null: false)
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

    :ok
  end
end
