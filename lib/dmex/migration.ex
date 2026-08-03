defmodule DMEx.Migration do
  @moduledoc """
  Creates the tables DMEx needs, from a migration in the host application.

  Every function here runs inside the host's `Ecto.Migration`, using the
  migration DSL directly. DMEx lives in your database alongside your own tables,
  so its schema is yours to migrate:

      defmodule MyApp.Repo.Migrations.AddDMEx do
        use Ecto.Migration

        def up, do: DMEx.Migration.up()
        def down, do: DMEx.Migration.down()
      end

  An application usually appends, subscribes, or both, and only needs the tables
  for the roles it plays:

      DMEx.Migration.up(tables: [:log])            # dmex_messages
      DMEx.Migration.up(tables: [:subscriptions])  # dmex_cursors, dmex_dead_messages
      DMEx.Migration.up()                          # both, the default

  ## Options

    * `:tables` — `[:log, :subscriptions]` (default both)
    * `:prefix` — a Postgres schema to create the tables in, created if absent.

  ## The two indexes

  **The primary key on `id`** serves every whole-log read directly: those are
  `id > cursor` range scans and nothing else. **`(message_type, id)`** serves a
  selective subscription's `WHERE id > $1 AND message_type = ANY($2)`, and the
  planner takes it when the selection is rare — measured on a 500k-row skewed
  log, at 0.1% of rows yes, at 1% and above it prefers the primary key, which is
  genuinely cheaper there. It costs one index entry per append, paid whether or
  not anyone selects against it, because the alternative is that server-side
  selection saves bandwidth and no database work at all.

  ## The one CHECK constraint

  `message_type` is restricted to `[A-Za-z0-9_.]+` because it goes on the wire as
  an SSE `event:` line and SSE field lines have no escaping. Everything else on
  the wire is spliced JSON, whose grammar already forbids raw control characters
  inside strings. `use DMEx.Message` enforces the same charset on a declared
  `type:` at compile time; this is the same rule for every other writer.

  A database constraint rather than a check in the append path, because it has
  to hold for every writer of the table, including a backfill or a psql session.
  """

  use Ecto.Migration

  @tables [:log, :subscriptions]

  @doc "Creates the DMEx tables. See the module documentation for options."
  @spec up(keyword()) :: :ok
  def up(opts \\ []) when is_list(opts) do
    %{tables: tables, prefix: prefix} = validate!(opts)

    if prefix, do: execute("CREATE SCHEMA IF NOT EXISTS \"#{prefix}\"")

    if :log in tables, do: up_log(prefix)
    if :subscriptions in tables, do: up_subscriptions(prefix)

    :ok
  end

  @doc """
  Drops the DMEx tables. A `:prefix` schema itself is left in place.

  Deliberately: `up/1` creates it with `IF NOT EXISTS`, so it may have existed
  first and may hold tables that are not DMEx's. Dropping a schema DMEx does not
  own is not something an undo of `up/1` gets to do.

  Note what this destroys: `dmex_messages` is an append-only log nothing else
  keeps a copy of, and dropping it is not recoverable from any consumer.
  """
  @spec down(keyword()) :: :ok
  def down(opts \\ []) when is_list(opts) do
    %{tables: tables, prefix: prefix} = validate!(opts)

    if :subscriptions in tables do
      drop_if_exists(table(:dmex_dead_messages, prefix: prefix))
      drop_if_exists(table(:dmex_cursors, prefix: prefix))
    end

    if :log in tables do
      drop_if_exists(table(:dmex_messages, prefix: prefix))
    end

    :ok
  end

  defp validate!(opts) do
    tables = Keyword.get(opts, :tables, @tables)

    unless is_list(tables) and tables != [] and Enum.all?(tables, &(&1 in @tables)) do
      raise ArgumentError,
            ":tables must be a non-empty list drawn from #{inspect(@tables)}, " <>
              "got: #{inspect(tables)}"
    end

    %{tables: tables, prefix: Keyword.get(opts, :prefix)}
  end

  ## The DDL

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

      # Entirely publisher-owned, so there are no reserved key names to avoid.
      # Defaulted rather than nullable so no read path has to distinguish "no
      # metadata" from NULL.
      add(:metadata, :jsonb, null: false, default: fragment("'{}'::jsonb"))

      # timestamptz specifically, and for observability only — never ordering,
      # which is what `id` is for. Postgrex returns %DateTime{} for this type,
      # which is what renders the trailing Z on the wire; a plain `timestamp`
      # comes back as %NaiveDateTime{} and silently drops it.
      add(:inserted_at, :timestamptz, null: false, default: fragment("now()"))
    end

    # The one secondary index, for exactly one query shape — see the moduledoc
    # for when the planner takes it. It is also the reason a whole-log read is a
    # *separate statement* (see `DMEx.Storage.Postgres.read/5`): folding both
    # into one query with a nullable predicate would defeat this index and the
    # primary key at once.
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

  defp qualified(name, nil), do: ~s("#{name}")
  defp qualified(name, prefix), do: ~s("#{prefix}"."#{name}")

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
