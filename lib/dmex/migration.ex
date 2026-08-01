defmodule DMEx.Migration do
  @moduledoc """
  Creates the tables DMEx needs, from a migration in the host application.

  Every function here runs inside the host's `Ecto.Migration`, in the migration
  process, and uses the migration DSL directly.

  DMEx lives in your database, alongside your own tables, so its schema is
  yours to migrate:

      defmodule MyApp.Repo.Migrations.AddDMEx do
        use Ecto.Migration

        def up, do: DMEx.Migration.up()
        def down, do: DMEx.Migration.down()
      end

  ## Which tables

  An application usually appends, subscribes, or both, and it only needs the
  tables for the roles it plays:

      DMEx.Migration.up(tables: [:log])            # dmex_messages
      DMEx.Migration.up(tables: [:subscriptions])  # dmex_cursors, dmex_dead_messages
      DMEx.Migration.up()                          # both, the default

  ## Options

    * `:tables` — `[:log, :subscriptions]` (default both)
    * `:prefix` — a Postgres schema to create the tables in. Also creates the
      schema itself if it does not exist.

  ## The two indexes, and why each exists

  **The primary key on `id`** serves every whole-log read directly: those are
  `id > cursor` range scans and nothing else.

  **`(message_type, id)`** serves a selective subscription's
  `WHERE id > $1 AND message_type = ANY($2) ORDER BY id`, and the planner takes
  it **when the selection is rare**. Measured on a 500k-row skewed log: a type
  making up 0.1% of rows uses the index; at 1% and above Postgres prefers
  walking the primary key in id order and filtering, which is genuinely the
  cheaper plan — that scan stops as soon as it has a batch, so the more of a log
  a subscription selects the less it examines per row returned.

  That is the behaviour you want rather than a shortfall. The index earns its
  keep exactly where a scan would walk thousands of rows to return one, and
  stays out of the way where it would be the slower plan.

  It is not free. Appends are serialized per app, so index maintenance sits on
  the one write path that cannot be parallelized away. The cost is one index
  entry per append, paid by every producer whether or not anyone selects
  against it; it is accepted because the alternative is that server-side
  selection saves bandwidth and no database work at all.

  ## The one CHECK constraint

  `message_type` is restricted to `[A-Za-z0-9_.]+`. That column goes on the wire
  as an SSE `event:` line, and SSE field lines have no escaping — a CR or LF in
  it would end the line and split the frame. Everything else on the wire is
  spliced JSON, whose grammar already forbids raw control characters inside
  strings; this column is not, so the log enforces the charset instead.

  The charset is exactly what an Elixir module name uses, so any legal
  `DMEx.Message` passes. It is a database constraint rather than a check in the
  append path because it has to hold for every writer of the table, including a
  backfill or a psql session.
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
  Drops the DMEx tables.

  Note what this destroys: `dmex_messages` is an append-only log that nothing
  else keeps a copy of, and dropping it is not recoverable from any consumer.
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

      # The only classification a row carries, and the only one it ever will:
      # it describes what the message *is*, never who might want to read it.
      # Who wants what is resolved at read time, by the reader.
      #
      # Constrained below, because this column goes on the wire as an SSE
      # `event:` line — unescaped, since SSE field lines have no escaping.
      add(:message_type, :text, null: false)

      # jsonb so operators can query inside payloads directly. The accepted cost
      # is normalization: key order is not preserved and duplicate keys are
      # dropped, so stored bytes are not byte-identical to what the publisher
      # encoded.
      add(:data, :jsonb, null: false)

      # Entirely publisher-owned; DMEx writes nothing of its own into it, so
      # there are no reserved key names to avoid. Defaulted rather than nullable
      # so no read path has to distinguish "no metadata" from NULL.
      add(:metadata, :jsonb, null: false, default: fragment("'{}'::jsonb"))

      # timestamptz specifically, and for observability only — never for
      # ordering, which is what `id` is for. Postgrex returns %DateTime{} for
      # this type, which is what renders the trailing Z on the wire; a plain
      # `timestamp` would come back as %NaiveDateTime{} and silently drop it.
      add(:inserted_at, :timestamptz, null: false, default: fragment("now()"))
    end

    # The one secondary index, for exactly one query shape: a selective
    # subscription's `WHERE id > $1 AND message_type = ANY($2) ORDER BY id`.
    #
    # The planner uses it **when the selection is rare and not otherwise**, and
    # that is the right behaviour rather than a limitation. Measured on a 500k
    # skewed log: selecting a type that is 0.1% of rows takes the index; at 1%
    # and above it prefers walking the primary key in id order and filtering,
    # which is genuinely cheaper — the scan stops as soon as it has a batch, so
    # the more of the log you select the less it examines per row returned.
    #
    # So this index earns its keep at the selectivity where a scan would have to
    # walk thousands of rows per row returned, and stays out of the way at the
    # selectivity where it would be the slower plan. Both branches are correct;
    # nothing forces the choice.
    #
    # It costs an index write per append, paid by every producer whether or not
    # anyone selects against it, and it is the reason a whole-log read is issued
    # as a *separate statement* — see `DMEx.Storage.Postgres.read/5`. Folding
    # both into one query with a nullable predicate would defeat this index and
    # the primary key at once.
    create_if_not_exists(index(:dmex_messages, [:message_type, :id], prefix: prefix))

    # `DMEx.SSE.Frame` writes this column straight onto an `event:` line, and SSE
    # field lines have no escaping: a CR or LF in here would end the line and
    # split the frame. Everything else on the wire is protected by JSON's grammar
    # forbidding raw control characters inside strings; this column is not
    # spliced through an encoder, so the log enforces the charset instead.
    #
    # `[A-Za-z0-9_.]` is exactly what an Elixir module name uses, so a legal
    # `DMEx.Message` always passes and nothing legitimate is turned away. A
    # constraint rather than a validation in the append path because it must
    # hold for *every* writer of this table, including a backfill or a psql
    # session.
    #
    # Written out rather than through `constraint/3`, because Ecto has no
    # `create_if_not_exists` for constraints and Postgres has no
    # `ADD CONSTRAINT IF NOT EXISTS` — and every other statement here is
    # re-runnable, which is a property worth keeping for the whole migration
    # rather than for all of it but one line.
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
      # The identifier a handler declares — "orders/order_projection", the
      # `log/id` pair from `use DMEx.Handler` — so it is the key
      # rather than a surrogate id. The resulting unique index is what enforces
      # "one cursor per subscription" in storage, not only in the process
      # registry.
      #
      # Keying on the declaration rather than the handler's module name is what
      # makes renaming that module free. What still costs a migration is
      # changing the declaration itself — a different `id:`, or a different key
      # in the reader's `logs:` — which is what
      # `DMEx.Handler.Cursors.rename!/3` is for.
      #
      # Both halves are `[a-z][a-z0-9_]*`, so "/" separates them unambiguously
      # and two distinct pairs can never render to one string.
      add(:subscription, :text, primary_key: true)
      add(:last_seen, :bigint, null: false)
      add(:updated_at, :timestamptz, null: false, default: fragment("now()"))
    end

    # **Structurally a message log.** Everything below `subscription` is
    # `dmex_messages` — the same column names, types and meanings, with `id`
    # still the producer's own log id and `inserted_at` still the original
    # append time. So a dead row *is* a log row: replaying one needs no
    # reshaping, no field lifting and no second decode path, and duplicate
    # suppression still means what it always meant.
    #
    # Equal as *values*, not as bytes. The payload reaching here has been
    # decoded out of a frame and re-encoded, so key order and whitespace come
    # from Elixir's encoder rather than from the producer's jsonb rendering —
    # the same caveat that already applies to anything reading `data` back out
    # of the log.
    #
    # Note what is deliberately **not** here: the exception, its message, and
    # the attempt count. A failure is an event, not a property of the message —
    # so it goes to logs and telemetry. Persisting it would make this table a
    # mixture of two things and stop a row from simply being a message.
    create_if_not_exists table(:dmex_dead_messages, primary_key: false, prefix: prefix) do
      # The table is shared across every subscription in the application — one
      # place to see everything anything gave up on — so a row has to say which
      # subscription owns it, by the same declared identifier `dmex_cursors` is
      # keyed on. That plus the log id is the identity: a subscription reads
      # exactly one producer's log, so its own ids cannot collide.
      add(:subscription, :text, primary_key: true)

      # From here down, this is dmex_messages.
      add(:id, :bigint, primary_key: true)
      add(:message_type, :text, null: false)
      add(:data, :jsonb, null: false)
      add(:metadata, :jsonb, null: false, default: fragment("'{}'::jsonb"))

      # The one column that does *not* mean what it means in the log: when this
      # row landed here, not when the message was appended. Defaulted the same
      # way, so nothing has to supply it.
      #
      # The producer's append time is genuinely lost, and that is the trade:
      # this table grows forever like the log does, and pruning it needs an axis
      # that answers "how long has this been sitting here". Append time cannot
      # answer that — a message appended a year ago and abandoned this morning
      # would look ancient. Nothing downstream misses it: a handler is given its
      # struct and the publisher's metadata, never the row's timestamps, so a
      # replayed message cannot tell the difference.
      add(:inserted_at, :timestamptz, null: false, default: fragment("now()"))
    end

    :ok
  end
end
