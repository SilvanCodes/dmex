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
  @message_entities DMEx.Storage.__table__(:message_entities)
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

    if :messages in tables do
      drop_if_exists(table(@message_entities, prefix: prefix))
      drop_if_exists(table(@messages, prefix: prefix))
    end

    :ok
  end

  defp up_messages(prefix) do
    create_if_not_exists table(@messages, primary_key: false, prefix: prefix) do
      # bigint, not int: a log nothing deletes from outgrows 2^31, and every
      # consumer cursor in the network is denominated in this column.
      add(:id, :bigserial, primary_key: true)

      # What the message *is* — never who might want to read it. One of the two
      # classifications a row carries, the other being what it is *about*, in
      # `dmex_message_entities` below; who wants what is still resolved at read
      # time, by the reader, and no column here will ever say otherwise.
      # Constrained below, because this one goes on the wire as an unescaped SSE
      # `event:` line — which the entity axis never does, and so is not.
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

    # For exactly one query shape: a selective subscription's `id > cursor` plus
    # a type predicate. Every whole-log read is a range scan the primary key
    # already serves, and `c:DMEx.Storage.read/5` says why a selective read is a
    # *separate statement* rather than one nullable predicate. It costs one index
    # entry per append, paid whether or not anyone selects against it, because
    # the alternative is that server-side selection saves bandwidth and no
    # database work at all.
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

    # A **backstop, deliberately looser than the real rule.** `DMEx.Message`
    # enforces `[A-Za-z0-9_]+` segments joined by single dots, so `".foo"` and
    # `"foo."` are compile errors there and legal here. The column's job is to
    # keep a row that never came through `use DMEx.Message` from putting a
    # character on an SSE `event:` line that the wire cannot escape; deciding what
    # a *well-formed* type looks like is the message module's, and duplicating
    # that decision in SQL would be a second rule to keep in step.
    #
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

    up_message_entities(prefix)
  end

  # What a message is *about*, as opposed to what it is: the second
  # classification a row carries, and the one `type` is deliberately not. Created
  # with `:messages` rather than as a role of its own, because entities exist only
  # where messages do and a producer asking for the log must not get half a schema.
  #
  # The second half of the invariant the log's `type` column states: a row says
  # what it is and what it is about, never who wants it. Both are the publisher's
  # own facts about the message, resolved against readers only at read time.
  defp up_message_entities(prefix) do
    create_if_not_exists table(@message_entities, primary_key: false, prefix: prefix) do
      # The identifier as the appending message gave it, uninterpreted: DMEx
      # composes nothing into it and parses nothing out. `DMEx.Message` has the
      # argument — an identifier that is already unique needs no namespace, and
      # one a publisher wants namespaced carries that inside the value. No charset
      # constraint, because unlike `type` this never reaches an SSE `event:` line.
      add(:entity, :text, primary_key: true)

      # bigint to match `dmex_messages.id`, which every cursor in the network is
      # also denominated in.
      add(:id, :bigint, primary_key: true)
    end

    # No index beyond the primary key, and none wanted. `(entity, id)` *is* this
    # table, and nothing asks the reverse question: entities never come back on a
    # read — they are a filter axis, not part of a message, and
    # `t:DMEx.Storage.row/0` is unchanged by their existence.
    #
    # Measured on a 500k-row log where one entity carries 1 row, one carries
    # 1 000, and one carries 250 000. The planner has **more than two** plans
    # here and picks on the entity table's own statistics: a second seeding, with
    # the same log but a different spread of entities across it, moved the
    # selective case to a `Merge Semi Join` that walks 49 503 log rows to return
    # 100. So read the shapes below as what this index makes *available*, not as
    # what is guaranteed — the `(type, id)` crossover above is a two-way choice
    # and this is not.
    #
    # The two that the first seeding produced:
    #
    #   * **Selective entity** — `Bitmap Index Scan on
    #     dmex_message_entities_pkey`, a `HashAggregate` that collapses a row
    #     carrying several of the selected entities, then one
    #     `dmex_messages_pkey` lookup each. The `ORDER BY id` is a **top-N
    #     heapsort** over that bounded set: the index hands back ids in
    #     `(entity, id)` order, but the bitmap scan does not preserve it, so the
    #     ordering is not free — it is merely cheap, because `LIMIT` caps what is
    #     sorted.
    #   * **Broad entity** — the plan inverts: `Index Scan using
    #     dmex_messages_pkey` walking the log forward, with an `Index Only Scan`
    #     semi-join probe into this table per row. No sort at all, and it stops at
    #     `LIMIT` — for the 250k-row entity it touched 200 log rows to return 100.
    #
    # All of them are bounded by the `LIMIT`, which is why `DMEx.Query` insists on
    # one, and is what keeps the worst plan here merely slower rather than
    # unbounded.
    # The entity predicate is left plain for the planner to choose between them,
    # for the same reason `type = ANY($1)` is.
    #
    # **Direction costs nothing, and neither does the shape of the bounds.** The
    # same session with `ORDER BY id DESC` produces whatever plan the ascending
    # one did, with `Index Scan Backward using dmex_messages_pkey` in place of the
    # forward scan — 199 log rows for 100 returned in the broad case either way.
    # And rendering the floor, omitting it, or replacing the ceiling with a
    # max-`bigint` sentinel are all plan-identical, down to the row estimates and
    # the rows actually touched. So neither a backwards page nor an absent bound
    # is a second query shape to reason about, which is what lets
    # `DMEx.Storage.SQL.bound/4` render both bounds under one rule.
    #
    # No foreign key either, deliberately. `c:DMEx.Storage.append/5` writes these
    # rows inside the transaction that inserts the message, so an orphan is
    # unrepresentable on the write path, and every statement inside the append's
    # serialized region costs throughput for every appender in the application
    # (`bench/e2e/append_throughput.exs`). What that gives up is `ON DELETE
    # CASCADE` for an operator pruning the log's tail — which is the retention
    # question ARCHITECTURE.md keeps open, and is the right place to decide it.
  end

  @doc false
  # Drops the entity index and builds it again empty, for
  # `DMEx.Storage.Migration.reindex_entities/1`. The creation is `up_messages/1`'s
  # own, so the rebuilt table cannot differ from the one a fresh migration makes.
  #
  # The log itself is never touched: this table holds nothing that is not derived
  # from it, which is the whole reason dropping it is a safe thing to offer.
  @spec reset_entities(String.t() | nil) :: :ok
  def reset_entities(prefix) do
    drop_if_exists(table(@message_entities, prefix: prefix))
    up_message_entities(prefix)

    :ok
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
