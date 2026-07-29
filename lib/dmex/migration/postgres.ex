defmodule DMEx.Migration.Postgres do
  @moduledoc """
  The Postgres DDL behind `DMEx.Migration`.

  Called from inside a host application's `Ecto.Migration`, so every function
  here runs in the migration process and uses the migration DSL directly.
  """

  use Ecto.Migration

  @doc false
  @spec up(%{tables: [atom()], prefix: String.t() | nil}) :: :ok
  def up(%{tables: tables, prefix: prefix}) do
    if prefix, do: execute("CREATE SCHEMA IF NOT EXISTS \"#{prefix}\"")

    if :producer in tables, do: up_producer(prefix)
    if :consumer in tables, do: up_consumer(prefix)

    :ok
  end

  @doc false
  @spec down(%{tables: [atom()], prefix: String.t() | nil}) :: :ok
  def down(%{tables: tables, prefix: prefix}) do
    if :consumer in tables do
      drop_if_exists(table(:dmex_dead_messages, prefix: prefix))
      drop_if_exists(table(:dmex_cursors, prefix: prefix))
    end

    if :producer in tables do
      drop_if_exists(table(:dmex_messages, prefix: prefix))
    end

    :ok
  end

  defp up_producer(prefix) do
    create_if_not_exists table(:dmex_messages, primary_key: false, prefix: prefix) do
      # bigint, not int: a log nothing deletes from outgrows 2^31, and every
      # consumer cursor in the network is denominated in this column.
      add(:id, :bigserial, primary_key: true)

      # The only classification a row carries, and the only one it ever will:
      # it describes what the message *is*, never who might want to read it.
      # Who wants what is resolved at read time, by the reader.
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

    # No secondary index, deliberately. See DMEx.Migration's module docs.
    :ok
  end

  defp up_consumer(prefix) do
    create_if_not_exists table(:dmex_cursors, primary_key: false, prefix: prefix) do
      # The identifier a handler declares — "orders/order_projection", the
      # `producer/id` pair from `use DMEx.Consumer.Handler` — so it is the key
      # rather than a surrogate id. The resulting unique index is what enforces
      # "one cursor per subscription" in storage, not only in the process
      # registry.
      #
      # Keying on the declaration rather than the handler's module name is what
      # makes renaming that module free. What still costs a migration is
      # changing the declaration itself — a different `id:`, or a different key
      # in the consumer's producer map — which is what
      # `DMEx.Consumer.Cursors.rename!/3` is for.
      #
      # Both halves are `[a-z][a-z0-9_]*`, so "/" separates them unambiguously
      # and two distinct pairs can never render to one string.
      add(:subscription, :text, primary_key: true)
      add(:last_seen, :bigint, null: false)

      # Informational: which producer this cursor last read from. Identity is
      # the subscription string alone, so this is for operators, not lookups —
      # which is why repointing a producer name at a new host needs no
      # migration.
      add(:producer_url, :text, null: false)
      add(:updated_at, :timestamptz, null: false, default: fragment("now()"))
    end

    # **Structurally a message log.** Everything below `producer_url` is
    # `dmex_messages` — the same column names, types and meanings, with `id`
    # still the producer's own log id and `inserted_at` still the original
    # publish time. So a dead row *is* a log row: replaying one needs no
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

      # Provenance rather than identity. Messages carry no origin, so without
      # this a dead row cannot say which producer it came from.
      add(:producer_url, :text, null: false)

      # From here down, this is dmex_messages.
      add(:id, :bigint, primary_key: true)
      add(:message_type, :text, null: false)
      add(:data, :jsonb, null: false)
      add(:metadata, :jsonb, null: false, default: fragment("'{}'::jsonb"))
      add(:inserted_at, :timestamptz, null: false)
    end

    :ok
  end
end
