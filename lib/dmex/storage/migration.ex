defmodule DMEx.Storage.Migration do
  @moduledoc """
  Creates the tables DMEx needs, from a migration in the host application.

  Every function here runs inside the host's `Ecto.Migration`. DMEx lives in your
  database alongside your own tables, so its schema is yours to migrate:

      defmodule MyApp.Repo.Migrations.AddDMEx do
        use Ecto.Migration

        def up, do: DMEx.Storage.Migration.up()
        def down, do: DMEx.Storage.Migration.down()
      end

  An application usually appends, subscribes, or both, and only needs the tables
  for the roles it plays:

      DMEx.Storage.Migration.up(tables: [:messages])                  # a producer
      DMEx.Storage.Migration.up(tables: [:cursors, :dead_messages])   # a consumer
      DMEx.Storage.Migration.up()                                     # all three, the default

  The DDL itself is the storage adapter's, named by its `c:DMEx.Storage.migration/0`
  — `DMEx.Storage.Postgres.Migration` or `DMEx.Storage.SQLite.Migration` — so those
  two lines are the same whatever the database is, and a host switching engines
  rewrites nothing.

  ## Options

    * `:tables` — any of `[:messages, :cursors, :dead_messages]`, named for the
      role each plays (default all three). A pure producer needs `:messages`
      alone; a pure consumer needs the other two.

      `:messages` creates two tables: the log, and the `dmex_message_entities`
      index beside it. One name rather than two, because entities exist only
      where messages do — a producer asking for its log must not end up with
      half a schema, and `c:DMEx.Storage.append/5` writes both or neither.
    * `:prefix` — a Postgres schema to create the tables in, created if absent.
      SQLite has no schemas and raises on it.
    * `:storage` — the `DMEx.Storage` adapter whose DDL to run. Defaults to the
      shipped adapter for the repo's engine, so only an adapter DMEx does not
      ship has to name itself here.
  """

  use Ecto.Migration

  @reindex_opts [:storage, :prefix, :batch_size]

  # Messages per statement, not entities: a message's own count is unbounded, so
  # this is the knob and the number is deliberately small. The binding constraint
  # is the driver's parameter limit — SQLite's `SQLITE_MAX_VARIABLE_NUMBER` is 999
  # on an old system library, and `entities_sql/2` binds one parameter per entity
  # plus one per message. A log with many entities per message should lower it.
  @default_batch_size 100

  @typedoc "One DMEx table, named as `:tables` names it."
  @type table :: :messages | :cursors | :dead_messages

  @tables [:messages, :cursors, :dead_messages]

  @doc "Creates the DMEx tables. See the module documentation for options."
  @spec up(keyword()) :: :ok
  def up(opts \\ []) when is_list(opts) do
    %{tables: tables, prefix: prefix, storage: storage} = validate!(opts)
    migration!(storage).up(tables, prefix)
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
    %{tables: tables, prefix: prefix, storage: storage} = validate!(opts)
    migration!(storage).down(tables, prefix)
  end

  @doc """
  Rebuilds `dmex_message_entities` from the log, by dropping it and indexing every
  message again.

      defmodule MyApp.Repo.Migrations.ReindexDMExEntities do
        use Ecto.Migration

        def up, do: DMEx.Storage.Migration.reindex_entities()
        def down, do: :ok
      end

  This is the operation that makes the entity index *derived* rather than stored:
  `c:DMEx.Message.entities/1` is a projection of a message's own fields, so the
  set is recomputable from the row for as long as the module exists. Run it after
  changing an `entities:` declaration — the rows already appended keep whatever
  was indexed at the time, and nothing else goes back for them.

  `down/0` is `:ok` rather than an inverse: there is no previous state to restore,
  only the one this rebuilds.

  ## What it does, in order

    1. Reads the log's tip and every distinct type in it, in one statement.
    2. **Refuses, before touching anything**, if any of those types resolves to no
       message module on this node. Such a row cannot be indexed here, and
       dropping the table first would have silently shrunk the index by exactly
       those rows. A type in this list is usually one whose module was renamed
       without `retired:` — `DMEx.Message` has that story.
    3. Drops the table and creates it again from the same DDL a fresh migration
       runs, so the rebuilt schema cannot drift from the declared one, and
       `flush/0`es so that lands before anything is written.
    4. Pages the log oldest-first up to the tip it read in step 1, casts each row
       through `c:DMEx.Message.cast/1`, asks its module for the entities, and
       writes them a batch per statement.

  Bounding at the tip is what makes a concurrent append safe rather than a
  conflict: appends serialize, so every id at or below that tip is already
  committed, and every row appended during the rebuild takes an id above it and
  indexes itself through `c:DMEx.Storage.append/5`. The window that is *not* safe
  is between the drop and the create, where the table does not exist and an append
  fails — loudly, and in a migration.

  Call it from `up/0` rather than `change/0`: it flushes, which `change/0` forbids
  — and there is no inverse to infer anyway.

  Re-runnable, and worth knowing: it is one transaction unless the host migration
  says `@disable_ddl_transaction true`, so a large log either rebuilds or does
  not. With the transaction disabled a crash leaves the index partial, and the
  fix is to run it again.

  ## Options

    * `:batch_size` — messages per insert, default `#{@default_batch_size}`. See
      the note on the attribute for why it is small.
    * `:prefix` and `:storage` — as `up/1` takes them.
  """
  @spec reindex_entities(keyword()) :: :ok
  def reindex_entities(opts \\ []) when is_list(opts) do
    DMEx.Config.keys!(
      opts,
      @reindex_opts,
      "`DMEx.Storage.Migration.reindex_entities/1`",
      " It takes `batch_size:`, `prefix:` and `storage:`."
    )

    # Every pure check before anything reaches the migrator: `repo/0` reads the
    # process dictionary, so a bad option outside a migration should name itself
    # rather than fail on the context.
    batch_size = batch_size!(opts)
    prefix = Keyword.get(opts, :prefix)

    # `:storage` is taken rather than fetched-with-nil: `DMEx.Storage.opts/1`
    # derives the adapter from the repo through `Keyword.get_lazy/3`, which an
    # explicit `storage: nil` would satisfy instead of falling back.
    storage = DMEx.Supervisor.storage!([repo: repo(), prefix: prefix] ++ Keyword.take(opts, [:storage]))

    # Both halves of step 1 from one statement, and the tip is the bound every
    # later read uses: see the moduledoc for why that makes appends safe.
    {:ok, {tip, types}} = DMEx.Storage.appended_since(storage, 0)

    resolvable!(types)

    migration!(storage.adapter).reset_entities(prefix)

    # `Ecto.Migration` queues DDL and runs it when the migration ends, so without
    # this the rebuild below would insert into the *old* table and the recreate
    # would then throw the result away. `flush/0` is what orders DDL before data
    # inside one migration, and it is why this cannot be called from `change/0`.
    flush()

    if tip, do: rebuild(storage, tip, batch_size, 0)

    :ok
  end

  # Before the drop, never after: a type with no module here cannot be indexed,
  # and discovering that halfway through a rebuild would leave the index missing
  # exactly those rows with nothing to say so.
  defp resolvable!(types) do
    case Enum.reject(types, &DMEx.Message.for_type/1) do
      [] ->
        :ok

      orphaned ->
        raise ArgumentError,
              "the log holds #{inspect(orphaned)}, which no message module on this node " <>
                "declares, so their entities cannot be recomputed and the index would come " <>
                "back missing exactly those rows. Nothing has been dropped. Either run this " <>
                "where those modules are compiled, or declare the strings on the modules that " <>
                "replaced them with `retired:`."
    end
  end

  defp rebuild(storage, tip, batch_size, cursor) do
    filters = %{
      types: :all,
      entities: :all,
      after_id: cursor,
      before_id: tip + 1,
      order: :asc,
      limit: batch_size
    }

    case DMEx.Storage.select(storage, filters) do
      {:ok, []} ->
        :ok

      {:ok, rows} ->
        DMEx.Storage.index_entities(storage, Enum.map(rows, &entry!/1))

        rebuild(storage, tip, batch_size, List.last(rows).id)

      {:error, reason} ->
        raise "reindexing the entity index failed reading the log: #{inspect(reason)}"
    end
  end

  # `resolvable!/1` has already proven every type in the log resolves, so a miss
  # here is a module set that changed underneath a running migration.
  defp entry!(row) do
    module = DMEx.Message.for_type(row.type)
    {:ok, message, _metadata} = DMEx.Message.decode(row, module)

    {row.id, module.entities(message)}
  end

  defp batch_size!(opts) do
    case Keyword.get(opts, :batch_size, @default_batch_size) do
      size when is_integer(size) and size > 0 ->
        size

      other ->
        raise ArgumentError, "`batch_size:` must be a positive integer, got: #{inspect(other)}"
    end
  end

  # The DDL comes from the adapter rather than from a `case` here, so a
  # third-party engine is migrated by naming itself — see
  # `c:DMEx.Storage.migration/0`.
  defp migration!(storage) do
    case DMEx.Storage.optional(storage, :migration, []) do
      {:ok, migration} ->
        migration

      :none ->
        raise ArgumentError,
              "#{inspect(storage)} names no migration, so DMEx cannot create its " <>
                "tables on that engine. A `DMEx.Storage` adapter points at its own " <>
                "DDL module from `c:DMEx.Storage.migration/0`."
    end
  end

  defp validate!(opts) do
    tables = Keyword.get(opts, :tables, @tables)

    if !(is_list(tables) and tables != [] and Enum.all?(tables, &(&1 in @tables))) do
      raise ArgumentError,
            ":tables must be a non-empty list drawn from #{inspect(@tables)}, " <>
              "got: #{inspect(tables)}"
    end

    %{
      tables: tables,
      prefix: Keyword.get(opts, :prefix),
      # `repo/0` reads the migrator's process dictionary, which is this process:
      # the host's `up/0` calls straight through to here.
      storage: Keyword.get_lazy(opts, :storage, fn -> DMEx.Storage.default_adapter(repo()) end)
    }
  end
end
