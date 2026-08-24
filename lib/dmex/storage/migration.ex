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
      tables they create (default all three). A pure producer needs `:messages`
      alone; a pure consumer needs the other two.
    * `:prefix` — a Postgres schema to create the tables in, created if absent.
      SQLite has no schemas and raises on it.
    * `:storage` — the `DMEx.Storage` adapter whose DDL to run. Defaults to the
      shipped adapter for the repo's engine, so only an adapter DMEx does not
      ship has to name itself here.
  """

  use Ecto.Migration

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
