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

  ## The two indexes

  **The primary key on `id`** serves every whole-log read directly: those are
  `id > cursor` range scans and nothing else. **`(type, id)`** serves a
  selective subscription's `id > cursor` plus a type predicate, and the planner
  takes it only when the selection is rare enough to be cheaper — see
  *What has been measured* in `ARCHITECTURE.md` for the numbers. It costs one
  index entry per append, paid whether or not anyone selects against it, because
  the alternative is that server-side selection saves bandwidth and no database
  work at all.

  ## The `type` CHECK

  `type` is restricted to `[A-Za-z0-9_.]+` because it goes on the wire as
  an SSE `event:` line and SSE field lines have no escaping. Everything else on
  the wire is spliced JSON, whose grammar already forbids raw control characters
  inside strings.

  A database constraint rather than a check in the append path, because it has
  to hold for every writer of the table: `use DMEx.Message` enforces the same
  charset on a declared `type:` at compile time, a backfill or a psql session
  goes nowhere near it.
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

  # A migration holds the Ecto repo and nothing else: `use DMEx.Log, storage:` is
  # out of reach from here, and DMEx reads no global application config. So the
  # two shipped adapters have to be named somewhere, and this is the least-bad
  # somewhere. It is a *default*, not the selection axis — that is
  # `c:DMEx.Storage.migration/0`, and `storage:` is how anything else gets in.
  @shipped %{
    Ecto.Adapters.Postgres => DMEx.Storage.Postgres,
    Ecto.Adapters.SQLite3 => DMEx.Storage.SQLite
  }

  # `repo/0` reads the migrator's process dictionary, and that is this process:
  # the host's `up/0` calls straight through to here.
  defp default_storage! do
    adapter = repo().__adapter__()

    Map.get_lazy(@shipped, adapter, fn ->
      raise ArgumentError, """
      DMEx ships no storage adapter for #{inspect(adapter)}, so a migration on it \
      has to name the one to use:

          DMEx.Storage.Migration.up(storage: MyApp.Storage)
      """
    end)
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
      storage: Keyword.get_lazy(opts, :storage, &default_storage!/0)
    }
  end
end
