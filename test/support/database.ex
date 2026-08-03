defmodule DMEx.Test.Database do
  @moduledoc """
  Database availability probing and setup for the test suites.

  Tests needing Postgres are tagged `:database` and excluded when none is
  reachable, so the pure units still run without one. SQLite tests are tagged
  `:sqlite` and need no server at all, which is what lets the storage layer be
  exercised on a machine with nothing running.
  """

  alias Ecto.Adapters.Postgres
  alias Ecto.Adapters.SQLite3

  @repos [DMEx.Test.Repo, DMEx.Test.IntegrationRepo]
  @sqlite_repos [DMEx.Test.SQLiteRepo, DMEx.Test.SQLiteIntegrationRepo]

  @doc """
  Whether the configured Postgres is accepting connections.

  Probes the socket directly rather than starting a repo, so an unreachable
  database costs one refused connect instead of a pool's worth of retries and
  error logging.
  """
  @spec available?() :: boolean()
  def available? do
    %{host: host, port: port} = uri()

    case :gen_tcp.connect(String.to_charlist(host), port, [:binary, active: false], 2_000) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        true

      {:error, _reason} ->
        false
    end
  end

  @doc """
  Creates both test databases if they do not exist.

  The Postgres image creates only `POSTGRES_DB`; the integration database is
  ours to make.
  """
  @spec setup!() :: :ok
  def setup! do
    Enum.each(@repos, fn repo ->
      config = repo.config()

      case Postgres.storage_up(config) do
        :ok ->
          :ok

        {:error, :already_up} ->
          :ok

        {:error, reason} ->
          raise """
          could not create storage for #{inspect(repo)}: #{inspect(reason)}

          It is configured for #{config[:hostname]}:#{config[:port]}/#{config[:database]}.
          If that host is wrong, set DMEX_POSTGRES_URL and/or
          DMEX_POSTGRES_INTEGRATION_URL — see config/test.exs for how the second
          is derived from the first.
          """
      end
    end)
  end

  @doc """
  Runs the DMEx migration against both test repos.

  Uses `Ecto.Migrator` rather than calling `DMEx.Storage.Migration.up/1` directly, so
  the suite exercises the same path a host application takes and re-runs are
  recorded in `schema_migrations` instead of repeated.
  """
  @spec migrate!() :: :ok
  def migrate! do
    Enum.each(@repos, fn repo ->
      # Down first, so an edit to `DMEx.Storage.Migration` takes effect on the next run.
      # Its DDL is deliberately re-runnable — `create_if_not_exists` throughout —
      # which means a *changed* column default or a new index would otherwise be
      # skipped against a database from before the edit, and the suite would test
      # yesterday's schema. Greenfield: the tables are cheap and hold nothing
      # worth keeping between runs.
      Ecto.Migrator.down(repo, 1, DMEx.Test.Migrations.Install, log: false)
      Ecto.Migrator.up(repo, 1, DMEx.Test.Migrations.Install, log: false)
    end)
  end

  @doc """
  Whether the SQLite adapter is available.

  Which is whether its optional dependency compiled — there is no server to
  reach, so `:sqlite` tests are excluded only in a build that left the driver
  out entirely.
  """
  @spec sqlite_available?() :: boolean()
  def sqlite_available?, do: Code.ensure_loaded?(SQLite3)

  @doc """
  Creates the SQLite test database, discarding whatever was there.

  Deleting the file is how a changed DDL takes effect: the migration is
  deliberately re-runnable, so an added index or a changed default would
  otherwise be skipped against a database from before the edit — the same reason
  `migrate!/0` runs `down` first on Postgres. The `-wal` and `-shm` siblings go
  too, or SQLite reopens the old database through them.
  """
  @spec setup_sqlite!() :: :ok
  def setup_sqlite! do
    Enum.each(@sqlite_repos, fn repo ->
      path = repo.config() |> Keyword.fetch!(:database)

      File.mkdir_p!(Path.dirname(path))

      for suffix <- ["", "-wal", "-shm"], do: File.rm_rf!(path <> suffix)

      :ok = SQLite3.storage_up(repo.config())
    end)
  end

  @doc "Runs the DMEx migration against the SQLite test repo."
  @spec migrate_sqlite!() :: :ok
  def migrate_sqlite! do
    Enum.each(@sqlite_repos, fn repo ->
      Ecto.Migrator.up(repo, 1, DMEx.Test.Migrations.Install, log: false)
    end)
  end

  @doc "The configured unit-suite database URI."
  @spec uri() :: URI.t()
  def uri do
    :dmex
    |> Application.fetch_env!(DMEx.Test.Repo)
    |> Keyword.fetch!(:url)
    |> URI.parse()
  end
end
