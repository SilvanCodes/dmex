defmodule DMEx.Test.Database do
  @moduledoc """
  Database availability probing and setup for the test suites.

  Tests needing Postgres are tagged `:database` and excluded when none is
  reachable, so the pure units still run without one. SQLite tests are tagged
  `:sqlite` and need no server at all, which is what lets the storage layer be
  exercised on a machine with nothing running.
  """

  alias DMEx.Test.IntegrationRepo
  alias DMEx.Test.Migrations.Install
  alias DMEx.Test.Repo
  alias Ecto.Adapters.Postgres
  alias Ecto.Adapters.SQLite3

  @repos [Repo, IntegrationRepo]
  @sqlite_repos [DMEx.Test.SQLiteRepo, DMEx.Test.SQLiteIntegrationRepo]

  # Any fixed key in a space nothing else in this database uses. It is not
  # DMEx's append lock: that one is `{classid, objid}` and scoped to a schema,
  # and this claims a whole database against another OS process.
  @integration_lock 0x646D6578

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
      Ecto.Migrator.down(repo, 1, Install, log: false)
      Ecto.Migrator.up(repo, 1, Install, log: false)
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
      path = Keyword.fetch!(repo.config(), :database)

      File.mkdir_p!(Path.dirname(path))

      for suffix <- ["", "-wal", "-shm"], do: File.rm_rf!(path <> suffix)

      :ok = SQLite3.storage_up(repo.config())
    end)
  end

  @doc "Runs the DMEx migration against the SQLite test repo."
  @spec migrate_sqlite!() :: :ok
  def migrate_sqlite! do
    Enum.each(@sqlite_repos, fn repo ->
      Ecto.Migrator.up(repo, 1, Install, log: false)
    end)
  end

  @doc "The configured unit-suite database URI."
  @spec uri() :: URI.t()
  def uri do
    :dmex
    |> Application.fetch_env!(Repo)
    |> Keyword.fetch!(:url)
    |> URI.parse()
  end

  @doc """
  Claims the integration database for this OS process, or refuses to share it.

  The integration suite truncates all three tables on every test setup and the
  benchmarks drain for minutes; both run against `dmex_integration_test`, and
  neither is sandboxed. Run together, a benchmark mid-drain watches its log
  vanish and reports as a run that never finished, which is how this was found.

  A session advisory lock on a connection this process owns is the whole guard.
  It is released when the VM exits, so there is nothing to clean up and a killed
  run does not leave the database claimed.
  """
  @spec claim_integration!(String.t()) :: :ok
  def claim_integration!(claimant) do
    {:ok, conn} =
      IntegrationRepo.config()
      |> Keyword.take([:hostname, :port, :database, :username, :password, :socket_dir, :ssl])
      |> Keyword.put(:pool_size, 1)
      |> Postgrex.start_link()

    case Postgrex.query!(conn, "SELECT pg_try_advisory_lock($1)", [@integration_lock]) do
      %{rows: [[true]]} ->
        :ok

      %{rows: [[false]]} ->
        raise """
        #{claimant} could not claim dmex_integration_test: something else is using it.

        The integration suite and the benchmarks share this database and truncate
        under each other. Run them one at a time.
        """
    end
  end
end
