defmodule DMEx.Test.Database do
  @moduledoc """
  Database availability probing and setup for the test suites.

  Most of DMEx's units — frame encoding and parsing, message casting, the queue,
  the control law — are pure and need no database at all. Rather than making the
  whole suite unrunnable without one, tests that need Postgres are tagged
  `:database` and excluded when none is reachable.
  """

  alias Ecto.Adapters.Postgres

  @repos [DMEx.Test.Repo, DMEx.Test.IntegrationRepo]

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
          If that host is wrong, set DMEX_POSTGRES_URL (the integration database
          is derived from it) or DMEX_POSTGRES_INTEGRATION_URL explicitly.
          """
      end
    end)
  end

  @doc """
  Runs the DMEx migration against both test repos.

  Uses `Ecto.Migrator` rather than calling `DMEx.Migration.up/1` directly, so
  the suite exercises the same path a host application takes and re-runs are
  recorded in `schema_migrations` instead of repeated.
  """
  @spec migrate!() :: :ok
  def migrate! do
    Enum.each(@repos, fn repo ->
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
