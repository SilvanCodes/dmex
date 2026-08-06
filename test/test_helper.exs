alias DMEx.Test.Database
alias DMEx.Test.Repo
alias DMEx.Test.SQLiteRepo
alias Ecto.Adapters.SQL.Sandbox

database_available? = Database.available?()
sqlite_available? = Database.sqlite_available?()

if database_available? do
  Database.setup!()

  {:ok, _} = Repo.start_link()
  {:ok, _} = DMEx.Test.IntegrationRepo.start_link()

  Database.migrate!()

  Sandbox.mode(Repo, :manual)
else
  uri = Database.uri()

  IO.puts("""

  \e[33mNo Postgres reachable at #{uri.host}:#{uri.port} — skipping :database tests.\e[0m

  The dev container ships one; if it is not running, rebuild the container
  (Dev Containers: Rebuild Container) so .devcontainer/docker-compose.yml
  takes effect, or point DMEX_POSTGRES_URL at your own instance.
  """)
end

if sqlite_available? do
  Database.setup_sqlite!()

  {:ok, _} = SQLiteRepo.start_link()
  {:ok, _} = DMEx.Test.SQLiteIntegrationRepo.start_link()

  Database.migrate_sqlite!()

  Sandbox.mode(SQLiteRepo, :manual)
end

# What a host application's `DMEx.Supervisor` does with its `otp_app:` at boot.
# The suite's own message modules live in `:dmex`, which declares no dependency
# on itself, so no scan reaches them unless something names it — and most tests
# here read the index without starting a supervisor.
DMEx.Message.refresh_index(:dmex)

exclude =
  if(database_available?, do: [], else: [:database]) ++
    if(sqlite_available?, do: [], else: [:sqlite])

ExUnit.start(exclude: exclude)
