database_available? = DMEx.Test.Database.available?()

if database_available? do
  DMEx.Test.Database.setup!()

  {:ok, _} = DMEx.Test.Repo.start_link()
  {:ok, _} = DMEx.Test.IntegrationRepo.start_link()

  DMEx.Test.Database.migrate!()

  Ecto.Adapters.SQL.Sandbox.mode(DMEx.Test.Repo, :manual)
else
  uri = DMEx.Test.Database.uri()

  IO.puts("""

  \e[33mNo Postgres reachable at #{uri.host}:#{uri.port} — skipping :database tests.\e[0m

  The dev container ships one; if it is not running, rebuild the container
  (Dev Containers: Rebuild Container) so .devcontainer/docker-compose.yml
  takes effect, or point DMEX_POSTGRES_URL at your own instance.
  """)
end

ExUnit.start(exclude: if(database_available?, do: [], else: [:database]))
