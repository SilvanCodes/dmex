import Config

config :dmex, DMEx.Test.Repo,
  url:
    System.get_env("DMEX_POSTGRES_URL") ||
      "postgres://postgres:postgres@postgres:5432/dmex_test",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 10,
  log: false

# The integration repo is deliberately *not* sandboxed. Two things in the design
# cannot be observed inside a sandbox transaction:
#
#   * LISTEN/NOTIFY — Postgres delivers notifications at COMMIT, and the sandbox
#     never commits, so every Dispatcher assertion would silently see nothing.
#   * Append ordering — the test needs genuinely concurrent transactions, which
#     is exactly what shared-mode sandbox collapses into one.
# Derived from DMEX_POSTGRES_URL when no dedicated variable is set, so a CI job
# that sets only the first does not reach the unit database for both suites.
#
# The derivation rewrites a trailing `/dmex_test` and nothing else: point
# DMEX_POSTGRES_URL at a differently-named database and both repos resolve to
# it. Set DMEX_POSTGRES_INTEGRATION_URL explicitly in that case — the
# integration repo is not sandboxed, so it would otherwise run against the unit
# database directly.
integration_url =
  System.get_env("DMEX_POSTGRES_INTEGRATION_URL") ||
    case System.get_env("DMEX_POSTGRES_URL") do
      nil -> "postgres://postgres:postgres@postgres:5432/dmex_integration_test"
      url -> String.replace_trailing(url, "/dmex_test", "/dmex_integration_test")
    end

config :dmex, DMEx.Test.IntegrationRepo,
  url: integration_url,
  pool_size: 10,
  log: false

config :logger, level: :warning
