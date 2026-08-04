import Config

unit_url =
  System.get_env("DMEX_POSTGRES_URL") ||
    "postgres://postgres:postgres@postgres:5432/dmex_test"

config :dmex, DMEx.Test.Repo,
  url: unit_url,
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
# The derivation replaces the whole database segment rather than rewriting a
# trailing `/dmex_test`, so it holds for any database name: host, port and
# credentials are kept, the name is always `dmex_integration_test`. Rewriting
# one literal meant a differently-named DMEX_POSTGRES_URL silently resolved
# both repos to the same database — and the integration repo is not sandboxed,
# so it truncates and writes directly.
integration_url =
  System.get_env("DMEX_POSTGRES_INTEGRATION_URL") ||
    unit_url |> URI.parse() |> Map.put(:path, "/dmex_integration_test") |> URI.to_string()

# The derivation cannot collide, but an explicit DMEX_POSTGRES_INTEGRATION_URL
# can. Refusing to boot beats discovering it from a truncated unit database.
if integration_url == unit_url do
  raise """
  DMEX_POSTGRES_INTEGRATION_URL and DMEX_POSTGRES_URL both point at #{unit_url}.

  The integration repo is deliberately un-sandboxed and truncates the tables it
  uses, so the two suites must not share a database. Point them at different
  ones, or unset DMEX_POSTGRES_INTEGRATION_URL to have it derived.
  """
end

config :dmex, DMEx.Test.IntegrationRepo,
  url: integration_url,
  pool_size: 10,
  log: false

# `setup_sqlite!/0` deletes these files before every run, so two suites sharing a
# path destroy each other mid-flight. Overridable to let concurrent runs isolate.
sqlite_dir = System.get_env("DMEX_SQLITE_DIR") || Path.expand("../tmp", __DIR__)

# SQLite needs no server, so this half of the storage suite runs anywhere. The
# two non-default settings are the ones `DMEx.Storage.SQLite` requires of any
# repo it is given, and it says so at boot:
#
#   * `:immediate` — `BEGIN DEFERRED` starts a read transaction, and promoting it
#     to a write returns SQLITE_BUSY_SNAPSHOT, which no busy handler can resolve.
#   * `:wal` — already the ecto_sqlite3 default, stated because it is load-bearing:
#     in rollback-journal mode a writer blocks readers, and DMEx's read path is one
#     continuous reader per open connection.
config :dmex, DMEx.Test.SQLiteRepo,
  database: Path.join(sqlite_dir, "dmex_test.db"),
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 5,
  journal_mode: :wal,
  default_transaction_mode: :immediate,
  busy_timeout: 5_000,
  log: false

config :dmex, DMEx.Test.SQLiteIntegrationRepo,
  database: Path.join(sqlite_dir, "dmex_integration_test.db"),
  pool_size: 5,
  journal_mode: :wal,
  default_transaction_mode: :immediate,
  busy_timeout: 5_000,
  log: false

# Deliberately left at the ecto_sqlite3 default, for the test that
# `DMEx.Storage.SQLite` refuses it.
config :dmex, DMEx.Test.DeferredRepo, database: Path.join(sqlite_dir, "dmex_deferred.db")

config :logger, level: :warning
