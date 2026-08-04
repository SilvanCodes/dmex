defmodule DMEx.Test.Log do
  @moduledoc "Log for the sandboxed unit suite."

  use DMEx.Log, repo: DMEx.Test.Repo
end

defmodule DMEx.Test.IntegrationLog do
  @moduledoc """
  Log for the integration suite.

  A distinct `:app` from `DMEx.Test.Log`, so the two never share an
  advisory lock or a notification channel even though they point at the same
  Postgres server.

  Takes the default pacing. It used to raise `max_batch_frequency` to 100 so
  frames arrived promptly; that is the default now, so the override would only
  restate it — and a benchmark log configured unlike everyone else's is how
  ARCHITECTURE's pacing figures drifted from what the code did.
  """

  use DMEx.Log,
    repo: DMEx.Test.IntegrationRepo,
    app: :dmex_integration
end

defmodule DMEx.Test.HeartbeatLog do
  @moduledoc """
  Log for the heartbeat suite, beating fast enough that a test does not sit
  through 15 seconds of it.

  Here rather than in the test file because `DMEx.message_types/1` keys on the
  application owning the log module, and a module defined inside a test file
  belongs to no application.
  """

  use DMEx.Log,
    repo: DMEx.Test.IntegrationRepo,
    app: :dmex_heartbeat,
    heartbeat_ms: 150,
    poll_interval: 100
end

defmodule DMEx.Test.SQLiteLog do
  @moduledoc """
  Log for the SQLite integration suite.

  Its own database file, so the unsandboxed writes here cannot reach the unit
  suite's. The notifier is whatever `DMEx.Storage.SQLite` names — stated nowhere,
  which is the point.
  """

  use DMEx.Log,
    repo: DMEx.Test.SQLiteIntegrationRepo,
    storage: DMEx.Storage.SQLite,
    app: :dmex_sqlite
end
