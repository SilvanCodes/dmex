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
    app_key: :dmex_integration
end

defmodule DMEx.Test.HeartbeatLog do
  @moduledoc """
  Log for the heartbeat suite, beating fast enough that a test does not sit
  through 15 seconds of it.

  Here rather than in the test file so it is compiled once and the suites that
  stream from it share one log.
  """

  use DMEx.Log,
    repo: DMEx.Test.IntegrationRepo,
    app_key: :dmex_heartbeat,
    heartbeat_interval: 150,
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
    app_key: :dmex_sqlite
end

defmodule DMEx.Test.StallLog do
  @moduledoc """
  Log for the stall-reclaim suite: a short deadline and a small write cap.

  Here rather than in the test file for the same reason as
  `DMEx.Test.HeartbeatLog`.
  """

  use DMEx.Log,
    repo: DMEx.Test.IntegrationRepo,
    app_key: :dmex_stall,
    heartbeat_interval: 60_000,
    max_batch_bytes: 16_384,
    stall_timeout: 300,
    stall_check_interval: 50
end

defmodule DMEx.Test.PatientLog do
  @moduledoc "`DMEx.Test.StallLog` with no watcher: `stall_timeout: :infinity` starts none."

  use DMEx.Log,
    repo: DMEx.Test.IntegrationRepo,
    app_key: :dmex_stall_patient,
    stall_timeout: :infinity
end

defmodule DMEx.Test.FastPollLog do
  @moduledoc "Log for the dispatcher suite, polling fast enough to observe."

  use DMEx.Log, repo: DMEx.Test.Repo, poll_interval: 50
end

defmodule DMEx.Test.BeamNotifiedLog do
  @moduledoc """
  Postgres storage, in-BEAM notification.

  Storage and notifier are separate axes, and this is the only pairing whose
  *notification* half a sandboxed test can observe at all: `:pg` delivers on
  send, not on COMMIT. The poll is effectively off, so a wakeup here can only
  have come from the notifier.
  """

  use DMEx.Log,
    repo: DMEx.Test.Repo,
    notifier: DMEx.Notifier.Beam,
    poll_interval: 60_000
end

defmodule DMEx.Test.ImpatientLog do
  @moduledoc "Log for the watcher suite: a deadline short enough to trip in a test."

  use DMEx.Log, repo: DMEx.Test.Repo, stall_timeout: 20, stall_check_interval: 10
end
