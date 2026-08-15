defmodule DMEx.Test.Log do
  @moduledoc "Log for the sandboxed unit suite."

  use DMEx.Log, repo: DMEx.Test.Repo
end

defmodule DMEx.Test.IntegrationLog do
  @moduledoc """
  Log for the integration suite. Default pacing, deliberately: a test log paced
  unlike everyone else's is how ARCHITECTURE's figures drifted from the code.

  A distinct database from `DMEx.Test.Log`'s, so the two never share an advisory
  lock or a notification channel even though they point at the same Postgres
  server.
  """

  use DMEx.Log, repo: DMEx.Test.IntegrationRepo
end

defmodule DMEx.Test.CoTenantLog do
  @moduledoc """
  A second log module on `DMEx.Test.IntegrationLog`'s table, so
  `DMEx.Storage.Postgres.AppendOrderingTest` can watch two log modules exclude
  each other — which is what keying the lock on the table rather than on the
  module buys.
  """

  use DMEx.Log, repo: DMEx.Test.IntegrationRepo
end

defmodule DMEx.Test.HeartbeatLog do
  @moduledoc """
  Log for the heartbeat suite, beating fast enough that a test does not sit
  through 15 seconds of it. Defined here so every suite streaming from it shares
  one compiled log.
  """

  use DMEx.Log,
    repo: DMEx.Test.IntegrationRepo,
    heartbeat_interval: 150,
    poll_interval: 100
end

defmodule DMEx.Test.SQLiteLog do
  @moduledoc """
  Log for the SQLite integration suite, on its own database file so the
  unsandboxed writes here cannot reach the unit suite's.
  """

  use DMEx.Log,
    repo: DMEx.Test.SQLiteIntegrationRepo,
    storage: DMEx.Storage.SQLite
end

defmodule DMEx.Test.StallLog do
  @moduledoc "Log for the stall-reclaim suite: a short deadline and a small write cap."

  use DMEx.Log,
    repo: DMEx.Test.IntegrationRepo,
    heartbeat_interval: 60_000,
    max_batch_bytes: 16_384,
    stall_timeout: 300,
    stall_check_interval: 50
end

defmodule DMEx.Test.PatientLog do
  @moduledoc "`DMEx.Test.StallLog` with no watcher: `stall_timeout: :infinity` starts none."

  use DMEx.Log,
    repo: DMEx.Test.IntegrationRepo,
    stall_timeout: :infinity
end

defmodule DMEx.Test.FastPollLog do
  @moduledoc "Log for the dispatcher suite, polling fast enough to observe."

  use DMEx.Log, repo: DMEx.Test.Repo, poll_interval: 50
end

defmodule DMEx.Test.BeamNotifiedLog do
  @moduledoc """
  Postgres storage, in-BEAM notification — the only pairing whose *notification*
  half a sandboxed test can observe: `:pg` delivers on send, not on COMMIT. The
  poll is effectively off, so a wakeup here can only have come from the notifier.
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
