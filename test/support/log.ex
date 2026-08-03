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
