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

  Reads twenty times more often than the default, because pacing is log
  configuration: a test that wants frames promptly has to say so here, where the
  setting lives, rather than on its subscription.
  """

  use DMEx.Log,
    repo: DMEx.Test.IntegrationRepo,
    app: :dmex_integration,
    max_batch_frequency: 100
end
