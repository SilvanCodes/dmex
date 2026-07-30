defmodule DMEx.Test.Producer do
  @moduledoc "Producer for the sandboxed unit suite."

  use DMEx.Producer, repo: DMEx.Test.Repo
end

defmodule DMEx.Test.IntegrationProducer do
  @moduledoc """
  Producer for the integration suite.

  A distinct `:app` from `DMEx.Test.Producer`, so the two never share an
  advisory lock or a notification channel even though they point at the same
  Postgres server.

  `batch_window` is a twentieth of the default, because pacing is producer
  configuration now: a test that wants frames promptly has to say so here, where
  the setting lives, rather than on its subscription.
  """

  use DMEx.Producer,
    repo: DMEx.Test.IntegrationRepo,
    app: :dmex_integration,
    batch_window: 10
end
