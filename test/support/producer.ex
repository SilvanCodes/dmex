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
  """

  use DMEx.Producer, repo: DMEx.Test.IntegrationRepo, app: :dmex_integration
end
