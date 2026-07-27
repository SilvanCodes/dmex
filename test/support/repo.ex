defmodule DMEx.Test.Repo do
  @moduledoc """
  Sandboxed repo for the unit suite.
  """
  use Ecto.Repo, otp_app: :dmex, adapter: Ecto.Adapters.Postgres
end

defmodule DMEx.Test.IntegrationRepo do
  @moduledoc """
  Non-sandboxed repo for the integration suite.

  Separate from `DMEx.Test.Repo` because the sandbox is not merely inconvenient
  for these tests but structurally incompatible with them — see `config/test.exs`.
  """
  use Ecto.Repo, otp_app: :dmex, adapter: Ecto.Adapters.Postgres
end
