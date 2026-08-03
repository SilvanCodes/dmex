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

defmodule DMEx.Test.SQLiteIntegrationRepo do
  @moduledoc """
  Non-sandboxed SQLite repo for the integration suite.

  Its own file, and no sandbox: the append-ordering test needs writes that
  genuinely commit and writers that genuinely contend.
  """
  use Ecto.Repo, otp_app: :dmex, adapter: Ecto.Adapters.SQLite3
end

defmodule DMEx.Test.DeferredRepo do
  @moduledoc """
  A SQLite repo left in ecto_sqlite3's default transaction mode.

  Never started — it exists so `DMEx.Storage.SQLite.validate!/1` has a repo to
  refuse.
  """
  use Ecto.Repo, otp_app: :dmex, adapter: Ecto.Adapters.SQLite3
end

defmodule DMEx.Test.SQLiteRepo do
  @moduledoc """
  Sandboxed repo for the SQLite half of the storage suite.

  A file database rather than `:memory:`: with a pool, every connection to
  `:memory:` gets a database of its own, so the migration would land on one
  connection and the tests on another.
  """
  use Ecto.Repo, otp_app: :dmex, adapter: Ecto.Adapters.SQLite3
end
