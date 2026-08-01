defmodule DMEx.IntegrationCase do
  @moduledoc """
  Case template for the integration suite.

  Runs against `DMEx.Test.IntegrationRepo` with **no sandbox**, so writes really
  commit — see `config/test.exs` for why. The cost is that cleanup is ours to
  do, and that these tests cannot run `async: true` against the same tables.

  Tagged `:integration`, so `mix test` skips them; run them with
  `mix test.integration` or `mix test.all`.
  """

  use ExUnit.CaseTemplate

  alias DMEx.Test.IntegrationRepo
  alias Ecto.Adapters.SQL

  using do
    quote do
      import DMEx.Case, only: [eventually: 1, eventually: 2, eventually: 3]

      alias DMEx.Test.IntegrationRepo, as: Repo

      @moduletag :integration
      @moduletag :database
    end
  end

  setup do
    truncate_all!()
    on_exit(&truncate_all!/0)
    :ok
  end

  @doc """
  Empties every DMEx table.

  `RESTART IDENTITY` matters here: ids are cursors, and a test that assumed a
  fresh log would otherwise see ids continuing from the previous test's tail.
  """
  @spec truncate_all!() :: :ok
  def truncate_all! do
    tables = ~w(dmex_messages dmex_cursors dmex_dead_messages)

    existing =
      Enum.filter(tables, fn table ->
        %{rows: [[exists?]]} =
          SQL.query!(IntegrationRepo, "SELECT to_regclass($1) IS NOT NULL", [table])

        exists?
      end)

    if existing != [] do
      SQL.query!(
        IntegrationRepo,
        "TRUNCATE #{Enum.join(existing, ", ")} RESTART IDENTITY CASCADE",
        []
      )
    end

    :ok
  end
end
