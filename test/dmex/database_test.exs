defmodule DMEx.DatabaseTest do
  @moduledoc """
  Confirms the sandboxed unit-suite database is actually usable. Skipped
  automatically when no Postgres is reachable.
  """

  use DMEx.Case, async: true, database: true

  alias Ecto.Adapters.SQL

  @probe "SELECT to_regclass('sandbox_probe') IS NOT NULL"

  test "a sandboxed connection is checked out and queryable" do
    assert %{rows: [[1]]} = SQL.query!(Repo, "SELECT 1", [])
  end

  test "writes are rolled back between tests" do
    SQL.query!(Repo, "CREATE TABLE sandbox_probe (id int)", [])

    assert %{rows: [[true]]} = SQL.query!(Repo, @probe, [])
  end

  test "the table created by the previous test is gone" do
    assert %{rows: [[false]]} = SQL.query!(Repo, @probe, [])
  end
end
