defmodule DMEx.Test.Migrations.Install do
  @moduledoc """
  The migration both test repos run, exercising the same public entry point a
  host application would write.
  """

  use Ecto.Migration

  alias DMEx.Storage.Migration

  def up, do: Migration.up()
  def down, do: Migration.down()
end
