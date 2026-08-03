defmodule DMEx.Test.Migrations.Install do
  @moduledoc """
  The migration both test repos run, exercising the same public entry point a
  host application would write.
  """

  use Ecto.Migration

  def up, do: DMEx.Storage.Migration.up()
  def down, do: DMEx.Storage.Migration.down()
end
