defmodule DMEx.Integration.ReindexEntitiesTest do
  # `DMEx.Storage.Migration.reindex_entities/1` runs inside `Ecto.Migration`, so
  # it needs a real migrator against a repo that is not sandboxed — which is what
  # the integration repo is. The claim under test is the one the entity index
  # rests on: it is a materialized projection of `c:DMEx.Message.entities/1`, so
  # it can be thrown away and recomputed from the log alone.
  use DMEx.IntegrationCase

  alias DMEx.Storage.Migration
  alias DMEx.Test.IntegrationLog, as: Log
  alias DMEx.Test.Messages.EntityPlaced
  alias DMEx.Test.Messages.EntityShipped
  alias Ecto.Adapters.SQL

  defmodule Reindex do
    @moduledoc false
    use Ecto.Migration

    def up, do: Migration.reindex_entities(batch_size: 2)
  end

  defmodule ReindexDefaults do
    @moduledoc false
    use Ecto.Migration

    def up, do: Migration.reindex_entities()
  end

  setup do
    start_supervised!(Log)
    :ok
  end

  test "rebuilds an index that was thrown away, exactly" do
    order = entity()
    customer = entity()

    {:ok, _} = Log.append(%EntityPlaced{order_id: order, customer_id: customer})
    {:ok, _} = Log.append(%EntityShipped{order_id: order, line_item_ids: [entity(), entity()]})
    {:ok, _} = Log.append(%EntityPlaced{order_id: entity()})

    before = index()
    assert length(before) == 6

    SQL.query!(Repo, "DELETE FROM dmex_message_entities", [])
    assert index() == []

    migrate!(Reindex)

    assert index() == before
  end

  test "a message declaring no entities contributes nothing, before and after" do
    {:ok, id} = Log.append(%DMEx.Test.Messages.OrderPlaced{order_id: 7})

    assert index() == []

    migrate!(Reindex)

    assert index() == []
    refute Enum.any?(index(), &match?({_, ^id}, &1))
  end

  test "pages the whole log, not one batch of it" do
    order = entity()
    for _ <- 1..7, do: {:ok, _} = Log.append(%EntityPlaced{order_id: order})

    before = index()
    assert length(before) == 7

    SQL.query!(Repo, "DELETE FROM dmex_message_entities", [])

    # batch_size: 2 against seven messages, so a rebuild that stops after one
    # statement comes back short rather than empty.
    migrate!(Reindex)

    assert index() == before
  end

  test "an index already correct is left as it was" do
    {:ok, _} = Log.append(%EntityPlaced{order_id: entity()})

    before = index()

    migrate!(ReindexDefaults)

    assert index() == before
  end

  # The refusal that has to happen *before* the drop: a type with no module here
  # cannot be recomputed, so rebuilding would silently return a smaller index.
  test "refuses a log holding a type this node cannot resolve, and drops nothing" do
    {:ok, _} = Log.append(%EntityPlaced{order_id: entity()})
    before = index()

    {:ok, _} =
      DMEx.Storage.append(
        Log.__dmex_log__().storage,
        "nothing.declares.this",
        ~s({"order_id":"x"}),
        "{}",
        ## Helpers
        []
      )

    assert_raise ArgumentError, ~r/no message module on this node declares/, fn ->
      migrate!(Reindex)
    end

    assert index() == before
  end

  # A timestamp version, because `schema_migrations` is not truncated between
  # integration runs and the migrator warns about running one older than a version
  # already recorded. The row is removed afterwards so nothing accumulates.
  defp migrate!(module) do
    version = System.os_time(:microsecond)

    try do
      Ecto.Migrator.up(Repo, version, module, log: false)
    after
      SQL.query!(Repo, "DELETE FROM schema_migrations WHERE version = $1", [version])
    end
  end

  defp index do
    %{rows: rows} =
      SQL.query!(Repo, "SELECT entity, id FROM dmex_message_entities ORDER BY id, entity", [])

    Enum.map(rows, &List.to_tuple/1)
  end

  defp entity, do: "e-#{System.unique_integer([:positive])}"
end
