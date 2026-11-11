defmodule DMEx.Test.Messages do
  @moduledoc "Message modules used across the test suites."

  # The declared types deliberately do not match the module names: a fixture
  # where the two coincide cannot catch a regression that falls back to
  # deriving the type from the module.

  defmodule OrderPlaced do
    @moduledoc false
    use DMEx.Message, type: "order_placed", fields: [:order_id, :customer_id, :total]
  end

  defmodule OrderShipped do
    @moduledoc false
    use DMEx.Message, type: "order_shipped", fields: [:order_id, carrier: "unknown"]
  end

  defmodule WithCustomEncoder do
    @moduledoc false
    use DMEx.Message, type: "with_custom_encoder", fields: [:order_id, :secret]
  end

  # An explicit implementation replaces the derivation `use DMEx.Message`
  # generates — the write-side counterpart to overriding `cast/1`.
  defimpl JSON.Encoder, for: WithCustomEncoder do
    def encode(%{order_id: order_id}, encoder) do
      JSON.encode_to_iodata!(%{order_id: order_id}, encoder)
    end
  end

  defmodule WithCustomCast do
    @moduledoc false
    use DMEx.Message, type: "with_custom_cast", fields: [:order_id, :placed_at]

    @impl true
    def cast(data) when is_map(data) do
      %__MODULE__{
        order_id: data["order_id"],
        placed_at: data |> Map.fetch!("placed_at") |> DateTime.from_iso8601() |> elem(1)
      }
    end
  end

  # The entity axis. Two types concerning one order, so a cross-type entity query
  # has something to find, and three shapes of declared field between them: a
  # plain identifier, one that is often unset, and a list that fans out.
  defmodule EntityPlaced do
    @moduledoc false
    use DMEx.Message,
      type: "entity.placed",
      fields: [:order_id, :customer_id],
      entities: [:order_id, :customer_id]
  end

  defmodule EntityShipped do
    @moduledoc false
    use DMEx.Message,
      type: "entity.shipped",
      fields: [:order_id, :line_item_ids],
      entities: [:order_id, :line_item_ids]
  end

  # A type that has been through an expand-contract rename: rows exist in the log
  # under both strings, and this module answers for both.
  defmodule Renamed do
    @moduledoc false
    use DMEx.Message, type: "orders.placed", fields: [:order_id], retired: ["order_placed_legacy"]
  end
end
