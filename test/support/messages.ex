defmodule DMEx.Test.Messages do
  @moduledoc "Message modules used across the test suites."

  defmodule OrderPlaced do
    @moduledoc false
    use DMEx.Message, [:order_id, :customer_id, :total]
  end

  defmodule OrderShipped do
    @moduledoc false
    use DMEx.Message, [:order_id, carrier: "unknown"]
  end

  defmodule WithCustomCast do
    @moduledoc false
    use DMEx.Message, [:order_id, :placed_at]

    @impl true
    def cast(data) when is_map(data) do
      %__MODULE__{
        order_id: data["order_id"],
        placed_at: data |> Map.fetch!("placed_at") |> DateTime.from_iso8601() |> elem(1)
      }
    end
  end
end
