defmodule DMEx.Config.Producer do
  @moduledoc """
  A producer's resolved configuration.

  Built once by `DMEx.Config.producer!/1` and carried in the producer module's
  compile-time `__dmex__/0` map. See `DMEx.Config` for what each field means and
  where its default came from.
  """

  defstruct batch_window: 200,
            max_batch_size: 500,
            batch_window_floor: 50,
            batch_window_ceiling: 5_000,
            max_batch_size_floor: 1,
            max_batch_size_ceiling: 5_000,
            heartbeat_ms: 15_000,
            poll_interval: 1_000,
            frame_max_bytes: 1_048_576

  @type t :: %__MODULE__{
          batch_window: pos_integer(),
          max_batch_size: pos_integer(),
          batch_window_floor: pos_integer(),
          batch_window_ceiling: pos_integer(),
          max_batch_size_floor: pos_integer(),
          max_batch_size_ceiling: pos_integer(),
          heartbeat_ms: pos_integer(),
          poll_interval: pos_integer(),
          frame_max_bytes: pos_integer()
        }
end
