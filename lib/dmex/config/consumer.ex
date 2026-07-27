defmodule DMEx.Config.Consumer do
  @moduledoc """
  A subscription's resolved configuration.

  Built once by `DMEx.Config.consumer!/1` when a subscription starts. See
  `DMEx.Config` for what each field means and where its default came from.

  Note what is *not* here: the learned `batch_window`/`max_batch_size` an
  adaptive subscription converges on. Those are in-memory stream state and die
  with the process, deliberately — the right pacing is a property of the current
  host, load and handler, not of the subscription, and a value pinned during one
  bad afternoon would quietly throttle that subscription forever.
  """

  defstruct batch_window: 200,
            batch_window_ceiling: 5_000,
            max_batch_size: 500,
            queue_limit: nil,
            start: :beginning,
            on_failure: :crash,
            max_attempts: 3,
            retry_backoff: 100,
            retry_backoff_max: 5_000,
            reconnect_backoff: 500,
            reconnect_backoff_max: 30_000,
            max_frame_bytes: 4_194_304,
            missed_heartbeats: 3,
            adaptive: true,
            ewma_alpha: 0.2,
            min_samples: 20,
            sample_staleness: 60_000,
            recovery_windows: 3,
            recovery_percent: 25,
            reparameterize_interval: 30_000

  @type t :: %__MODULE__{
          batch_window: pos_integer(),
          batch_window_ceiling: pos_integer(),
          max_batch_size: pos_integer(),
          queue_limit: pos_integer() | nil,
          start: :beginning | :latest | non_neg_integer(),
          on_failure: :crash | :skip | :dead_message,
          max_attempts: pos_integer(),
          retry_backoff: pos_integer(),
          retry_backoff_max: pos_integer(),
          reconnect_backoff: pos_integer(),
          reconnect_backoff_max: pos_integer(),
          max_frame_bytes: pos_integer(),
          missed_heartbeats: pos_integer(),
          adaptive: boolean(),
          ewma_alpha: float(),
          min_samples: pos_integer(),
          sample_staleness: pos_integer(),
          recovery_windows: pos_integer(),
          recovery_percent: pos_integer(),
          reparameterize_interval: pos_integer()
        }
end
