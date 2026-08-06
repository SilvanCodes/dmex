defmodule DMEx.Handler.Subscriptions do
  @moduledoc """
  Owns one `DMEx.Handler.Subscription` process per live subscription.

  **Restart intensity is set never to trip, on purpose.** `on_failure: :crash`
  is specified to crash-loop on a poison message forever, and a
  `DynamicSupervisor`'s default three-in-five would blow in under a second —
  killing this supervisor and every sibling subscription with it. The rate
  limiting that matters is the jittered `reconnect_backoff_base` pause
  `DMEx.Handler.Pipeline` takes before re-raising, since a restart itself is
  immediate.
  """

  use DynamicSupervisor

  @doc false
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @impl DynamicSupervisor
  def init(_opts) do
    DynamicSupervisor.init(
      strategy: :one_for_one,
      max_restarts: 1_000_000,
      max_seconds: 1
    )
  end
end
