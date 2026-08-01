defmodule DMEx.Handler.Supervisor do
  @moduledoc """
  Owns one `DMEx.Handler.Stream` per live subscription.

  **Restart intensity is set never to trip, on purpose.** `on_failure: :crash`
  is specified to crash-loop on a poison message forever, and a subscription can
  crash-loop for reasons no failure mode covers — a repo that is down, a log
  answering every connect with garbage. A `DynamicSupervisor`'s default budget
  of three restarts in five seconds is blown by either in under a second, and
  when it is, the *supervisor* dies and takes every other subscription in the
  application with it.

  So the rate limiting that matters lives inside `DMEx.Handler.Stream` instead,
  as jittered reconnect backoff before each connect. A crash-looping
  subscription slows itself down, and its siblings never notice.
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
