defmodule DMEx.Handler.Subscriptions do
  @moduledoc """
  Owns one `DMEx.Handler.Subscription` process per live subscription.

  The discovered ones are its **static children**: `DMEx.Supervisor.init/1`
  already knows every handler, every binding and the storage, so it builds their
  child specs directly rather than starting them from a process afterwards. That
  is what makes them restartable — a child list is the only thing a supervisor can
  put back — and what makes a discovered subscription that cannot start refuse the
  boot, which is what `DMEx.Supervisor` refuses for an unbound log and a duplicate
  id already.

  `DMEx.Supervisor.subscribe/2` adds one at runtime through
  `Supervisor.start_child/2`, so there is one home for a subscription rather than
  two, and `unsubscribe/2` reaches every subscription the same way.

  **Restart intensity is set never to trip, on purpose.** `on_failure: :crash` is
  specified to crash-loop on a poison message forever, and the default
  three-in-five would blow in under a second — killing this supervisor and every
  sibling subscription with it. The rate limiting that matters is the jittered
  `reconnect_backoff_base` pause `DMEx.Handler.Pipeline` takes before re-raising,
  since a restart itself is immediate.
  """

  use Supervisor

  @doc false
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    Supervisor.start_link(__MODULE__, Keyword.fetch!(opts, :children), name: Keyword.fetch!(opts, :name))
  end

  @impl Supervisor
  def init(children) do
    Supervisor.init(children, strategy: :one_for_one, max_restarts: 1_000_000, max_seconds: 1)
  end
end
