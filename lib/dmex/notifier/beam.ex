defmodule DMEx.Notifier.Beam do
  @moduledoc """
  Notification as BEAM messages, over `:pg` process groups.

  The default for engines the database cannot notify through, `DMEx.Storage.SQLite`
  among them, and available to any of them — a Postgres deployment behind a
  transaction-pooling pooler can take this instead of `LISTEN`/`NOTIFY`.

  The scope is per log rather than global, so two logs in one application do not
  contend for one process and cannot deliver into each other. Nothing needs
  configuring: `:pg` works on a single node, and reaches every connected node
  when the BEAM is clustered.

  **Reaching other nodes is the whole of what `:pg` buys here.** The only member
  of a scope's group is the log's own `DMEx.Log.Dispatcher`, a process registered
  under a name a plain `send/2` would reach — so on a single node this is an ETS
  lookup and a local send where a direct send would do, and anyone weighing `:pg`
  for a one-node deployment should read that as costing a little and buying
  nothing. It is here for the clustered case, where the group is the only thing
  that knows which nodes have a dispatcher.

  **What it does not reach** is a writer outside this cluster — another OS
  process, or a second BEAM opening the same SQLite file. Those appends surface
  through `DMEx.Log.Dispatcher`'s poll, at up to one poll interval of latency
  and no loss of correctness.
  """

  @behaviour DMEx.Notifier

  @impl DMEx.Notifier
  def child_specs(opts) do
    scope = scope(opts)

    [%{id: scope, start: {:pg, :start_link, [scope]}}]
  end

  @impl DMEx.Notifier
  def listen(opts) do
    scope = scope(opts)

    case Process.whereis(scope) do
      nil ->
        {:error, :not_running}

      pid ->
        # The scope, not a connection: it is what the group membership lives in,
        # so its death is what costs this subscription its notifications.
        with :ok <- safe_join(scope, Map.fetch!(opts, :channel)), do: {:ok, pid}
    end
  end

  @impl DMEx.Notifier
  def notify(type, opts) when is_binary(type) do
    scope = scope(opts)
    channel = Map.fetch!(opts, :channel)

    # Three of the five elements exist only because `c:DMEx.Notifier.listen/1`
    # fixes the delivered shape to the one Postgrex sends directly — see there
    # for why it is not normalised. The dispatcher reads none of them; the ref is
    # minted rather than faked with some other term because the contract types it
    # as a `reference()`, and one allocation per notify is cheaper than a
    # contract with an exception in it.
    ref = make_ref()

    # Cluster-wide: `get_members/2` returns remote members too, and a send to
    # one is an ordinary distributed send. It answers `[]` for a scope that is
    # not running, so there is nothing to guard.
    for pid <- :pg.get_members(scope, channel) do
      send(pid, {:notification, self(), ref, channel, type})
    end

    :ok
  end

  # `:pg.join/3` is a `gen_server` call, so it *exits* when the scope dies
  # between the lookup and the join — and the behaviour promises this is
  # best-effort. The same reasoning as `DMEx.Notifier.Postgres.safe_listen/2`.
  defp safe_join(scope, channel) do
    :pg.join(scope, channel, self())
  catch
    :exit, reason -> {:error, reason}
  end

  # Derived by the log, not here — see `t:DMEx.Notifier.opts/0`.
  defp scope(opts), do: Map.fetch!(opts, :name)
end
