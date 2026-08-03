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
        :ok = :pg.join(scope, Map.fetch!(opts, :channel), self())

        # The scope, not a connection: it is what the group membership lives in,
        # so its death is what costs this subscription its notifications.
        {:ok, pid}
    end
  end

  @impl DMEx.Notifier
  def notify(opts, type) when is_binary(type) do
    scope = scope(opts)
    channel = Map.fetch!(opts, :channel)

    if Process.whereis(scope) do
      ref = make_ref()

      # Cluster-wide: `get_members/2` returns remote members too, and a send to
      # one is an ordinary distributed send.
      for pid <- :pg.get_members(scope, channel) do
        send(pid, {:notification, self(), ref, channel, type})
      end
    end

    :ok
  end

  defp scope(opts), do: opts |> Map.fetch!(:log) |> Module.concat("Notifications")
end
