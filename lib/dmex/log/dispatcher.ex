defmodule DMEx.Log.Dispatcher do
  @moduledoc """
  Tells a log's live streams that something was appended.

  One per log, started by that log's supervisor. It subscribes to the adapter's
  notification connection and runs a poll timer; on either signal it broadcasts
  a bare `:wakeup` across `DMEx.Log.Registry`.

  **Belt and suspenders.** `NOTIFY` gives low latency; the poll makes dispatch
  *robust*. A missed or coalesced notification costs up to one poll interval of
  latency and never a stuck consumer — the same arrangement Oban's stager uses,
  and why a storage engine with no notification support needs no separate code
  path: it runs the poll alone.

  **It never reads rows or fans out data.** Each stream reads for itself,
  bounded by its own `max_batch_size`, so this shared per-app process holds no
  message data on any connection's behalf. A notification carries a routing key
  and nothing else — never a range, an id, or a row. Missing a hint costs
  latency, never correctness, so the send loop stays correct with this process
  down.

  **The poll must not wake connections for nothing.** Unconditional broadcast
  re-creates a thundering herd: a thousand connections at a one-second interval
  is a thousand reads per second on a completely idle log. So the poll asks what
  has been appended since the highest id already announced and broadcasts only
  if that moved — one cheap query per interval per *app*, zero wakeups when
  nothing was appended.

  **Both signals route.** A wakeup goes only to connections whose selection
  contains the appended type; a whole-log subscription matches everything. That
  is why the poll asks for distinct types and not just `max(id)`: the
  notification path cannot advance `last_announced`, since it is handed no id,
  so on a log appending more than once per interval the poll independently sees
  movement and broadcasts too. Duplicate wakeups are fine: a spurious one costs
  a read that finds nothing.
  """

  use GenServer

  alias DMEx.Log.Status

  require Logger

  @doc """
  Starts a dispatcher for `log:`.

  Done automatically by the log's own supervisor.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    log = Keyword.fetch!(opts, :log)

    GenServer.start_link(__MODULE__, log, name: name(log))
  end

  @doc "The dispatcher name for a log module."
  @spec name(module()) :: atom()
  def name(log) when is_atom(log), do: Module.concat(log, "Dispatcher")

  @impl GenServer
  def init(log) do
    %{storage: storage, storage_opts: storage_opts, config: config} = log.__dmex__()

    state = %{
      log: log,
      storage: storage,
      storage_opts: storage_opts,
      channel: storage_opts.channel,
      poll_interval: config.poll_interval,
      last_announced: 0,
      notifier_ref: nil,
      warned?: false
    }

    # Seeded from the current tip, so a dispatcher starting against a log with
    # history does not announce it. In `init/1` so no append can land before the
    # baseline is taken; costs one index descent on a blocking start.
    {:ok, %{state | last_announced: current_max_id(state)}, {:continue, :listen}}
  end

  @impl GenServer
  def handle_continue(:listen, state) do
    schedule_poll(state)

    {:noreply, ensure_listening(state)}
  end

  @impl GenServer
  def handle_info({:notification, _pid, _ref, _channel, payload}, state) do
    # Collapse a burst of appends into one broadcast pass. Payloads accumulate
    # rather than being discarded, because the union of the types in a burst is
    # exactly the routing key set for the one pass that replaces it.
    types = drain_notifications([payload])

    {:noreply, broadcast(state, types)}
  end

  @impl GenServer
  def handle_info(:poll, state) do
    schedule_poll(state)

    # The poll doubles as the retry for LISTEN, so no second timer is needed and
    # a notifier that starts late or restarts is picked up within one interval.
    state = ensure_listening(state)

    case appended_since(state) do
      {nil, _types} ->
        {:noreply, state}

      {max_id, types} when max_id > state.last_announced ->
        {:noreply, broadcast(%{state | last_announced: max_id}, types)}

      _ ->
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{notifier_ref: ref} = state) do
    # The notification connection died. Polling already covers correctness, so
    # this is a latency regression until the next poll re-establishes LISTEN.
    {:noreply, %{state | notifier_ref: nil}}
  end

  @impl GenServer
  def handle_info(_message, state), do: {:noreply, state}

  ## Internals

  # Best-effort by design. An engine with no notification support, a notifier
  # that has not started, and one that just crashed are the same situation here:
  # run polling-only until one succeeds. None may take the dispatcher down,
  # because polling is what makes dispatch robust in the first place.
  defp ensure_listening(%{notifier_ref: ref} = state) when is_reference(ref), do: state

  defp ensure_listening(state) do
    case state.storage.listen(state.storage_opts) do
      {:ok, pid} ->
        %{state | notifier_ref: Process.monitor(pid), warned?: false}

      # No notification mechanism at all, so polling-only is the design rather
      # than a degradation. Nothing to warn about.
      :unsupported ->
        state

      {:error, _reason} ->
        warn_once(state)
    end
  end

  defp warn_once(%{warned?: true} = state), do: state

  defp warn_once(state) do
    Logger.warning(
      "DMEx could not LISTEN on #{state.channel} for #{inspect(state.log)}. " <>
        "Falling back to polling every #{state.poll_interval}ms, which costs latency " <>
        "but never correctness."
    )

    %{state | warned?: true}
  end

  defp drain_notifications(types) do
    receive do
      {:notification, _pid, _ref, _channel, payload} -> drain_notifications([payload | types])
    after
      0 -> Enum.uniq(types)
    end
  end

  defp broadcast(state, types) do
    woken = :counters.new(1, [])

    # Two gates, and they answer different questions. `concerns?/2` asks whether
    # this append is any of this connection's business at all; `Status.flip/1`
    # asks whether it already has a wakeup queued. The second is what bounds a
    # stalled connection's mailbox at one wakeup rather than at the append rate
    # times the stall duration, however long it stalls.
    DMEx.Log.Registry.each(state.log, fn pid, status, selection ->
      if concerns?(selection, types) and Status.flip(status) do
        send(pid, :wakeup)
        :counters.add(woken, 1, 1)
      end
    end)

    :telemetry.execute(
      [:dmex, :dispatcher, :broadcast],
      %{
        streams: DMEx.Log.Registry.count(state.log),
        woken: :counters.get(woken, 1),
        types: length(types)
      },
      %{log: state.log}
    )

    state
  end

  # A whole-log subscription is concerned with every append.
  defp concerns?(:all, _types), do: true
  defp concerns?(selection, types), do: Enum.any?(types, &MapSet.member?(selection, &1))

  # `{nil, []}` on an empty window and on an error alike: an engine that cannot
  # answer must not take the dispatcher down, and leaving `last_announced`
  # untouched means the next poll asks the same question over the same window.
  defp appended_since(%{storage: storage, storage_opts: opts, last_announced: last}) do
    case storage.appended_since(opts, last) do
      {:ok, {max_id, types}} -> {max_id, types}
      {:error, _reason} -> {nil, []}
    end
  end

  defp current_max_id(%{storage: storage, storage_opts: opts, last_announced: last}) do
    case storage.max_id(opts) do
      {:ok, max_id} -> max_id
      {:error, _reason} -> last
    end
  end

  defp schedule_poll(state), do: Process.send_after(self(), :poll, state.poll_interval)
end
