defmodule DMEx.Log.Dispatcher do
  @moduledoc """
  Tells a log's live streams that something was appended.

  One per log, started by that log's supervisor. It subscribes to the log's
  `DMEx.Notifier` and runs a poll timer; on either signal it broadcasts a bare
  `:wakeup` across `DMEx.Log.Registry`.

  **Belt and suspenders.** The notifier gives low latency; the poll makes
  dispatch *robust*. A missed or coalesced notification costs up to one poll
  interval of latency and never a stuck consumer — the same arrangement Oban's
  stager uses, and why a notifier that is down, or `:unsupported`, needs no
  separate code path here: the poll runs alone.

  **It never reads rows or fans out data.** Each stream reads for itself,
  bounded by its own `max_batch_size`, so this shared per-app process holds no
  message data on any connection's behalf. A notification carries a routing key
  and nothing else — never a range, an id, or a row. Missing a hint costs
  latency, never correctness, so the send loop stays correct with this process
  down.

  **The poll must not wake connections for nothing.** Unconditional broadcast
  re-creates a thundering herd: a thousand connections at a one-second interval
  is a thousand reads per second on a completely idle log. So the poll asks what
  has been appended past `polled_through`, its own cursor, and broadcasts only
  if anything was — one cheap query per interval per *app*, zero wakeups when
  nothing was appended.

  **Both signals route.** A wakeup goes only to connections whose selection
  contains the appended type; a whole-log subscription matches everything. That
  is why the poll asks for distinct types and not just `max(id)`. Only the poll
  writes `polled_through` — a notification carries no id to advance it with — so
  on a log appending more than once per interval the poll sees that movement
  again and broadcasts too. Duplicate wakeups are fine: a spurious one costs a
  read that finds nothing.
  """

  use GenServer

  alias DMEx.Log.Selection
  alias DMEx.Log.Status

  require Logger

  # Indices into the one `:counters` array a broadcast pass tallies into.
  @streams 1
  @woken 2

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
    %{
      storage: storage,
      storage_opts: storage_opts,
      notifier: notifier,
      notifier_opts: notifier_opts,
      config: config
    } = log.__dmex__()

    state = %{
      log: log,
      storage: storage,
      storage_opts: storage_opts,
      notifier: notifier,
      notifier_opts: notifier_opts,
      channel: notifier_opts.channel,
      poll_interval: config.poll_interval,
      polled_through: 0,
      notifier_ref: nil,
      warned_listen?: false,
      warned_unexpected?: false
    }

    # Seeded from the current tip, so a dispatcher starting against a log with
    # history does not announce it. In `init/1` so no append can land before the
    # baseline is taken; costs one index descent on a blocking start.
    {:ok, %{state | polled_through: current_max_id(state)}, {:continue, :listen}}
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

    # `c:DMEx.Storage.appended_since/2` answers strictly past the cursor it is
    # handed, and that cursor is `polled_through`, so there is no third case: a
    # non-nil `max_id` has moved by construction.
    case appended_since(state) do
      {nil, _types} -> {:noreply, state}
      {max_id, types} -> {:noreply, broadcast(%{state | polled_through: max_id}, types)}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{notifier_ref: ref} = state) do
    # The notification connection died. Polling already covers correctness, so
    # this is a latency regression until the next poll re-establishes LISTEN.
    {:noreply, %{state | notifier_ref: nil}}
  end

  @impl GenServer
  def handle_info(message, state), do: {:noreply, warn_unexpected(state, message)}

  ## Internals

  # Best-effort by design. An engine with no notification support, a notifier
  # that has not started, and one that just crashed are the same situation here:
  # run polling-only until one succeeds. None may take the dispatcher down,
  # because polling is what makes dispatch robust in the first place.
  defp ensure_listening(%{notifier_ref: ref} = state) when is_reference(ref), do: state

  defp ensure_listening(state) do
    case state.notifier.listen(state.notifier_opts) do
      {:ok, pid} ->
        %{state | notifier_ref: Process.monitor(pid), warned_listen?: false}

      # No notification mechanism at all, so polling-only is the design rather
      # than a degradation. Nothing to warn about.
      :unsupported ->
        state

      {:error, _reason} ->
        warn_listen(state)
    end
  end

  defp warn_listen(%{warned_listen?: true} = state), do: state

  defp warn_listen(state) do
    Logger.warning(
      "DMEx could not LISTEN on #{state.channel} for #{inspect(state.log)}. " <>
        "Falling back to polling every #{state.poll_interval}ms, which costs latency " <>
        "but never correctness."
    )

    %{state | warned_listen?: true}
  end

  # The other way to end up polling-only: a notifier whose message is not
  # `DMEx.Notifier`'s `{:notification, pid, ref, channel, type}` tuple. Its
  # `listen/1` succeeded, so nothing above notices, and silence would leave the
  # latency half of the design off with no way to tell. Once, because a
  # misbehaving notifier sends one of these per append.
  defp warn_unexpected(%{warned_unexpected?: true} = state, _message), do: state

  defp warn_unexpected(state, message) do
    Logger.warning(
      "DMEx dispatcher for #{inspect(state.log)} ignored an unexpected message: " <>
        "#{inspect(message)}. A notifier must send DMEx.Notifier's " <>
        "{:notification, pid, ref, channel, type} tuple. Polling continues regardless, " <>
        "so this costs latency but never correctness."
    )

    %{state | warned_unexpected?: true}
  end

  defp drain_notifications(types) do
    receive do
      {:notification, _pid, _ref, _channel, payload} -> drain_notifications([payload | types])
    after
      0 -> Enum.uniq(types)
    end
  end

  defp broadcast(state, types) do
    # Both telemetry measurements come out of the one walk. `streams` used to be
    # a second pass over the same entries, and `DMEx.Log.Registry.count/1` was
    # the expensive spelling of it.
    tally = :counters.new(2, [])

    # Two gates, and they answer different questions. `Selection.concerns?/2`
    # asks whether this append is any of this connection's business at all;
    # `Status.flip/1` asks whether it already has a wakeup queued. The second is
    # what bounds a stalled connection's mailbox at one wakeup rather than at the
    # append rate times the stall duration, however long it stalls.
    DMEx.Log.Registry.each(state.log, fn pid, status, selection ->
      :counters.add(tally, @streams, 1)

      if Selection.concerns?(selection, types) and Status.flip(status) do
        send(pid, :wakeup)
        :counters.add(tally, @woken, 1)
      end
    end)

    :telemetry.execute(
      [:dmex, :dispatcher, :broadcast],
      %{
        streams: :counters.get(tally, @streams),
        woken: :counters.get(tally, @woken),
        types: length(types)
      },
      %{log: state.log}
    )

    state
  end

  # `{nil, []}` on an empty window and on an error alike: an engine that cannot
  # answer must not take the dispatcher down, and leaving `polled_through`
  # untouched means the next poll asks the same question over the same window.
  defp appended_since(%{storage: storage, storage_opts: opts, polled_through: last}) do
    case storage.appended_since(opts, last) do
      {:ok, {max_id, types}} -> {max_id, types}
      {:error, _reason} -> {nil, []}
    end
  end

  defp current_max_id(%{storage: storage, storage_opts: opts, polled_through: last}) do
    case storage.max_id(opts) do
      {:ok, max_id} -> max_id
      {:error, _reason} -> last
    end
  end

  defp schedule_poll(state), do: Process.send_after(self(), :poll, state.poll_interval)
end
