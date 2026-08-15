defmodule DMEx.Log.Dispatcher do
  @moduledoc """
  Tells a log's live streams that something was appended.

  One per log, started by that log's supervisor. It subscribes to the log's
  `DMEx.Notifier` and runs a poll timer; on either signal it broadcasts a bare
  `:wakeup` across `DMEx.Log.Streams`, reading no rows and fanning out no data —
  each stream reads for itself. The notifier gives latency and the poll makes
  dispatch *robust*, so a notifier that is down, or `:unsupported`, needs no
  separate code path here: the poll runs alone.

  **The poll must not wake connections for nothing.** Unconditional broadcast
  re-creates a thundering herd: a thousand connections at a one-second interval
  is a thousand reads per second on a completely idle log. So the poll asks what
  has been appended past `polled_through`, its own cursor, and broadcasts only
  if anything was — one cheap query per interval per *app*.

  Both signals route: a wakeup goes only to connections whose selection contains
  the appended type, which is why the poll asks for distinct types and not just
  `max(id)`. Only the poll writes `polled_through`, so a log appending more than
  once per interval broadcasts twice — fine, since a spurious wakeup costs a read
  that finds nothing.
  """

  use GenServer

  alias DMEx.Log.Selection
  alias DMEx.Log.Status
  alias DMEx.Log.Streams
  alias DMEx.Notifier
  alias DMEx.Storage

  require Logger

  @doc "Starts a dispatcher for `log:`."
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
    %{storage: storage, notifier: notifier, config: config} = log.__dmex__()

    state = %{
      log: log,
      storage: storage,
      notifier: notifier,
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
    {:noreply, broadcast(state, drain_notifications([payload]))}
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
    case Notifier.listen(state.notifier) do
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
      "DMEx could not LISTEN on #{state.notifier.channel} for #{inspect(state.log)}. " <>
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
    streams = Streams.all(state.log)

    # Two gates answering different questions: `Selection.concerns?/2` whether
    # this append is any of this connection's business at all,
    # `Status.mark_pending/1` whether it already has a wakeup queued. The second
    # is what bounds a stalled connection's mailbox at one wakeup rather than at
    # the append rate times the stall duration, however long it stalls.
    woken =
      Enum.count(streams, fn {pid, status, selection} ->
        if Selection.concerns?(selection, types) and Status.mark_pending(status) do
          send(pid, :wakeup)

          true
        end
      end)

    :telemetry.execute(
      [:dmex, :dispatcher, :broadcast],
      %{streams: length(streams), woken: woken, types: length(types)},
      %{log: state.log}
    )

    state
  end

  # `{nil, []}` on an empty window and on an error alike: an engine that cannot
  # answer must not take the dispatcher down, and leaving `polled_through`
  # untouched means the next poll asks the same question over the same window.
  defp appended_since(%{storage: storage, polled_through: last}) do
    case Storage.appended_since(storage, last) do
      {:ok, {max_id, types}} -> {max_id, types}
      {:error, _reason} -> {nil, []}
    end
  end

  defp current_max_id(%{storage: storage, polled_through: last}) do
    case Storage.max_id(storage) do
      {:ok, max_id} -> max_id
      {:error, _reason} -> last
    end
  end

  defp schedule_poll(state), do: Process.send_after(self(), :poll, state.poll_interval)
end
