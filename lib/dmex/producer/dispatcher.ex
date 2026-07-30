defmodule DMEx.Producer.Dispatcher do
  @moduledoc """
  Tells a producer's live streams that something was published.

  One per producer, started by that producer's supervisor. It does two things
  and nothing else: it holds the `LISTEN` connection, and it runs a poll timer.
  On either signal it broadcasts a bare `:wakeup` across `DMEx.Producer.Registry`.

  ## Belt and suspenders

  `NOTIFY` gives low latency; the poll makes dispatch *robust*. A missed or
  coalesced notification — a connection blip, a restarted listener — costs up to
  one poll interval of latency and never a stuck consumer. This is the same
  arrangement Oban's stager uses, and it is why a storage engine with no
  notification support needs no separate code path: it simply runs the poll
  alone.

  ## What it does not do

  It does not read rows, and it does not fan out data. Each stream reads for
  itself, bounded by its own `max_batch_size`, so this one shared per-app
  process never holds message data on any connection's behalf.

  A notification carries **a routing key and nothing else — never a range, an
  id, or a row.** The `message_type` says which connections could possibly care;
  it never says what to do about it. Each loop still reads from its own cursor
  to whatever is there now, the cursor in each stream is still the sole
  authority, and this is still only ever a hint that work may exist. Missing a
  hint costs latency, never correctness, which is why the producer's send loop
  stays correct even with this process down.

  ## The poll must not wake connections for nothing

  A poll that unconditionally broadcasts re-creates a thundering herd: at a
  thousand connections and a one-second interval, a thousand reads per second on
  a completely idle log. So the poll asks what has been published since the
  highest id already announced, and broadcasts only if that moved. One cheap
  query per interval per *app*, and zero wakeups when nothing was published.

  ## Both signals route, and they have to

  A wakeup goes only to connections whose selection contains the published type
  (`DMEx.Producer.Registry`); a whole-log subscription matches everything.

  That is why the poll asks for the distinct types and not just `max(id)`. The
  notification path does not advance `last_announced` — it cannot, since it is
  handed no id — so on any log publishing more than once per interval the poll
  independently sees movement and broadcasts too. Were that broadcast unrouted,
  it would wake every connection every second regardless of selection, and the
  routing would buy nothing on exactly the busy logs where it matters. Both
  paths route, or neither does.

  Duplicate wakeups across the two paths are fine and always were: a spurious
  wakeup costs one read that finds nothing.
  """

  use GenServer

  alias DMEx.Producer.Status

  require Logger

  @doc """
  Starts a dispatcher for `producer:`.

  Started automatically by the producer's own supervisor; you should not need to
  start one yourself.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    producer = Keyword.fetch!(opts, :producer)

    GenServer.start_link(__MODULE__, producer, name: name(producer))
  end

  @doc "The dispatcher name for a producer module."
  @spec name(module()) :: atom()
  def name(producer) when is_atom(producer), do: Module.concat(producer, "Dispatcher")

  @impl GenServer
  def init(producer) do
    %{storage: storage, storage_opts: storage_opts, config: config} = producer.__dmex__()

    state = %{
      producer: producer,
      storage: storage,
      storage_opts: storage_opts,
      channel: storage_opts.channel,
      poll_interval: config.poll_interval,
      last_announced: 0,
      notifier_ref: nil,
      warned?: false
    }

    # Seeded from the current tip rather than from 0, so a dispatcher starting
    # against a log with history does not announce that history as new.
    #
    # This happens in init/1, before start_link/1 returns, and that placement is
    # load-bearing. Seeding in handle_continue/2 leaves a window between "the
    # dispatcher is started" and "the baseline is taken": a publish landing in
    # it is read into the baseline and therefore never announced, so already
    # connected streams wait out a full heartbeat instead of being woken. It
    # costs one index descent on a blocking start.
    {:ok, %{state | last_announced: current_max_id(state)}, {:continue, :listen}}
  end

  @impl GenServer
  def handle_continue(:listen, state) do
    schedule_poll(state)

    {:noreply, ensure_listening(state)}
  end

  @impl GenServer
  def handle_info({:notification, _pid, _ref, _channel, payload}, state) do
    # Collapse a burst of publishes into one broadcast pass. The payloads are
    # accumulated rather than discarded, because the union of the types in a
    # burst is exactly the routing key set for the one pass that replaces it.
    types = drain_notifications([payload])

    {:noreply, broadcast(state, types)}
  end

  @impl GenServer
  def handle_info(:poll, state) do
    schedule_poll(state)

    # The poll doubles as the retry for LISTEN, so no second timer is needed and
    # a notifier that starts late or restarts is picked up within one interval.
    state = ensure_listening(state)

    case published_since(state) do
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

  # Establishing LISTEN is best-effort by design. A storage engine with no
  # notification support, a notifier that has not started yet, and a notifier
  # that just crashed are all the same situation to this process: run
  # polling-only until one succeeds. None of them may take the dispatcher down,
  # because polling is the mechanism that makes dispatch robust in the first
  # place.
  defp ensure_listening(%{notifier_ref: ref} = state) when is_reference(ref), do: state

  defp ensure_listening(state) do
    case state.storage.listen(state.storage_opts) do
      {:ok, pid} ->
        %{state | notifier_ref: Process.monitor(pid), warned?: false}

      # This engine has no notification mechanism at all, so polling-only is the
      # design rather than a degradation. Nothing to warn about.
      :unsupported ->
        state

      {:error, _reason} ->
        warn_once(state)
    end
  end

  defp warn_once(%{warned?: true} = state), do: state

  defp warn_once(state) do
    Logger.warning(
      "DMEx could not LISTEN on #{state.channel} for #{inspect(state.producer)}. " <>
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
    # this publish is any of this connection's business at all; `Status.flip/1`
    # asks whether it already has a wakeup queued. The second is what bounds a
    # stalled connection's mailbox at one wakeup rather than at the publish rate
    # times the stall duration, however long it stalls.
    DMEx.Producer.Registry.each(state.producer, fn pid, status, selection ->
      if concerns?(selection, types) and Status.flip(status) do
        send(pid, :wakeup)
        :counters.add(woken, 1, 1)
      end
    end)

    :telemetry.execute(
      [:dmex, :dispatcher, :broadcast],
      %{
        streams: DMEx.Producer.Registry.count(state.producer),
        woken: :counters.get(woken, 1),
        # A count, so it is a measurement. It sat in metadata under the same name
        # the *list* uses everywhere else in this module, which read as the list.
        types: length(types)
      },
      %{producer: state.producer}
    )

    state
  end

  # A whole-log subscription is concerned with every publish, which is what
  # keeps a bare `EventSource` working exactly as it did before routing existed.
  defp concerns?(:all, _types), do: true
  defp concerns?(selection, types), do: Enum.any?(types, &MapSet.member?(selection, &1))

  # `{nil, []}` on an empty window, and on an error: a storage engine that
  # cannot answer must not be reported as "nothing was published", but it also
  # must not take the dispatcher down. Leaving `last_announced` untouched means
  # the next poll asks the same question over the same window.
  defp published_since(%{storage: storage, storage_opts: opts, last_announced: last}) do
    case storage.published_since(opts, last) do
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
