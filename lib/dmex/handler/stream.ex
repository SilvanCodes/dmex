defmodule DMEx.Handler.Stream do
  @moduledoc """
  One subscription: one transport, one handler, one cursor.

  Reads its durable `last_seen`, asks its `DMEx.Transport` to open at that
  position, and runs every row the transport yields through
  `DMEx.Handler.Pipeline`, in order, one at a time.

  ## Resumption is entirely cursor-driven

  On every connect — first or thousandth — the stream reads its durable
  `last_seen` and hands it to the transport. It never resumes from the last row
  it saw and never trusts anything in memory, which is what makes a dropped
  connection a non-event. `stream_from:` applies only when there is no cursor at
  all, and is resolved once in `init/1` so every connect until then agrees.

  ## Reconnects are jittered

  `DMEx.Handler.Supervisor` sets no restart budget, so the pacing is here: each
  restart triggers a fresh read against a *different application's* database
  when the transport is a wire.

  One retry policy covers every transport: a transport says only whether an
  event ends this connection (`{:disconnect, reason, source}`) or the
  subscription (`{:stop, reason}`), and when to try again is decided here.

  ## Liveness

  A connection can die without either end noticing — a vanished peer, a NAT
  rebind, a dropped flow — and simply never deliver anything again.
  `c:DMEx.Transport.liveness/1` is how long total silence may last before this
  process gives up; `:infinity` arms no timer at all.

  Silence means *no bytes at all*, not no messages. A heartbeat is activity, so
  a caught-up stream on an idle log stays healthy indefinitely.

  ## Skips are checkpointed on a timer, not per row

  `DMEx.Handler.Pipeline` defers a skipped row's cursor advance. This process
  settles it: a `checkpoint_interval` timer, armed per process rather than per
  connection, plus a flush before a reconnect (which re-reads the durable
  cursor) and in `terminate/2` (so a clean stop does not discard skips).

  The `terminate/2` half only works because this process **traps exits**: a
  non-trapping child of a `DynamicSupervisor` is killed outright by
  `terminate_child/2` and the callback never runs. Trapping is also what turns a
  linked reader's death into an ordinary reconnect.

  ## Backpressure is the absence of a buffer

  Rows run through the pipeline **inline**, one at a time, as the transport
  yields them: no queue, no drain loop, at most one batch in flight —
  ARCHITECTURE invariant 8.
  """

  use GenServer

  alias DMEx.Handler.Pipeline

  require Logger

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :subscription)},
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent
    }
  end

  @impl GenServer
  def init(opts) do
    subscription = Keyword.fetch!(opts, :subscription)

    Process.flag(:trap_exit, true)

    # Before anything opens, which is what enforces one live stream per
    # subscription id: a second subscribe/2 for the same identity fails in init
    # rather than after both have started clobbering one cursor.
    case register(opts[:registry], subscription) do
      {:ok, _} ->
        state = %{
          opts: opts,
          transport: Keyword.fetch!(opts, :transport),
          # Both, deliberately: the id is the durable identity everything is
          # keyed by, the module is what a human reads in a log line.
          handler: Keyword.fetch!(opts, :handler),
          subscription: subscription,
          # Where the rows come from, for the operator: a URL, or the module of
          # a log this application owns.
          origin: Keyword.fetch!(opts, :origin),
          config: resolve_stream_from(Keyword.fetch!(opts, :config)),
          pipeline: Pipeline.build(opts),
          storage: Keyword.fetch!(opts, :storage),
          storage_opts: Keyword.fetch!(opts, :storage_opts),
          source: nil,
          last_activity: nil,
          liveness_timer: nil,
          cursor: nil,
          attempt: 0
        }

        # Per process rather than per connection: a pending run of skips
        # outlives a disconnect.
        {:ok, schedule_checkpoint(state), {:continue, :connect}}

      {:error, {:already_registered, _pid}} ->
        {:stop, {:already_subscribed, subscription}}
    end
  end

  @impl GenServer
  def handle_continue(:connect, state), do: connect(state)

  @impl GenServer
  def handle_info(:connect, state), do: connect(state)

  @impl GenServer
  def handle_info(:checkpoint, state) do
    {:noreply, state |> flush() |> schedule_checkpoint()}
  end

  @impl GenServer
  def handle_info(:liveness, %{source: nil} = state), do: {:noreply, state}

  @impl GenServer
  def handle_info(:liveness, state) do
    silent_for = System.monotonic_time(:millisecond) - state.last_activity

    if silent_for > state.transport.liveness(state.source) do
      # Nothing ever arrived and nothing ever closed — the shape a vanished peer
      # takes. Dropping costs a re-read from the durable cursor.
      {:noreply, reconnect(state, {:silent_for, silent_for})}
    else
      {:noreply, schedule_liveness(state)}
    end
  end

  @impl GenServer
  def handle_info(message, %{source: source} = state) when not is_nil(source) do
    drive(message, state)
  end

  @impl GenServer
  def handle_info(_message, state), do: {:noreply, state}

  # One process message can carry more than one thing to act on — a batch of
  # rows and then the connection ending. `:continue` asks the transport for
  # whatever is left once the rows are run, so a terminal event never discards
  # the rows that preceded it.
  defp drive(message, state) do
    case state.transport.handle_message(message, state.source) do
      :unknown ->
        {:noreply, state}

      {:noreply, source} ->
        {:noreply, touch(%{state | source: source})}

      {:rows, rows, source} ->
        state = touch(%{state | source: source})

        drive(:continue, Enum.reduce(rows, state, &run/2))

      {:disconnect, reason, source} ->
        {:noreply, reconnect(%{state | source: source}, reason)}

      {:stop, reason} ->
        {:stop, reason, state}
    end
  end

  # The local transport's handoff: running the batch inside the call is what
  # makes the reader block until it is done.
  @impl GenServer
  def handle_call({:rows, rows}, _from, state) do
    state = touch(state)

    {:reply, :ok, Enum.reduce(rows, state, &run/2)}
  end

  # A clean stop should not throw away skips already made. Rescued because the
  # repo's pool may already be on its way down, and re-skipping is the ordinary
  # cost of a pending run rather than something worth a noisy shutdown crash.
  @impl GenServer
  def terminate(_reason, state) do
    try do
      flush(state)
    rescue
      _ -> :ok
    end

    close(state)

    :ok
  end

  ## Connecting

  # Once per process, not once per connect. `stream_from` applies only until the
  # first message is handled, and on a quiet log that window is days — so
  # re-reading the clock on every reconnect would silently skip whatever arrived
  # while this stream was down. Freezing it here means a reconnect resolves to
  # the same position the first connect did.
  #
  # It also empties the atom out of everything downstream: past this line a
  # transport sees `pos_integer() | DateTime.t()` and nothing else.
  #
  # A process *restart* does re-read the clock. Closing that too would mean
  # persisting the resolved position before any message is handled, which
  # `dmex_cursors.last_seen` has nowhere to put and the SSE consumer could not
  # compute anyway — the log resolves it, and the wire carries no `max_id`.
  defp resolve_stream_from(%{stream_from: :current} = config) do
    %{config | stream_from: DateTime.utc_now()}
  end

  defp resolve_stream_from(config), do: config

  defp connect(state) do
    with {:ok, cursor} <- durable_cursor(state),
         {:ok, source} <- state.transport.connect(connect_opts(state, cursor)) do
      {:noreply,
       %{
         state
         | source: source,
           cursor: cursor,
           # The durable cursor just read is the authority, so anything still
           # marked pending was either already written or is about to be
           # re-delivered and re-skipped. Either way it is not owed a write.
           pipeline: %{state.pipeline | pending: false},
           attempt: 0
       }
       |> touch()}
    else
      {:error, reason} -> {:noreply, reconnect(state, reason)}
    end
  end

  # The whole option list plus where to resume: a transport takes what it needs
  # and ignores the rest, so adding one costs no plumbing here.
  defp connect_opts(state, cursor) do
    # `config:` is overridden rather than taken from `opts`, which still holds
    # the declared one: `stream_from: :current` was resolved to an instant in
    # `init/1`, and a transport reading around that would re-read the clock on
    # every reconnect.
    Keyword.merge(state.opts, cursor: cursor, owner: self(), config: state.config)
  end

  # `:none` rather than 0 for "never committed anything" — that distinction is
  # what makes `stream_from:` apply exactly once.
  defp durable_cursor(state) do
    case state.storage.last_seen(state.storage_opts, state.subscription) do
      {:ok, nil} -> {:ok, :none}
      {:ok, cursor} -> {:ok, cursor}
      {:error, reason} -> {:error, reason}
    end
  end

  defp close(%{source: nil}), do: :ok
  defp close(state), do: state.transport.close(state.source)

  ## Running

  defp run(row, state) do
    {:ok, pipeline} = Pipeline.run(row, %{state.pipeline | cursor: cursor_of(state)})

    %{state | pipeline: pipeline, cursor: pipeline.cursor}
  end

  defp cursor_of(%{cursor: :none}), do: 0
  defp cursor_of(%{cursor: cursor}), do: cursor

  ## Reconnecting

  defp reconnect(state, reason) do
    # Before anything else: the next connect resumes from the *durable* cursor,
    # so an unwritten run of skips would be re-read and re-skipped for nothing.
    state = flush(state)

    close(state)

    if state.liveness_timer, do: Process.cancel_timer(state.liveness_timer)

    attempt = state.attempt + 1

    delay =
      DMEx.backoff(attempt, state.config.reconnect_backoff, state.config.reconnect_backoff_max)

    Logger.debug(fn ->
      "DMEx #{state.subscription} (#{inspect(state.handler)}) reconnecting to " <>
        "#{inspect(state.origin)} in #{delay}ms after #{inspect(reason)}"
    end)

    :telemetry.execute(
      [:dmex, :handler, :reconnect],
      %{attempt: attempt, delay: delay},
      %{
        subscription: state.subscription,
        handler: state.handler,
        origin: state.origin,
        reason: reason
      }
    )

    Process.send_after(self(), :connect, delay)

    %{state | source: nil, liveness_timer: nil, attempt: attempt}
  end

  ## Checkpointing

  defp flush(state) do
    {:ok, pipeline} = Pipeline.flush(state.pipeline)

    %{state | pipeline: pipeline}
  end

  # No reference kept, unlike `:liveness_timer`: this timer is never cancelled
  # or rearmed early, and `handle_info(:checkpoint, ...)` is correct whenever it
  # lands — including after a reconnect, since a pending run of skips outlives
  # one.
  defp schedule_checkpoint(state) do
    Process.send_after(self(), :checkpoint, state.config.checkpoint_interval)

    state
  end

  ## Liveness

  defp touch(state) do
    # Re-armed on every sign of life rather than only on connect, because the
    # deadline itself can change: an SSE transport does not know the log's
    # advertised interval until the log has spoken.
    state
    |> Map.put(:last_activity, System.monotonic_time(:millisecond))
    |> schedule_liveness()
  end

  # Checked twice per deadline, so a dead connection is noticed within about
  # half an interval. The floor keeps a transport with a very short interval
  # from turning this into a busy loop; `:infinity` arms nothing.
  defp schedule_liveness(%{source: nil} = state), do: state

  defp schedule_liveness(state) do
    if state.liveness_timer, do: Process.cancel_timer(state.liveness_timer)

    case state.transport.liveness(state.source) do
      :infinity ->
        %{state | liveness_timer: nil}

      deadline ->
        interval = max(div(deadline, 2), 250)

        %{state | liveness_timer: Process.send_after(self(), :liveness, interval)}
    end
  end

  defp register(nil, _subscription), do: {:ok, self()}

  defp register(registry, subscription) do
    Registry.register(registry, {:subscription, subscription}, nil)
  end
end
