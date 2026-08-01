defmodule DMEx.Handler.Stream do
  @moduledoc """
  One subscription: one transport, one handler, one cursor.

  Reads its durable `last_seen`, asks its `DMEx.Transport` to open at that
  position, and runs every row the transport yields through
  `DMEx.Handler.Pipeline`, in order, one at a time.

  What the transport is never appears here: a log bound to a URL gets
  `DMEx.Transport.SSE` and a socket, one bound to a module gets
  `DMEx.Transport.Local` and a reader in this VM. Everything below is the same
  either way, which is the point of the split — connecting, framing, decoding
  and liveness are protocol facts, while a cursor, a pipeline and a checkpoint
  are not.

  ## Resumption is entirely cursor-driven

  On every connect — first or thousandth — the stream reads its durable
  `last_seen` and hands it to the transport. It never resumes from the last
  row it saw, and it never trusts anything it holds in memory. That is what
  makes a dropped connection a non-event: nothing is lost and nothing is
  skipped, because the log holds no handler state and resumption is the
  same `id > cursor` read a cold connect does.

  `start:` only applies when there is no cursor at all. Once one exists it is
  ignored entirely, because the durable position is always the authority.

  ## Reconnects are jittered, and that matters more than it looks

  Backoff lives here rather than in the supervisor, so a subscription that
  crash-loops on a poison message does so at backoff pace instead of as fast as
  a `DynamicSupervisor` will restart it. Each restart reopens the connection and
  triggers a fresh log-side read — against a *different application's* database
  when the transport is a wire — so an un-backed-off crash-loop is something
  DMEx would be inflicting on someone else.

  One retry policy covers every transport. A transport says only whether an
  event ends this connection (`{:disconnect, reason, source}`) or the
  subscription (`{:stop, reason}`); when to try again is decided here.

  ## Liveness

  A connection can die without either end noticing: the peer vanishes, a NAT
  rebinds, an intermediary drops the flow, and the socket simply never delivers
  anything again. `c:DMEx.Transport.liveness/1` is how long total silence may
  last before this process gives up on it — and `:infinity` from a transport
  with nothing to lose means no timer is armed at all.

  Silence means *no bytes at all*, not no messages. A heartbeat is activity, so
  a caught-up stream on an idle log stays healthy indefinitely.

  ## Skips are checkpointed on a timer, not per row

  A skipped row runs no handler, so `DMEx.Handler.Pipeline` defers its cursor
  advance instead of committing one per row. This process owns settling that:
  a `checkpoint_interval` timer, armed for the process's whole life rather than
  per connection, plus a flush before each of the two points that re-read the
  durable cursor — a reconnect and `terminate/2`. Without those a run of skips
  would be re-read and re-skipped for nothing on the next connect.

  The `terminate/2` half only works because this process **traps exits**: a
  non-trapping child of a `DynamicSupervisor` is killed outright by
  `terminate_child/2`, and the callback never runs. Trapping is also what turns
  a linked reader's death into an ordinary reconnect.

  ## Backpressure is the absence of a buffer

  Rows are run through the pipeline **inline**, one at a time, as the transport
  yields them. There is no queue and no drain loop, and the transport's contract
  is that producing the next batch is blocked until this one is done — the
  socket's receive window for `DMEx.Transport.SSE`, a synchronous call for
  `DMEx.Transport.Local`. The most that can be in flight is one batch.

  A buffer would only have relocated the problem: a few hundred rows of slack
  absorb nothing at a sustained mismatch, and every row of slack is one more row
  the reader keeps accepting before it says stop.
  """

  use GenServer

  alias DMEx.Backoff
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

    # Without this the flush in `terminate/2` is dead code: a non-trapping child
    # of a `DynamicSupervisor` is killed outright by `terminate_child/2` and on
    # shutdown, so the callback never runs and a pending run of skips is lost.
    # It is also what delivers a linked reader's exit as a message.
    Process.flag(:trap_exit, true)

    # Registering here — before anything opens — is what enforces one live
    # stream per subscription id: a second subscribe/2 for the same identity
    # fails in init rather than after both have started clobbering one cursor.
    case register(opts[:registry], subscription) do
      {:ok, _} ->
        state = %{
          opts: opts,
          transport: Keyword.fetch!(opts, :transport),
          # Both, deliberately. The id is the durable identity everything is
          # keyed by; the module is what invokes the callback and what a human
          # reads in a log line.
          handler: Keyword.fetch!(opts, :handler),
          subscription: subscription,
          # Where the rows come from, for the operator rather than for the code:
          # a URL, or the module of a log this application owns.
          origin: Keyword.fetch!(opts, :origin),
          config: Keyword.fetch!(opts, :config),
          pipeline: Pipeline.build(opts),
          storage: Keyword.fetch!(opts, :storage),
          storage_opts: Keyword.fetch!(opts, :storage_opts),
          source: nil,
          last_activity: nil,
          liveness_timer: nil,
          cursor: nil,
          checkpoint_timer: nil,
          attempt: 0
        }

        # Armed once for the process's whole life rather than per connection: a
        # pending run of skips outlives a disconnect.
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
  # rows and then the connection ending, say. `:continue` asks the transport for
  # whatever is left over once the rows are run, so a terminal event never
  # discards the rows that preceded it and never has to wait for a message that
  # is not coming.
  defp drive(message, state) do
    case state.transport.handle_message(message, state.source) do
      :unknown ->
        {:noreply, state}

      {:noreply, source} ->
        {:noreply, touch(%{state | source: source})}

      {:rows, rows, source} ->
        state = touch(%{state | source: source})

        report_lag(rows, state)

        drive(:continue, Enum.reduce(rows, state, &run/2))

      {:disconnect, reason, source} ->
        {:noreply, reconnect(%{state | source: source}, reason)}

      {:stop, reason} ->
        {:stop, reason, state}
    end
  end

  # The local transport's handoff. Running the batch inside the call is what
  # makes the reader block until it is done, which is the whole of the
  # backpressure on that path — and replying only afterwards is what says so.
  @impl GenServer
  def handle_call({:rows, rows}, _from, state) do
    state = touch(state)

    report_lag(rows, state)

    {:reply, :ok, Enum.reduce(rows, state, &run/2)}
  end

  # A clean stop — an `unsubscribe/2`, a supervisor shutdown — should not throw
  # away skips already made. Rescued because the repo's pool may already be on
  # its way down, and re-skipping is the ordinary cost of a pending run rather
  # than something worth a noisy shutdown crash.
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

  # The subscription's whole option list plus where to resume: a transport takes
  # what it needs and ignores the rest, so adding one costs no plumbing here.
  defp connect_opts(state, cursor) do
    Keyword.merge(state.opts, cursor: cursor, owner: self())
  end

  # `:none` rather than 0 for "never committed anything" — that distinction is
  # what makes `start: :latest` meaningful exactly once.
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

  # Inline, one row at a time. That *is* the backpressure: while this runs, the
  # transport is not being asked for more and cannot deliver more, so the log's
  # write blocks. Nothing buffers, so there is nothing to bound.
  defp run(row, state) do
    {:ok, pipeline} = Pipeline.run(row, %{state.pipeline | cursor: cursor_of(state)})

    %{state | pipeline: pipeline, cursor: pipeline.cursor}
  end

  defp cursor_of(%{cursor: :none}), do: 0
  defp cursor_of(%{cursor: cursor}), do: cursor

  # How far behind the tip this handler is, in ms rather than in ids. The wire
  # carries no `max_id` to subtract from, and `inserted_at` answers the question
  # the id difference was a proxy for anyway — "how stale is what I am handling".
  #
  # Once per batch rather than once per row: a batch is one read's worth of
  # rows, so this samples at the rate rows actually arrive instead of emitting an
  # event per message.
  #
  # It compares the log's clock against this one, so it is only as good as the
  # two machines' NTP agreement. That is fine for a seconds-scale signal and
  # would not be for anything finer — and on a local transport both clocks are
  # the same clock.
  defp report_lag([], _state), do: :ok

  defp report_lag(rows, state) do
    case rows |> List.last() |> Map.get("inserted_at") |> as_datetime() do
      nil ->
        :ok

      at ->
        :telemetry.execute(
          [:dmex, :handler, :lag],
          %{lag: DateTime.diff(DateTime.utc_now(), at, :millisecond), rows: length(rows)},
          %{subscription: state.subscription, handler: state.handler, origin: state.origin}
        )
    end
  end

  defp as_datetime(%DateTime{} = at), do: at
  defp as_datetime(%NaiveDateTime{} = at), do: DateTime.from_naive!(at, "Etc/UTC")

  defp as_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, at, _offset} -> at
      {:error, _reason} -> nil
    end
  end

  defp as_datetime(_other), do: nil

  ## Reconnecting

  defp reconnect(state, reason) do
    # Before anything else: the next connect resumes from the *durable* cursor,
    # so an unwritten run of skips would be re-read and re-skipped for nothing.
    state = flush(state)

    close(state)

    if state.liveness_timer, do: Process.cancel_timer(state.liveness_timer)

    attempt = state.attempt + 1

    delay =
      Backoff.delay(attempt, state.config.reconnect_backoff, state.config.reconnect_backoff_max)

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

  # Settles a coalesced run of skips. A no-op unless one is outstanding, so the
  # timer costs nothing on a stream whose rows are all handled.
  defp flush(state) do
    {:ok, pipeline} = Pipeline.flush(state.pipeline)

    %{state | pipeline: pipeline}
  end

  defp schedule_checkpoint(state) do
    timer = Process.send_after(self(), :checkpoint, state.config.checkpoint_interval)

    %{state | checkpoint_timer: timer}
  end

  ## Liveness

  defp touch(state) do
    # Re-armed on every sign of life rather than only on connect, because the
    # deadline itself can change: an SSE transport does not know the log's
    # advertised interval until the log has spoken, and the first timer would
    # otherwise be sized against a guess for its whole first period.
    state
    |> Map.put(:last_activity, System.monotonic_time(:millisecond))
    |> schedule_liveness()
  end

  # Checked twice per deadline, so a dead connection is noticed within about
  # half an interval of becoming provably dead rather than a whole one. The
  # floor keeps a transport with a very short interval from turning this into a
  # busy loop, and `:infinity` arms nothing at all.
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
