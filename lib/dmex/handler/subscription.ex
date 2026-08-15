defmodule DMEx.Handler.Subscription do
  # Where reconnect doubling stops, and where a reconnect stops being
  # unremarkable. Above the moduledoc only because the moduledoc names it.
  @max_attempts 10

  @moduledoc """
  One subscription: one transport, one handler, one cursor.

  Reads the durable `last_seen`, starts a `DMEx.Transport` reader there, and runs
  every batch that reader hands it through `DMEx.Handler.Pipeline`, in order, one
  at a time.

  **A reader is a process, so failure has one shape.** A refused connection, a
  dead socket, silence past the deadline: all arrive as one exit signal, and rows
  already handed over came as a call that returned. Only `DMEx.Transport.fatal/1`
  is answered by stopping, which is why the child is `:transient` and the reason a
  `{:shutdown, _}`: `init/1` connects immediately at `attempt: 0`, so a
  `:permanent` child would turn "stop" into an unbacked-off loop against the
  endpoint that just refused it. An ordinary crash still restarts, which is what
  `on_failure: :crash` rests on.

  **Resumption is cursor-driven.** Every connect, first or thousandth, re-reads
  the durable cursor: nothing in memory is trusted, so a dropped connection is a
  non-event and `last_seen:` applies only when there is no cursor at all.

  **Reconnects are jittered, and counted to #{@max_attempts}.** Pacing belongs
  here because each reconnect reads a *different application's* database, and one
  policy covers every transport: a reader says only that it died.
  `DMEx.Transport.connected/1` is the only thing that clears the count, so a
  reader that died before establishing anything counts as the failed attempt it
  was. Restarts are not paced here at all; `DMEx.Handler.Pipeline` pauses before
  it re-raises.

  **Skips are checkpointed on a timer, not per row.** `DMEx.Handler.Pipeline`
  defers a skipped row's cursor advance and this process settles it — on a
  `checkpoint_interval` timer, before a reconnect, and in `terminate/2`. That
  callback only runs because this process **traps exits** — a non-trapping child
  is killed outright by `Supervisor.terminate_child/2`. Trapping is also what
  turns a reader's death into an ordinary reconnect.
  """

  use GenServer

  alias DMEx.Handler.Pipeline
  alias DMEx.Storage
  alias DMEx.Transport

  require Logger

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :subscription)},
      start: {__MODULE__, :start_link, [opts]},
      # A deliberate `{:shutdown, _}` stop stays stopped; everything else — a
      # crash, a poison message under `on_failure: :crash` — is restarted.
      restart: :transient
    }
  end

  @impl GenServer
  def init(opts) do
    subscription = Keyword.fetch!(opts, :subscription)

    Process.flag(:trap_exit, true)

    # Registered before anything opens, so `unsubscribe/2` can find this process
    # by id. The duplicate guard is the supervisor's — a subscription id *is* the
    # child id — and this only catches a `Subscription` started outside one.
    case register(Keyword.fetch!(opts, :registry), subscription) do
      {:ok, _pid} ->
        # Both carry handler, subscription, storage, origin and config, and the
        # copy is deliberate: `:opts` is the list a reader is handed whole —
        # `DMEx.Transport.start_reader/1` dispatches on its `:transport`, and a
        # reader reads the rest of it too — while the pipeline is what everything
        # in *this* process reads, and the only one of the two carrying the
        # moving `:cursor`.
        state = %{opts: opts, pipeline: Pipeline.new(opts), reader: nil, attempt: 0}

        # Per process rather than per connection: a pending run of skips outlives
        # a disconnect.
        {:ok, schedule_checkpoint(state), {:continue, :connect}}

      {:error, {:already_registered, _pid}} ->
        {:stop, {:already_subscribed, subscription}}
    end
  end

  @impl GenServer
  def handle_continue(:connect, state), do: connect(state)

  @impl GenServer
  def handle_info(:connect, state), do: connect(state)

  def handle_info(:checkpoint, state) do
    {:noreply, state |> flush() |> schedule_checkpoint()}
  end

  # A reader ends the connection by ending, whatever went wrong inside it.
  # `{:shutdown, reason}` rather than `reason`: it carries what happened and it is
  # what a `:transient` child has to exit with to stay stopped.
  def handle_info({:EXIT, reader, {:fatal, reason}}, %{reader: reader} = state) do
    {:stop, {:shutdown, reason}, %{state | reader: nil}}
  end

  def handle_info({:EXIT, reader, reason}, %{reader: reader} = state) do
    {:noreply, reconnect(%{state | reader: nil}, reason)}
  end

  # A reader this subscription already gave up on, signalling after the fact:
  # `reconnect/2` cleared `:reader` before this arrived and the next connect
  # starts a new one, so there is nothing to do and nothing to say.
  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  # A stray message is not worth a crash: a subscription that dies of an unknown
  # `handle_info` loses a live stream. Logged rather than swallowed, because
  # nothing this process owns sends one, so the sender is outside it.
  def handle_info(message, state) do
    Logger.warning(
      "DMEx #{state.pipeline.subscription} (#{inspect(state.pipeline.handler)}) ignored an " <>
        "unexpected message: #{inspect(message)}. Only its reader and its own checkpoint timer " <>
        "send this process anything. The subscription continues regardless."
    )

    {:noreply, state}
  end

  # The reader's handoff: running the batch inside the call is what blocks it
  # until the pipeline is done, which is the whole of the backpressure.
  @impl GenServer
  def handle_call({:rows, rows}, _from, state) do
    {:reply, :ok, Enum.reduce(rows, state, &run/2)}
  end

  # Sent once per connection, by a reader that got one. Nothing else clears the
  # backoff, so a reader dying before this counts as an attempt.
  @impl GenServer
  def handle_cast(:connected, state), do: {:noreply, %{state | attempt: 0}}

  # A clean stop should not throw away skips already made, and should not turn a
  # shutdown into a crash either: re-skipping is the ordinary cost of a pending
  # run. Any kind, because the failure that matters — the repo's pool already on
  # its way down — arrives as an **exit** from the `GenServer` call inside
  # `Ecto.Repo.transaction/1`, not as an exception, and a `rescue` alone let it
  # kill `terminate/2` before `stop_reader/1`.
  #
  # Only a graceful stop, because only that reaches here with the current state.
  # A callback that raised leaves `state` at its last *return* — before the batch
  # it died in — and `handle_call({:rows, _})` runs a whole batch inside one
  # return, so a pending run there can sit below an advance a handled row in that
  # same batch already committed. `c:DMEx.Storage.advance_cursor/3` is absolute,
  # so flushing it would move the durable cursor backwards and re-run side
  # effects that succeeded. The restart re-reads and re-skips instead.
  @impl GenServer
  def terminate(reason, state) do
    if graceful?(reason) do
      try do
        flush(state)
      catch
        _kind, _reason -> :ok
      end
    end

    stop_reader(state)
  end

  defp graceful?(:normal), do: true
  defp graceful?(:shutdown), do: true
  defp graceful?({:shutdown, _reason}), do: true
  defp graceful?(_reason), do: false

  ## Connecting

  defp connect(state) do
    case durable_cursor(state) do
      {:ok, cursor} ->
        # The whole option list plus where to resume and who to hand rows to: a
        # transport takes what it needs, so adding one costs no plumbing here.
        reader = Transport.start_reader(Keyword.merge(state.opts, cursor: cursor, owner: self()))

        # The durable cursor just read is the authority, so it is seeded here
        # rather than carried alongside — and anything still marked pending was
        # either already written or is about to be re-delivered and re-skipped.
        pipeline = %{state.pipeline | cursor: resume_at(cursor), pending: false}

        {:noreply, %{state | reader: reader, pipeline: pipeline}}

      # No cursor read means no position to start a reader at. Same answer as a
      # reader that could not connect.
      {:error, reason} ->
        {:noreply, reconnect(state, reason)}
    end
  end

  # `:none` rather than 0 for "never committed anything" — that distinction is
  # what makes `last_seen:` apply exactly once.
  defp durable_cursor(%{pipeline: pipeline}) do
    case Storage.last_seen(pipeline.storage, pipeline.subscription) do
      {:ok, nil} -> {:ok, :none}
      other -> other
    end
  end

  # The reader is handed the cursor raw, `:none` included, because that is what
  # `DMEx.Handler.Request.position/2` needs to apply `last_seen:`. The pipeline is
  # handed this instead: it compares `id <= cursor`, and every number sorts below
  # every atom in Erlang term order, so `:none` there would suppress every row as
  # a duplicate rather than none of them.
  defp resume_at(:none), do: 0
  defp resume_at(cursor), do: cursor

  # Unlinked first, so the exit this sends does not come back as one more
  # disconnect to reconnect from.
  defp stop_reader(%{reader: nil}), do: :ok

  defp stop_reader(%{reader: reader}) do
    Process.unlink(reader)
    Process.exit(reader, :shutdown)

    :ok
  end

  ## Running

  defp run(row, state) do
    {:ok, pipeline} = Pipeline.run(row, state.pipeline)

    %{state | pipeline: pipeline}
  end

  ## Reconnecting

  # Only ever reached with no reader: its death is what gets here, or there was
  # never one to start, so there is nothing to close first.
  defp reconnect(state, reason) do
    # Before anything else: the next connect resumes from the *durable* cursor, so
    # an unwritten run of skips would be re-read and re-skipped for nothing.
    %{pipeline: pipeline} = state = flush(state)

    attempt = min(state.attempt + 1, @max_attempts)
    config = pipeline.config
    delay = DMEx.backoff(attempt, config.reconnect_backoff_base, config.reconnect_backoff_max)

    log(pipeline, attempt, delay, reason)

    :telemetry.execute(
      [:dmex, :handler, :reconnect],
      %{attempt: attempt, delay: delay},
      %{
        subscription: pipeline.subscription,
        handler: pipeline.handler,
        origin: pipeline.origin,
        reason: reason
      }
    )

    Process.send_after(self(), :connect, delay)

    %{state | attempt: attempt}
  end

  # At the cap, and from then on: an origin that has refused this many
  # consecutive connects is not a blip, and a `debug` line is invisible in the
  # deployment where that matters.
  defp log(pipeline, @max_attempts, delay, reason) do
    Logger.warning(fn ->
      "DMEx #{pipeline.subscription} (#{inspect(pipeline.handler)}) has failed " <>
        "#{@max_attempts} consecutive connects to #{inspect(pipeline.origin)}, " <>
        "retrying in #{delay}ms after #{inspect(reason)}"
    end)
  end

  defp log(pipeline, _attempt, delay, reason) do
    Logger.debug(fn ->
      "DMEx #{pipeline.subscription} (#{inspect(pipeline.handler)}) reconnecting to " <>
        "#{inspect(pipeline.origin)} in #{delay}ms after #{inspect(reason)}"
    end)
  end

  ## Checkpointing

  defp flush(state) do
    {:ok, pipeline} = Pipeline.flush(state.pipeline)

    %{state | pipeline: pipeline}
  end

  # No reference kept: this timer is never cancelled or rearmed early, and
  # `handle_info(:checkpoint, ...)` is correct whenever it lands — including after
  # a reconnect, since a pending run of skips outlives one.
  defp schedule_checkpoint(state) do
    Process.send_after(self(), :checkpoint, state.pipeline.config.checkpoint_interval)

    state
  end

  defp register(registry, subscription) do
    Registry.register(registry, {:subscription, subscription}, nil)
  end
end
