defmodule DMEx.Handler.Subscription do
  @moduledoc """
  One subscription: one transport, one handler, one cursor.

  Reads its durable `last_seen`, starts a `DMEx.Transport` reader at that
  position, and runs every batch that reader hands it through
  `DMEx.Handler.Pipeline`, in order, one at a time.

  ## The reader is a process, so failure has one shape

  `c:DMEx.Transport.start_reader/1` spawns a linked process; everything that can
  go wrong in it — a refused connection, a dead socket, silence past the
  deadline — arrives here as one exit signal. So there is no protocol to
  interpret and no way for a connection ending to discard rows already handed
  over: those arrived as a call that returned before anything could end.

  Only `DMEx.Transport.fatal/1` is answered differently, by stopping instead of
  retrying — which is why the child is `:transient` and the stop reason is a
  `{:shutdown, reason}`. A `:permanent` child is restarted whatever it exits
  with, and `init/1` reconnects immediately at `attempt: 0`, so "stop" would
  have meant an unbacked-off request loop against the endpoint that just
  refused it. An ordinary crash still restarts, which is what `on_failure:
  :crash` rests on.

  ## Resumption is entirely cursor-driven

  On every connect — first or thousandth — the stream reads its durable
  `last_seen` and hands it to the reader. It never resumes from the last row it
  saw and never trusts anything in memory, which is what makes a dropped
  connection a non-event. `last_seen:` applies only when there is no cursor at
  all.

  ## Reconnects are jittered

  `DMEx.Handler.Subscriptions` sets no restart budget, so the pacing is here for a
  *disconnect*: each reconnect triggers a fresh read against a *different
  application's* database when the transport is a wire. A restart is not paced
  here at all — `init/1` starts at `attempt: 0` and connects immediately — so a
  crash loop is paced by `DMEx.Handler.Pipeline` instead, before it re-raises.

  One retry policy covers every transport: a reader says only that it died, and
  when to try again is decided here. `DMEx.Transport.connected/1` is what clears
  the backoff, so a reader that dies before establishing anything counts as the
  failed attempt it was.

  ## Skips are checkpointed on a timer, not per row

  `DMEx.Handler.Pipeline` defers a skipped row's cursor advance. This process
  settles it: a `checkpoint_interval` timer, armed per process rather than per
  connection, plus a flush before a reconnect (which re-reads the durable
  cursor) and in `terminate/2` (so a clean stop does not discard skips).

  The `terminate/2` half only works because this process **traps exits**: a
  non-trapping child of a `DynamicSupervisor` is killed outright by
  `terminate_child/2` and the callback never runs. Trapping is also what turns a
  reader's death into an ordinary reconnect.

  ## Backpressure is the absence of a buffer

  Rows run through the pipeline **inline**, one at a time, inside the reader's
  `DMEx.Transport.deliver/2` call: no queue, no drain loop — ARCHITECTURE
  invariant 8.
  """

  use GenServer

  alias DMEx.Handler.Pipeline
  alias DMEx.Storage
  alias DMEx.Transport

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
      # `:transient`, so a deliberate `{:shutdown, _}` stop stays stopped and
      # everything else — a crash, a poison message under `on_failure: :crash` —
      # is restarted as before.
      restart: :transient
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
          # The reader's whole input, and `:transport` in it is what
          # `DMEx.Transport.start_reader/1` dispatches on — so the module is not
          # named twice.
          opts: opts,
          # Both, deliberately: the id is the durable identity everything is
          # keyed by, the module is what a human reads in a log line.
          handler: Keyword.fetch!(opts, :handler),
          subscription: subscription,
          # Where the rows come from, for the operator: a URL, or the module of
          # a log this application owns.
          origin: Keyword.fetch!(opts, :origin),
          config: Keyword.fetch!(opts, :config),
          pipeline: Pipeline.new(opts),
          storage: Keyword.fetch!(opts, :storage),
          reader: nil,
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

  # A reader ends the connection by ending, whatever went wrong inside it.
  #
  # `{:shutdown, reason}` rather than `reason`: it carries what happened and it
  # is what a `:transient` child has to exit with to stay stopped.
  @impl GenServer
  def handle_info({:EXIT, reader, {:fatal, reason}}, %{reader: reader} = state) do
    {:stop, {:shutdown, reason}, %{state | reader: nil}}
  end

  @impl GenServer
  def handle_info({:EXIT, reader, reason}, %{reader: reader} = state) do
    {:noreply, reconnect(%{state | reader: nil}, reason)}
  end

  # A reader this subscription has already given up on — killed by `terminate/2`
  # — and its signal arriving after the fact.
  @impl GenServer
  def handle_info(_message, state), do: {:noreply, state}

  # The reader's handoff: running the batch inside the call is what makes it
  # block until the pipeline is done, which is the whole of the backpressure.
  @impl GenServer
  def handle_call({:rows, rows}, _from, state) do
    {:reply, :ok, Enum.reduce(rows, state, &run/2)}
  end

  # Sent once per connection, by a reader that got one. Nothing else clears the
  # backoff, so a reader dying before this counts as an attempt.
  @impl GenServer
  def handle_cast(:connected, state), do: {:noreply, %{state | attempt: 0}}

  # A clean stop should not throw away skips already made, and it should not turn
  # a shutdown into a crash either: re-skipping is the ordinary cost of a pending
  # run.
  #
  # `catch` as well as `rescue`, because the failure the comment has always named
  # — the repo's pool already on its way down — arrives as an **exit**, from the
  # `GenServer` call inside `Ecto.Repo.transaction/1`, not as an exception. With
  # only the rescue, `terminate/2` died there and never reached `stop_reader/1`.
  @impl GenServer
  def terminate(_reason, state) do
    try do
      flush(state)
    rescue
      _exception -> :ok
    catch
      :exit, _reason -> :ok
    end

    stop_reader(state)
  end

  ## Connecting

  defp connect(state) do
    case durable_cursor(state) do
      {:ok, cursor} ->
        {:noreply,
         %{
           state
           | reader: Transport.start_reader(reader_opts(state, cursor)),
             # The durable cursor just read is the authority, so it is seeded
             # here rather than carried alongside — and anything still marked
             # pending was either already written or is about to be re-delivered
             # and re-skipped. Either way it is not owed a write.
             pipeline: %{state.pipeline | cursor: resume_at(cursor), pending: false}
         }}

      # The cursor could not be read, so there is no position to start a reader
      # at. Same answer as a reader that could not connect.
      {:error, reason} ->
        {:noreply, reconnect(state, reason)}
    end
  end

  # The whole option list plus where to resume and who to hand rows to: a
  # transport takes what it needs and ignores the rest, so adding one costs no
  # plumbing here.
  defp reader_opts(state, cursor) do
    Keyword.merge(state.opts, cursor: cursor, owner: self())
  end

  # The reader is handed the cursor raw, `:none` included, because that is
  # what `DMEx.Handler.Request.position/2` needs to apply `last_seen:`. The
  # pipeline is handed this instead: it compares `id <= cursor`, and every number
  # sorts below every atom in Erlang term order, so `:none` there would suppress
  # every row as a duplicate rather than none of them.
  defp resume_at(:none), do: 0
  defp resume_at(cursor), do: cursor

  # `:none` rather than 0 for "never committed anything" — that distinction is
  # what makes `last_seen:` apply exactly once.
  defp durable_cursor(state) do
    case Storage.last_seen(state.storage, state.subscription) do
      {:ok, nil} -> {:ok, :none}
      other -> other
    end
  end

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
    # Before anything else: the next connect resumes from the *durable* cursor,
    # so an unwritten run of skips would be re-read and re-skipped for nothing.
    state = flush(state)

    attempt = state.attempt + 1

    delay = DMEx.backoff(attempt, state.config.reconnect_backoff_base, state.config.reconnect_backoff_max)

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

    %{state | attempt: attempt}
  end

  ## Checkpointing

  defp flush(state) do
    {:ok, pipeline} = Pipeline.flush(state.pipeline)

    %{state | pipeline: pipeline}
  end

  # No reference kept: this timer is never cancelled or rearmed early, and
  # `handle_info(:checkpoint, ...)` is correct whenever it lands — including
  # after a reconnect, since a pending run of skips outlives one.
  defp schedule_checkpoint(state) do
    Process.send_after(self(), :checkpoint, state.config.checkpoint_interval)

    state
  end

  defp register(nil, _subscription), do: {:ok, self()}

  defp register(registry, subscription) do
    Registry.register(registry, {:subscription, subscription}, nil)
  end
end
