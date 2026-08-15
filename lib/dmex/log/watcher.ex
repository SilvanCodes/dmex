defmodule DMEx.Log.Watcher do
  @moduledoc """
  Reclaims connections that stopped making progress.

  One per log, started by that log's supervisor, holding no state about any
  connection: every sweep reads the live set out of `DMEx.Log.Streams`, compares
  each armed deadline against the clock, and kills whatever is past it. That is
  ARCHITECTURE's "every blocking step has a deadline", enforced from outside
  because a wedged process is not running to enforce it itself.

  Its own process rather than a job on `DMEx.Log.Dispatcher`'s timer: the
  dispatcher queries the database inside its handler, so pool exhaustion would
  block the sweep at exactly the moment connections are stalling.

  **The kill is untrappable**, because an exit signal sent to a process trapping
  exits becomes a message in the mailbox a wedged process is not servicing, and
  `:kill` also frees one suspended on a busy port — which is what a blocked
  `chunk/2` is. What it cannot see is a peer that vanished while the kernel still
  accepts our bytes: deadlines keep being met, and TCP keepalive is the only
  lever — see [the deployment guide](deployment.md).
  """

  use GenServer

  alias DMEx.Log.Status
  alias DMEx.Log.Streams

  require Logger

  @doc """
  Starts a watcher for `log:`.

  Done automatically by the log's own supervisor — and not at all when that log
  sets `stall_timeout: :infinity`, since there would be nothing to enforce.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    log = Keyword.fetch!(opts, :log)

    GenServer.start_link(__MODULE__, log, name: name(log))
  end

  @doc "The watcher name for a log module."
  @spec name(module()) :: atom()
  def name(log) when is_atom(log), do: Module.concat(log, "Watcher")

  @doc """
  Sweeps immediately and returns how many connections were reclaimed.

  The timer does this on its own; this is for tests and for an operator with a
  console who does not want to wait out an interval.
  """
  @spec sweep(module()) :: non_neg_integer()
  def sweep(log) when is_atom(log), do: GenServer.call(name(log), :sweep)

  @impl GenServer
  def init(log) do
    state = %{log: log, interval: log.__dmex_log__().config.stall_check_interval}

    schedule(state)

    {:ok, state}
  end

  # No catch-all below: the only senders are this process' own `:sweep` timer
  # and `sweep/1`'s call, so an unexpected message is impossible and crashing on
  # one is free — the supervisor restarts a watcher that holds no state.
  @impl GenServer
  def handle_info(:sweep, state) do
    schedule(state)
    reclaim_overdue(state)

    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:sweep, _from, state), do: {:reply, reclaim_overdue(state), state}

  ## Internals

  # One clock reading for the whole pass, so every connection is judged against
  # the same instant however long the walk takes. The list comes out of the
  # registry in this process, so a sweep costs one atomics read per connection
  # and nothing that can block.
  defp reclaim_overdue(state) do
    now = System.monotonic_time(:millisecond)

    Enum.count(Streams.all(state.log), fn {pid, status, _selection} ->
      if Status.overdue?(status, now) do
        reclaim(state.log, pid, status, now)

        true
      end
    end)
  end

  defp reclaim(log, pid, status, now) do
    overdue_by = now - Status.deadline(status)

    Status.disarm(status)

    :telemetry.execute(
      [:dmex, :stream, :reclaim],
      %{overdue_by: overdue_by},
      %{log: log}
    )

    Logger.warning(
      "DMEx reclaimed a stalled #{inspect(log)} stream: #{inspect(pid)} missed its " <>
        "deadline by #{overdue_by}ms. Nothing is lost — the consumer resumes from its own " <>
        "durable cursor — but a consumer that cannot keep up will do this repeatedly."
    )

    Process.exit(pid, :kill)
  end

  defp schedule(state), do: Process.send_after(self(), :sweep, state.interval)
end
