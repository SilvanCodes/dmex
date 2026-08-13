defmodule DMEx.Log.Watcher do
  @moduledoc """
  Reclaims connections that stopped making progress.

  One per log, started by that log's supervisor. It holds no state about any
  connection: every sweep reads the live set out of `DMEx.Log.Streams`,
  compares each armed deadline against the clock, and kills whatever is past it.

  ## Why anything is needed at all

  The send loop bounds memory by construction, but it cannot reclaim itself:
  **a process blocked in `chunk/2` cannot time itself out, because it is not
  running.** Nor can one blocked in a read against a database that stopped
  answering, or waiting on a pool checkout that never returns.

  So the loop states its budget before entering each blocking step (see
  `DMEx.Log.Status`) and this process enforces it from outside. That is the
  whole mechanism: no per-connection timer, no progress heuristics, no
  inspecting another process's state. Socket options are still worth setting —
  [the deployment guide](deployment.md) says why — but nothing here depends on them.

  ## Why the kill is untrappable

  `Process.exit(pid, :kill)`, never a gentler reason. An ordinary exit signal
  sent to a process trapping exits becomes a message in its mailbox — and the
  mailbox is exactly what a wedged process is not servicing. Killing also frees
  a process suspended on a busy port, which is what a blocked `chunk/2` is.

  That reclaims the request process, its registry entry, the database connection
  it had checked out (the pool monitors its client), and the socket through the
  web server's teardown. **Dropping a connection is not a delivery event** —
  ARCHITECTURE says why, and it is what makes it safe to be blunt.

  ## What it cannot see

  A peer that vanished while the kernel still accepts our bytes: deadlines keep
  being met and nothing inside this VM can tell. TCP keepalive is the only
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
    state = %{log: log, interval: log.__dmex__().config.stall_check_interval}

    schedule(state)

    {:ok, state}
  end

  @impl GenServer
  def handle_info(:sweep, state) do
    schedule(state)
    reclaim_overdue(state)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(_message, state), do: {:noreply, state}

  @impl GenServer
  def handle_call(:sweep, _from, state), do: {:reply, reclaim_overdue(state), state}

  ## Internals

  # One clock reading for the whole pass, so every connection is judged against
  # the same instant however long the walk takes.
  defp reclaim_overdue(state) do
    now = System.monotonic_time(:millisecond)
    reclaimed = :counters.new(1, [])

    # `each/2` runs here rather than in the registry's own processes, so a sweep
    # costs one atomics read per connection and nothing that can block. A
    # deadline is a deadline whatever the connection selected.
    Streams.each(state.log, fn pid, status, _selection ->
      if Status.overdue?(status, now) do
        reclaim(state.log, pid, status, now)
        :counters.add(reclaimed, 1, 1)
      end
    end)

    :counters.get(reclaimed, 1)
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
