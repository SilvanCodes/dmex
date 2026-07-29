defmodule DMEx.Producer.Watcher do
  @moduledoc """
  Reclaims connections that stopped making progress.

  One per producer, started by that producer's supervisor. It holds no state
  about any connection: every sweep reads the live set out of
  `DMEx.Producer.Registry`, compares each connection's armed deadline against the
  clock, and kills whatever is past it.

  ## Why anything is needed at all

  The send loop bounds memory by construction — it holds at most the frame it is
  currently writing, and the next read cannot happen until the previous write
  returns. What it cannot do is reclaim itself: **a process blocked in `chunk/2`
  cannot time itself out, because it is not running.** Nor can one blocked in a
  read against a database that has stopped answering, or waiting on a pool
  checkout that never returns.

  So the loop states its budget before entering each blocking step (see
  `DMEx.Producer.Status`), and this process enforces it from outside. That is the
  whole mechanism: no per-connection timer, no progress heuristics, no inspecting
  another process's state.

  DMEx used to leave this entirely to the socket layer, on the strength of
  `send_timeout` on the web server's listener. That was a poor contract for a
  library — it made correctness depend on transport configuration DMEx neither
  owns nor can verify, and it failed silently when an operator did not set it.
  It also only ever saw a stalled *socket*; a wedged query looked perfectly
  healthy to it. Socket options are still worth setting, and
  `guides/deployment.md` says why, but nothing here depends on them.

  ## Why the kill is untrappable

  `Process.exit(pid, :kill)`, never a gentler reason. An ordinary exit signal
  sent to a process trapping exits becomes a message in its mailbox — and the
  mailbox is exactly what a wedged process is not servicing, so the signal would
  be read at precisely the moment it was no longer needed. Killing also frees a
  process suspended on a busy port, which is what a blocked `chunk/2` is.

  What that reclaims: the request process, its registry entry, the database
  connection it had checked out (the pool monitors its client), and the socket,
  through the web server's own teardown — the same path a client disconnect
  takes. **Dropping a connection is not a delivery event**: the producer holds no
  consumer state, the consumer's `last_seen` is durable, and resumption is the
  same `id > last_seen` read a cold connect does. That is what makes it safe to
  be blunt.

  ## What it cannot see

  A peer that vanished while the kernel still accepts our bytes. Writes keep
  returning `:ok`, deadlines keep being met, and nothing running inside this VM
  can tell the difference. Heartbeats mean there is always unacked data in
  flight, so TCP's own retransmission timeout eventually errors the socket;
  tuned keepalive shortens that. This is the one case the socket layer still
  owns.

  ## Where global limits would land

  This process already knows the whole live set for a producer, which makes it
  the seat for the aggregate machinery v1 deliberately leaves out: a connection
  budget, `503`/`Retry-After` admission control asked for at connect, and a
  shedding policy — which can rank candidates by deadline, since it holds every
  one. See `ROADMAP.md`.
  """

  use GenServer

  alias DMEx.Producer.Status

  require Logger

  @doc """
  Starts a watcher for `producer:`.

  Started automatically by the producer's own supervisor — and not started at all
  when that producer sets `stall_timeout: :infinity`, since there would be
  nothing to enforce.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    producer = Keyword.fetch!(opts, :producer)

    GenServer.start_link(__MODULE__, producer, name: name(producer))
  end

  @doc "The watcher name for a producer module."
  @spec name(module()) :: atom()
  def name(producer) when is_atom(producer), do: Module.concat(producer, "Watcher")

  @doc """
  Sweeps immediately and returns how many connections were reclaimed.

  The timer does this on its own; this is for tests and for an operator with a
  console who does not want to wait out an interval.
  """
  @spec sweep(module()) :: non_neg_integer()
  def sweep(producer) when is_atom(producer), do: GenServer.call(name(producer), :sweep)

  @impl GenServer
  def init(producer) do
    %{config: config} = producer.__dmex__()

    state = %{producer: producer, interval: config.stall_check_interval}

    schedule(state)

    {:ok, state}
  end

  @impl GenServer
  def handle_info(:sweep, state) do
    schedule(state)
    sweep_now(state)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(_message, state), do: {:noreply, state}

  @impl GenServer
  def handle_call(:sweep, _from, state), do: {:reply, sweep_now(state), state}

  ## Internals

  # One clock reading for the whole pass, so every connection is judged against
  # the same instant however long the walk takes.
  defp sweep_now(state) do
    now = System.monotonic_time(:millisecond)
    reclaimed = :counters.new(1, [])

    # `each/2` runs here rather than in the registry's own processes, so a sweep
    # costs one atomics read per connection and nothing that can block.
    DMEx.Producer.Registry.each(state.producer, fn pid, status ->
      if Status.overdue?(status, now) do
        reclaim(state.producer, pid, status, now)
        :counters.add(reclaimed, 1, 1)
      end
    end)

    :counters.get(reclaimed, 1)
  end

  defp reclaim(producer, pid, status, now) do
    overdue_by = now - Status.deadline(status)

    # Disarmed before the signal, not after: the kill is asynchronous and the
    # entry survives until the registry processes the `:DOWN`, so an overlapping
    # sweep would otherwise report this connection a second time.
    Status.disarm(status)

    :telemetry.execute(
      [:dmex, :stream, :reclaim],
      %{overdue_by: overdue_by},
      %{producer: producer}
    )

    Logger.warning(
      "DMEx reclaimed a stalled #{inspect(producer)} stream: #{inspect(pid)} missed its " <>
        "deadline by #{overdue_by}ms. Nothing is lost — the consumer resumes from its own " <>
        "durable cursor — but a consumer that cannot keep up will do this repeatedly."
    )

    Process.exit(pid, :kill)
  end

  defp schedule(state), do: Process.send_after(self(), :sweep, state.interval)
end
