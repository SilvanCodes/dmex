defmodule DMEx.Producer.Status do
  @moduledoc """
  One connection's shared status: its wakeup flag and its deadline.

  Two things about a stream process have to be readable from *outside* it,
  because the interesting case is precisely the one where that process is not
  running: whether it already has a wakeup queued, and by when it expects to be
  past whatever it is currently blocked in. Both are single machine words, both
  are written far more often than they are read, and neither may cost a message
  round trip to a process that might be wedged — so both live in one `:atomics`
  reference of two slots.

  ## The wakeup flag

  Wakeups are the last place in the producer where memory could grow without
  bound. A stalled connection is blocked inside `chunk/2` and therefore cannot
  service its mailbox, while publishes keep arriving — so `:wakeup` messages
  would accumulate at the publish rate for as long as the stall lasts.

  Since a wakeup carries no information, N of them and one of them are the same
  instruction. So each connection gets a one-bit flag, and the dispatcher only
  sends when it flips from clear to pending. Mailbox depth per connection becomes
  O(1) rather than O(publish rate x stall duration), however long a stall lasts
  and however many connections stall together — and, usefully, without depending
  on `stall_timeout` being tuned correctly.

  `:atomics.exchange/3` is what makes `flip/1` a test-and-set rather than a read
  followed by a write, so two dispatchers cannot both conclude they should send.

  ## The deadline

  **A process blocked in `chunk/2` cannot time itself out, because it is not
  running.** The same is true of one blocked in a read against a database that
  has stopped answering. So the loop states its budget *before* entering each
  blocking step — `arm/2` — and `DMEx.Producer.Watcher` is what enforces it. A
  deadline in the past means the loop did not get past a step it expected to
  clear, which is the whole of "stuck" and needs no inference about process
  state.

  Deadlines are `System.monotonic_time(:millisecond)`, so they are immune to
  wall-clock adjustment and directly comparable between the stream that writes
  one and the watcher that reads it.

  ## Both live in the connection's registry entry

  The reference is created by `DMEx.Registry.register/2` and stored as the value
  of that entry. The placement does the cleanup for free: the entry dies with the
  request process, and the status with it. There is no table to own, no process
  to own it, and no monitor watching for connections that died mid-stall — a
  connection cannot outlive its own status, or the other way round.

  The two slots are written by different processes — the dispatcher flips the
  flag, the stream arms its own deadline — and never contend: `:atomics`
  operations are per element, not per reference.

  ## Clear before the read, never after

  `DMEx.Producer.Stream` clears its flag *before* issuing its read. A publish
  landing between the clear and the read re-sets the flag and enqueues a wakeup,
  so the loop runs again and performs a possibly-empty read. **Spurious wakeups
  are harmless; missed ones would not be, and this ordering makes them
  impossible.** Clearing after the read would open a window where a publish is
  both invisible to the read and unable to set the flag.
  """

  @flag 1
  @deadline 2

  # `:atomics` slots are 64-bit signed, and this is the maximum. It reads as "no
  # deadline" everywhere a comparison is made, which is what both a disarmed
  # connection and a producer running without a stall timeout need.
  @never 9_223_372_036_854_775_807

  @typedoc "One connection's status."
  @opaque t :: :atomics.atomics_ref()

  @typedoc "How long a step may take before the watcher reclaims the connection."
  @type budget :: pos_integer() | :infinity

  @doc """
  A fresh status: flag clear, deadline armed for `budget`.

  Armed rather than blank, so a connection is watched from the instant it
  registers — the connect path writes response headers and the `retry:` frame
  before the send loop ever runs, and those writes can stall like any other.
  """
  @spec new(budget()) :: t()
  def new(budget) do
    ref = :atomics.new(2, [])

    arm(ref, budget)

    ref
  end

  @doc """
  Marks the connection as having a wakeup queued, and says whether it already did.

  Returns `true` when this call flipped the flag from clear to pending — that
  is, when the caller should actually send.
  """
  @spec flip(t()) :: boolean()
  def flip(ref), do: :atomics.exchange(ref, @flag, 1) == 0

  @doc "Clears the flag. Called by the stream itself, before every read."
  @spec clear(t() | nil) :: :ok
  def clear(nil), do: :ok

  def clear(ref) do
    :atomics.put(ref, @flag, 0)

    :ok
  end

  @doc "Whether a wakeup is currently queued. For tests and telemetry."
  @spec pending?(t()) :: boolean()
  def pending?(ref), do: :atomics.get(ref, @flag) == 1

  @doc """
  Declares that the next step should be done within `budget` ms.

  Called by the stream process itself, immediately before it blocks. `:infinity`
  arms no deadline at all, which is how a producer opts out of reclaiming.
  """
  @spec arm(t() | nil, budget()) :: :ok
  def arm(nil, _budget), do: :ok

  def arm(ref, :infinity) do
    :atomics.put(ref, @deadline, @never)

    :ok
  end

  def arm(ref, budget) when is_integer(budget) and budget > 0 do
    :atomics.put(ref, @deadline, System.monotonic_time(:millisecond) + budget)

    :ok
  end

  @doc """
  Drops the deadline, so nothing is overdue until the next `arm/2`.

  The watcher does this to a connection it has just reclaimed. Killing is
  asynchronous and the registry entry outlives the signal by however long the
  registry takes to process the `:DOWN`, so without this a second sweep could
  report the same connection again.
  """
  @spec disarm(t()) :: :ok
  def disarm(ref), do: arm(ref, :infinity)

  @doc "The armed deadline, in `System.monotonic_time(:millisecond)`."
  @spec deadline(t()) :: integer()
  def deadline(ref), do: :atomics.get(ref, @deadline)

  @doc "Whether this connection's deadline has passed as of `now`."
  @spec overdue?(t(), integer()) :: boolean()
  def overdue?(ref, now), do: deadline(ref) < now
end
