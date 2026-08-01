defmodule DMEx.Log.Status do
  @moduledoc """
  One connection's shared status: its wakeup flag and its deadline.

  Two things about a stream process have to be readable from *outside* it,
  because the interesting case is precisely the one where that process is not
  running: whether it already has a wakeup queued, and by when it expects to be
  past whatever it is blocked in. Both are single machine words written far more
  often than read, and neither may cost a round trip to a process that might be
  wedged — so both live in one `:atomics` reference of two slots.

  **The wakeup flag** is what keeps mailbox depth O(1) rather than O(append rate
  × stall duration): a stalled connection is blocked inside `chunk/2` and cannot
  service its mailbox while appends keep arriving, and since a wakeup carries no
  information, N of them and one are the same instruction. So the dispatcher
  only sends when the flag flips from clear to pending. `:atomics.exchange/3`
  makes `flip/1` a test-and-set, so two dispatchers cannot both conclude they
  should send.

  `DMEx.Log.Stream` clears its flag *before* issuing its read, never after. An
  append landing between the clear and the read re-sets the flag and enqueues a
  wakeup, so the loop runs again and performs a possibly-empty read — spurious
  wakeups are harmless, missed ones would not be, and clearing afterwards would
  open a window where an append is both invisible to the read and unable to set
  the flag.

  **The deadline** exists because a process blocked in `chunk/2` cannot time
  itself out — nor can one blocked in a read against a database that stopped
  answering. So the loop states its budget *before* entering each blocking step
  (`arm/2`) and `DMEx.Log.Watcher` enforces it. A deadline in the past means the
  loop did not get past a step it expected to clear, which is the whole of
  "stuck". Deadlines are `System.monotonic_time(:millisecond)`, immune to
  wall-clock adjustment and directly comparable between writer and reader.

  Both live in the connection's registry entry, created by
  `DMEx.Log.Registry.register/3`, which does the cleanup for free: a connection
  cannot outlive its own status. The two slots are written by different
  processes — the dispatcher flips the flag, the stream arms its deadline — and
  never contend, since `:atomics` operations are per element.
  """

  @flag 1
  @deadline 2

  # `:atomics` slots are 64-bit signed, and this is the maximum. It reads as "no
  # deadline" everywhere a comparison is made, which is what both a disarmed
  # connection and a log running without a stall timeout need.
  @never 9_223_372_036_854_775_807

  @typedoc "One connection's status."
  @opaque t :: :atomics.atomics_ref()

  @typedoc "How long a step may take before the watcher reclaims the connection."
  @type budget :: pos_integer() | :infinity

  @doc """
  Builds a fresh status: flag clear, deadline armed for `budget`.

  Armed rather than blank, so a connection is watched from the instant it
  registers — the connect path writes response headers and the `retry:` frame
  before the send loop runs, and those writes can stall like any other.
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
  arms no deadline at all, which is how a log opts out of reclaiming.
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

  The watcher does this to a connection it has just reclaimed: killing is
  asynchronous and the registry entry outlives the signal, so without this a
  second sweep could report the same connection again.
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
