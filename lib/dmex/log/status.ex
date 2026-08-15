defmodule DMEx.Log.Status do
  @moduledoc """
  One connection's shared status: its wakeup flag and its deadline.

  Two single machine words that have to be readable from *outside* the stream
  process, because the interesting case is precisely the one where that process
  is not running, and both written far more often than read — so both live in one
  `:atomics` reference of two slots, held by `DMEx.Log.Streams`. The dispatcher
  writes the flag, the stream arms the deadline, and `:atomics` operations are
  per element, so the two never contend. `mark_pending/1` is a test-and-set,
  which is what bounds a stalled connection's mailbox at one wakeup however long
  it stalls; the deadline is `System.monotonic_time(:millisecond)`, so the stream
  that writes it and the watcher that reads it compare the same clock.

  **Why not put the deadline in the registry entry**, which the stream owns and the
  watcher already reads? `Registry.update_value/3` is the function that would do
  it, and it raises for a `keys: :duplicate` registry — which this one has to be,
  since every stream registers under one key. Even if it did not: the loop arms
  three times and disarms twice per read cycle, at up to `max_batch_frequency`
  cycles a second per connection, and `:atomics.put/3` is the cheaper write by a
  wide margin.
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

  `true` means this call flipped the flag from clear to pending — that is, that
  the caller should actually send.
  """
  @spec mark_pending(t()) :: boolean()
  def mark_pending(ref), do: :atomics.exchange(ref, @flag, 1) == 0

  @doc "Clears the flag. Called by the stream itself, before every read."
  @spec clear(t()) :: :ok
  def clear(ref), do: :atomics.put(ref, @flag, 0)

  @doc "Whether a wakeup is currently queued. For tests and telemetry."
  @spec pending?(t()) :: boolean()
  def pending?(ref), do: :atomics.get(ref, @flag) == 1

  @doc """
  Declares that the next step should be done within `budget` ms.

  Called by the stream process itself, immediately before it blocks. `:infinity`
  arms no deadline at all, which is how a log opts out of reclaiming.
  """
  @spec arm(t(), budget()) :: :ok
  def arm(ref, :infinity), do: :atomics.put(ref, @deadline, @never)

  def arm(ref, budget) when is_integer(budget) and budget > 0,
    do: :atomics.put(ref, @deadline, System.monotonic_time(:millisecond) + budget)

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
