defmodule DMEx.Producer.Pending do
  @moduledoc """
  At most one queued wakeup per connection.

  Wakeups are the last place in the producer where memory could grow without
  bound. A stalled connection is blocked inside `chunk/2` and therefore cannot
  service its mailbox, while publishes keep arriving — so `:wakeup` messages
  would accumulate at the publish rate for as long as the stall lasts.

  `send_timeout` already bounds that at `publish_rate x send_timeout` per
  connection, which is survivable but not free: a partition that stalls many
  connections at once multiplies it by the number of them, exactly when the
  producer is least able to absorb it. It is also the one bound that depends on
  a tunable being set correctly rather than on structure.

  Since a wakeup carries no information, N of them and one of them are the same
  instruction. So each connection gets a one-bit flag, and the dispatcher only
  sends when it flips from clear to pending. Mailbox depth per connection
  becomes O(1) rather than O(publish rate x stall duration), however long a
  stall lasts and however many connections stall together.

  ## The flag lives in the connection's registry entry

  It is an `:atomics` reference, created by the stream and stored as the value
  of its `DMEx.Registry` entry. That placement does the cleanup for free: the
  entry dies with the request process, and the flag with it. There is no table
  to own, no process to own it, and no monitor watching for connections that
  died while pending — a connection cannot outlive its own flag, or the other
  way round.

  `:atomics.exchange/3` is what makes `flip/1` a test-and-set rather than a read
  followed by a write, so two dispatchers cannot both conclude they should send.

  ## Clear before the read, never after

  `DMEx.Producer.Stream` clears its flag *before* issuing its read. A publish
  landing between the clear and the read re-sets the flag and enqueues a wakeup,
  so the loop runs again and performs a possibly-empty read. **Spurious wakeups
  are harmless; missed ones would not be, and this ordering makes them
  impossible.** Clearing after the read would open a window where a publish is
  both invisible to the read and unable to set the flag.
  """

  @typedoc "One connection's wakeup flag."
  @opaque t :: :atomics.atomics_ref()

  @doc "A fresh flag, clear."
  @spec new() :: t()
  def new, do: :atomics.new(1, [])

  @doc """
  Marks the connection as having a wakeup queued, and says whether it already did.

  Returns `true` when this call flipped the flag from clear to pending — that
  is, when the caller should actually send.
  """
  @spec flip(t()) :: boolean()
  def flip(ref), do: :atomics.exchange(ref, 1, 1) == 0

  @doc "Clears the flag. Called by the stream itself, before every read."
  @spec clear(t() | nil) :: :ok
  def clear(nil), do: :ok

  def clear(ref) do
    :atomics.put(ref, 1, 0)

    :ok
  end

  @doc "Whether a wakeup is currently queued. For tests and telemetry."
  @spec pending?(t()) :: boolean()
  def pending?(ref), do: :atomics.get(ref, 1) == 1
end
