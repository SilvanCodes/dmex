defmodule DMEx.Consumer.Queue do
  @moduledoc """
  The bounded FIFO between the socket and the handler — and the measurement.

  **The queue is not there to absorb overload.** `queue_limit` rows of slack
  absorb nothing at a sustained mismatch, and pretending otherwise would just
  relocate the problem. It is there so the process can observe two things a
  consumer holding exactly one frame cannot:

    * **depth** — how close the socket is to being held off, which is whether
      there is pressure *right now*;
    * **drain rate** — this handler's actual capacity, measured rather than
      configured (see `DMEx.Consumer.Meter`).

  It gates the socket re-arm, so the TCP receive window still closes when the
  handler falls behind and structural backpressure still holds. The slack does
  not weaken that: the re-arm still stops, just after `queue_limit` rows rather
  than one frame's worth.

  ## Three rules about what goes in

  All three exist so every number the controller reads is in one unit.

    * **It holds rows, not frames and not structs.** A row enters exactly as
      `DMEx.SSE.Frame` yielded it; turning its `data` into a message struct happens
      in the pipeline, on the way *out*. Decoding on the way in would put per-row
      JSON work on the socket path — above the bound, so unbounded by it, and
      invisible to the drain measurement even though it is real cost.

    * **Every row is admitted, including ones that will be skipped.** The module
      lookup happens in the pipeline, not at the door, because a skip still
      advances the cursor. This matters more than it looks: in v1 a subscription
      receives the producer's whole log, so most rows may be skips, and
      filtering them here would leave depth and drain rate counting *handled*
      rows while `max_batch_size` counts *wire* rows. Every use of one to
      compute the other would then need a mix ratio that changes whenever the
      producer's traffic shape does.

    * **It is FIFO, and one pipeline drains it.** Ordering within a subscription
      is a delivery guarantee and duplicate suppression is an `id > last_seen`
      comparison; both assume rows are handed on in arrival order, one at a
      time. The queue adds slack, never concurrency.

  ## The gate leaves room for a whole frame

  Rows are admitted a frame at a time, up to `max_batch_size` at once. A gate of
  "re-arm while depth is below `queue_limit`" would let a full frame land on a
  nearly-full queue and overshoot by up to a frame. So the gate is
  `depth + max_batch_size <= queue_limit`: re-arm only while a whole frame would
  still fit. At the default `2 x max_batch_size` that reads as "re-arm while
  depth is at most one frame's worth", and peak depth is exactly `queue_limit`.

  Two things fall out of that, and they are convenient rather than coincidental.
  The pressure threshold and the re-arm threshold become **the same number** —
  `max_batch_size`, half of `queue_limit` — so "depth above the high-water mark"
  and "the socket is being held off" are one condition rather than two that have
  to be kept consistent. And the trigger for adaptation is then just that
  condition sustained past one `batch_window`.

  ## It is not durable, and does not need to be

  Everything in it sits above `last_seen` with no handler having run, so
  throwing it away is indistinguishable from never having received it. The
  producer still holds those rows, and the next connect re-reads them from the
  same cursor every reconnect already resumes from.
  """

  defstruct rows: nil, depth: 0, limit: 1_000, frame: 500

  @type t :: %__MODULE__{
          rows: :queue.queue(map()),
          depth: non_neg_integer(),
          limit: pos_integer(),
          frame: pos_integer()
        }

  @doc """
  An empty queue bounded at `limit`, expecting frames of up to `frame` rows.

  `frame` is the arrival quantum — the `max_batch_size` currently in force — and
  is what the re-arm gate reserves room for.
  """
  @spec new(pos_integer(), pos_integer()) :: t()
  def new(limit, frame) when is_integer(limit) and is_integer(frame) do
    %__MODULE__{rows: :queue.new(), limit: limit, frame: frame}
  end

  @doc "Admits a frame's rows, in order."
  @spec push(t(), [map()]) :: t()
  def push(queue, rows) when is_list(rows) do
    %{
      queue
      | rows: Enum.reduce(rows, queue.rows, &:queue.in/2),
        depth: queue.depth + length(rows)
    }
  end

  @doc "Takes the oldest row, or `:empty`."
  @spec pop(t()) :: {:ok, map(), t()} | :empty
  def pop(queue) do
    case :queue.out(queue.rows) do
      {{:value, row}, rows} -> {:ok, row, %{queue | rows: rows, depth: queue.depth - 1}}
      {:empty, _rows} -> :empty
    end
  end

  @doc "How many rows are waiting."
  @spec depth(t()) :: non_neg_integer()
  def depth(%__MODULE__{depth: depth}), do: depth

  @doc "Whether anything is waiting."
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{depth: depth}), do: depth == 0

  @doc """
  Whether the socket may be re-armed: could a whole frame still fit?

  This is `depth + max_batch_size <= queue_limit`, which is what makes
  `queue_limit` a true bound rather than an approximate one.
  """
  @spec room?(t()) :: boolean()
  def room?(%__MODULE__{} = queue), do: queue.depth + queue.frame <= queue.limit

  @doc """
  The depth above which the queue counts as under pressure.

  The same number as the re-arm gate's headroom, deliberately — "depth above the
  high-water mark" and "the socket is being held off" are one condition.
  """
  @spec high_water(t()) :: pos_integer()
  def high_water(%__MODULE__{} = queue), do: queue.limit - queue.frame

  @doc """
  The depth below which pressure counts as cleared.

  Strictly below `high_water/1`, so depth hovering at one threshold cannot
  oscillate the controller — the hysteresis half of the anti-flapping story.
  """
  @spec low_water(t()) :: non_neg_integer()
  def low_water(%__MODULE__{} = queue), do: div(high_water(queue), 2)

  @doc """
  Whether the queue is under pressure, clear of it, or in between.

  The thresholds live here rather than at the call site because this module is
  what defines them — handing a controller three loose numbers invites passing
  a high-water mark from one queue and a low-water mark from another.

  `:normal` is the hysteresis band: pressure is not *raised* until depth exceeds
  the high mark and not *cleared* until it falls to the low one, so depth
  hovering at either threshold cannot oscillate a decision.
  """
  @spec pressure(t()) :: :high | :normal | :low
  def pressure(%__MODULE__{} = queue) do
    cond do
      queue.depth > high_water(queue) -> :high
      queue.depth <= low_water(queue) -> :low
      true -> :normal
    end
  end

  @doc """
  Re-sizes the arrival quantum after a re-parameterization.

  Adaptation only ever moves `max_batch_size` *below* the configured value, so
  the `limit`-to-`frame` ratio can only widen from its 2x default, and the
  invariant holds by itself once set.
  """
  @spec resize(t(), pos_integer()) :: t()
  def resize(%__MODULE__{} = queue, frame), do: %{queue | frame: frame}
end
