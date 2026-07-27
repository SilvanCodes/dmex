defmodule DMEx.Consumer.Meter do
  @moduledoc """
  How fast this handler actually is, measured rather than configured.

  ## Service time, not throughput

  The obvious measurement is messages committed per wall-clock second, and it is
  wrong — worth being precise about, because it is the one every throughput
  counter already gives you. **Committed-per-second is the arrival rate whenever
  the consumer is not saturated.** A handler capable of 5 000 rows/s that is sent
  50 reports 50; an idle connection reports zero. Neither number is a capacity,
  and feeding either into a sizing rule would shrink an idle consumer's batches
  toward the floor precisely when nothing was wrong.

  So the primitive is the duration of one row's pipeline pass: decode,
  transaction begin, `handle_message/2`, cursor write, commit. Capacity is
  `1 / mean_service_time`, and that has three properties throughput does not:

    * **It is defined at depth 1.** Service time is a property of a single row,
      so a stream that never holds more than one row still measures it. No
      backlog is required to learn how fast the handler is — which is the entire
      point, since a backlog is the condition the measurement exists to avoid.

    * **Idle contributes no samples rather than bad ones.** The EWMA is indexed
      by *sample*, not by clock. An hour of silence leaves the estimate exactly
      where the last row left it, rather than decaying it toward zero and making
      every quiet consumer re-learn its own capacity each time traffic resumes.

    * **It is the right statistic for sizing a batch.** Draining `n` rows costs
      the *sum* of `n` service times, and by linearity the expected sum is
      `n x mean` whatever the distribution's shape. Tail latency governs a single
      message's deadline, not a batch's total, so tracking a high quantile
      instead would systematically undersize batches for any handler with
      bimodal cost — one cheap message type and one expensive one, which is the
      common case rather than the exotic one.

  ## What a sample contains, and what it must not

    * **Queue wait is excluded.** Time a row spends *in* the queue is a function
      of the backlog ahead of it, not of the handler. Including it would make
      service time grow with depth — folding the pressure signal into the
      capacity signal, when the controller needs them independent: one decides
      *whether* to act, the other decides *how much*.

    * **Retry backoff is excluded, along with the failed attempts.** Those sleeps
      are wall-clock the stream spends not draining, but they are a failure mode,
      not a capacity. One poison message under exponential backoff would
      otherwise drag the estimate down by orders of magnitude and pin the
      subscription's batches at the floor long after `on_failure` had resolved
      it. **The sample is the successful pass.**

    * **Skips are sampled at their real cost.** A row with no matching module
      costs a lookup, a cursor write and a commit: cheap beside a handler, but
      not free, and genuinely paid per wire row. Since the queue admits them and
      `max_batch_size` counts them, the estimate has to price them.

  ## Utilization is for humans

  Alongside service time the meter accumulates busy time over a sliding window.
  It triggers nothing and is not in the control loop at all — it is the
  telemetry that separates "keeping up with room to spare" from "keeping up with
  none", which the lag signal cannot, because both of those are caught up.

  | signal | what it says | when the stream is idle |
  | --- | --- | --- |
  | service time | how fast this handler is | last known value, unchanged |
  | utilization | how much of that capacity is spoken for | about 0 |
  | depth | whether there is pressure right now | 0 |
  """

  alias DMEx.Config

  @window 10_000

  defstruct mean: nil,
            samples: 0,
            last_sample_at: nil,
            busy: 0,
            window_start: nil,
            alpha: 0.2,
            min_samples: 20,
            staleness: 60_000

  @type t :: %__MODULE__{
          mean: float() | nil,
          samples: non_neg_integer(),
          last_sample_at: integer() | nil,
          busy: number(),
          window_start: integer() | nil,
          alpha: float(),
          min_samples: pos_integer(),
          staleness: pos_integer()
        }

  @doc "A meter carrying this subscription's smoothing and warm-up settings."
  @spec new(Config.Consumer.t()) :: t()
  def new(%Config.Consumer{} = config) do
    %__MODULE__{
      alpha: config.ewma_alpha,
      min_samples: config.min_samples,
      staleness: config.sample_staleness
    }
  end

  @doc """
  Records one successful pipeline pass of `duration` ms.

  Call this only for a pass that committed — see the module documentation on why
  failed attempts and their backoff must not be sampled.
  """
  @spec sample(t(), number(), integer()) :: t()
  def sample(meter, duration, now) when is_number(duration) do
    meter
    |> restart_if_stale(now)
    |> update_mean(duration)
    |> accumulate_busy(duration, now)
    |> Map.put(:last_sample_at, now)
  end

  @doc """
  Rows this handler can drain per millisecond, or `nil` before it is warm.

  `nil` rather than a guess: a controller acting on an unwarmed estimate is
  chasing its own start-up.
  """
  @spec drain_rate(t()) :: float() | nil
  def drain_rate(%__MODULE__{mean: nil}), do: nil
  def drain_rate(%__MODULE__{mean: mean}) when mean <= 0, do: nil
  def drain_rate(%__MODULE__{mean: mean}), do: 1 / mean

  @doc """
  Whether the estimate may drive a re-parameterization.

  False below `min_samples`, and false again after a stale gap until the
  estimate has been re-warmed — the mean survives, the confidence does not.
  """
  @spec warm?(t()) :: boolean()
  def warm?(%__MODULE__{samples: samples, min_samples: min}), do: samples >= min

  @doc "Mean service time in ms, or `nil`."
  @spec mean(t()) :: float() | nil
  def mean(%__MODULE__{mean: mean}), do: mean

  @doc """
  The fraction of the recent window this pipeline had anything to do.

  Reported, never acted on.
  """
  @spec utilization(t(), integer()) :: float()
  def utilization(%__MODULE__{window_start: nil}, _now), do: 0.0

  def utilization(%__MODULE__{} = meter, now) do
    case now - meter.window_start do
      elapsed when elapsed > 0 -> min(meter.busy / elapsed, 1.0)
      _ -> 0.0
    end
  end

  ## Internals

  # After a long silence the estimate is stale rather than wrong, and is treated
  # that way: kept as the starting point — a handler's speed rarely changes for
  # free, and it is the only evidence there is — but re-warmed to the minimum
  # sample count before it may drive anything. That costs nothing in practice,
  # since the samples come from the very rows creating the pressure.
  defp restart_if_stale(%{last_sample_at: nil} = meter, _now), do: meter

  defp restart_if_stale(meter, now) do
    if now - meter.last_sample_at > meter.staleness do
      %{meter | samples: 0, busy: 0, window_start: now}
    else
      meter
    end
  end

  defp update_mean(%{mean: nil} = meter, duration) do
    %{meter | mean: duration * 1.0, samples: 1}
  end

  defp update_mean(meter, duration) do
    mean = meter.alpha * duration + (1 - meter.alpha) * meter.mean

    %{meter | mean: mean, samples: meter.samples + 1}
  end

  defp accumulate_busy(%{window_start: nil} = meter, duration, now) do
    %{meter | busy: duration, window_start: now}
  end

  defp accumulate_busy(meter, duration, now) do
    if now - meter.window_start > @window do
      # Slide rather than grow without bound. Utilization is a recent fraction,
      # not a lifetime average.
      %{meter | busy: duration, window_start: now}
    else
      %{meter | busy: meter.busy + duration}
    end
  end
end
