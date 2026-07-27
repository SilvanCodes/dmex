defmodule DMEx.Consumer.Control do
  @moduledoc """
  The AIMD law: back off fast on sustained pressure, recover slowly.

  TCP backpressure is one bit. It can say *stop*; it cannot say *send me less at
  a time*. `batch_window` and `max_batch_size` are exactly that second message,
  and they are already per-connection consumer-supplied values — so what the
  consumer needs is a way to know what to ask for, and an occasion to ask.

  Without this, a consumer that cannot keep up settles into a reconnect loop:
  the producer's `send_timeout` closes the stalled socket, the consumer
  reconnects, is handed frames at the same rate it just failed to drain, and
  stalls again. Nothing is lost, but each reconnect costs the producer a fresh
  catch-up read — **the slower the consumer, the more often it makes the
  producer work.**

  ## Pressure triggers; capacity sizes; utilization is for humans

  Adaptation fires only on depth sustained above the high-water mark for longer
  than one `batch_window` — a condition an idle or comfortably-keeping-up stream
  cannot reach, because rows leave faster than they arrive. Service time
  triggers nothing; it is read only *after* the trigger, to answer "then how
  much should I ask for?".

  ## Backing off

      max_batch_size' = clamp(drain_rate x batch_window, 1, configured)

  Ask for no more per window than the handler empties per window. `drain_rate`
  is `1 / mean_service_time`, not an observed throughput — see
  `DMEx.Consumer.Meter` for why that distinction is the whole design.

  If that computation hits the floor and pressure persists, the consumer widens
  `batch_window` instead, which lowers frame *rate* once frame *size* has
  nowhere left to go.

  ## Recovering, in the reverse order

  After several consecutive windows spent near-empty, the consumer steps back
  toward its configured pacing a notch at a time. **`batch_window` narrows back
  before `max_batch_size` grows**, and the two are not symmetric in what they
  cost while a stream is quiet: an oversized `max_batch_size` costs nothing at
  all when there is little to send — it is a `LIMIT` on the producer's read, not
  a demand — whereas a widened `batch_window` is a floor on frame rate, and so
  sits directly in the end-to-end latency of every message on an otherwise idle
  stream. A consumer that widened its window under load and then went quiet is
  paying that latency for nothing.

  ## Flapping

  Three things damp it, and this module owns two: separate high- and low-water
  marks, so depth hovering at one threshold cannot oscillate the decision, and a
  minimum interval between changes that must comfortably exceed one
  close-drain-reconnect cycle. The third is the jittered delay on the reconnect
  itself, which lives in `DMEx.Consumer.Stream`.

  ## Three limits, stated plainly

    * **Adaptation cannot make a handler faster.** If drain rate is below publish
      rate the consumer falls behind permanently, and no framing parameter
      changes that. What it converts is the *symptom*: instead of a reconnect
      loop driven by `send_timeout`, the subscription runs stably at its own
      speed and falls visibly behind — which the lag telemetry already reports.
      Visible lag is a capacity problem an operator can see; a reconnect loop is
      a capacity problem disguised as a network one.

    * **Learned values are in-memory and reset on restart.** Persisting them
      alongside `last_seen` was rejected: the right pacing is a property of the
      current host, load and handler, not of the subscription, and a value
      pinned during one bad afternoon would quietly throttle it forever.

    * **The producer never initiates any of this.** It holds no consumer state
      and has no channel to push a rate hint down. The only producer-side inputs
      are its clamp bounds and the values it echoes back.
  """

  alias DMEx.Config
  alias DMEx.Consumer.Meter

  defstruct pressure_since: nil, quiet_since: nil, changed_at: nil

  @type t :: %__MODULE__{
          pressure_since: integer() | nil,
          quiet_since: integer() | nil,
          changed_at: integer() | nil
        }

  @typedoc "A pacing pair, either requested or granted."
  @type pacing :: %{batch_window: pos_integer(), max_batch_size: pos_integer()}

  @typedoc """
  Everything a decision depends on.

  `:granted` is what the producer actually applied, from its echo — decisions
  are made against that rather than against `:requested`, or the controller
  adapts against a number that never took effect and concludes its own
  arithmetic is broken.
  """
  @type context :: %{
          meter: Meter.t(),
          granted: pacing(),
          requested: pacing(),
          config: Config.Consumer.t(),
          now: integer()
        }

  @doc "A controller that has seen nothing yet."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  Folds one observation in.

  Called after every push and every drain, so the controller sees pressure as it
  actually moves rather than as a periodic sample. The verdict comes from
  `DMEx.Consumer.Queue.pressure/1`, which owns the thresholds — what this needs
  to know is only how long a state has held, not what the marks are.
  """
  @spec observe(t(), :high | :normal | :low, integer()) :: t()
  def observe(control, :high, now) do
    %{control | pressure_since: control.pressure_since || now, quiet_since: nil}
  end

  def observe(control, :low, now) do
    %{control | pressure_since: nil, quiet_since: control.quiet_since || now}
  end

  # The hysteresis band: no longer pressured, not yet quiet enough to recover.
  def observe(control, :normal, _now) do
    %{control | pressure_since: nil}
  end

  @doc """
  Decides whether to re-parameterize, and to what.

  Returns `{:hold, control}` or `{:adapt, requested, control}`, where `requested`
  is what the next connect should ask for. Pure: everything it needs is in the
  arguments, including `now`.

  See `t:context/0` for what it reads.
  """
  @spec decide(t(), context()) :: {:hold, t()} | {:adapt, pacing(), t()}
  def decide(control, %{
        meter: meter,
        granted: granted,
        requested: requested,
        config: config,
        now: now
      }) do
    cond do
      not config.adaptive ->
        {:hold, control}

      too_soon?(control, config, now) ->
        {:hold, control}

      backing_off?(control, granted, now) ->
        back_off(control, meter, granted, requested, config, now)

      recovering?(control, granted, config, now) ->
        recover(control, requested, config, now)

      true ->
        {:hold, control}
    end
  end

  ## Triggers

  # `adaptive: false` pins the pacing, for a subscription that would rather have
  # predictable framing than an automatically-fitted one.
  defp too_soon?(%{changed_at: nil}, _config, _now), do: false

  defp too_soon?(control, config, now) do
    now - control.changed_at < config.reparameterize_interval
  end

  # Sustained past one window, so a single burst does not trip it: the producer
  # has had a full window to deliver a frame the consumer had no room for.
  defp backing_off?(%{pressure_since: nil}, _granted, _now), do: false

  defp backing_off?(control, granted, now) do
    now - control.pressure_since > granted.batch_window
  end

  defp recovering?(%{quiet_since: nil}, _granted, _config, _now), do: false

  defp recovering?(control, granted, config, now) do
    now - control.quiet_since >= config.recovery_windows * granted.batch_window
  end

  ## Backing off

  defp back_off(control, meter, granted, requested, config, now) do
    case Meter.drain_rate(meter) do
      # Not warm enough to size anything. Hold the configured pacing and let the
      # queue fill or not — re-parameterizing off two or three samples is how a
      # control loop ends up chasing its own warm-up.
      nil ->
        {:hold, control}

      rate ->
        if Meter.warm?(meter) do
          apply_back_off(control, rate, granted, requested, config, now)
        else
          {:hold, control}
        end
    end
  end

  defp apply_back_off(control, rate, granted, requested, config, now) do
    target = max(trunc(rate * granted.batch_window), 1)
    size = min(target, config.max_batch_size)

    cond do
      size < requested.max_batch_size ->
        change(control, %{requested | max_batch_size: size}, now)

      # Frame size has nowhere left to go, so lower frame *rate* instead.
      requested.max_batch_size <= 1 and requested.batch_window < config.batch_window_ceiling ->
        window = min(requested.batch_window * 2, config.batch_window_ceiling)

        change(control, %{requested | batch_window: window}, now)

      true ->
        {:hold, control}
    end
  end

  ## Recovering

  defp recover(control, requested, config, now) do
    cond do
      # Reverse order: the window is what costs latency on a quiet stream, so it
      # is the one worth reclaiming first.
      requested.batch_window > config.batch_window ->
        window = max(requested.batch_window - config.batch_window, config.batch_window)

        change(control, %{requested | batch_window: window}, now)

      requested.max_batch_size < config.max_batch_size ->
        step = max(div(config.max_batch_size * config.recovery_percent, 100), 1)
        size = min(requested.max_batch_size + step, config.max_batch_size)

        change(control, %{requested | max_batch_size: size}, now)

      true ->
        {:hold, control}
    end
  end

  defp change(control, requested, now) do
    {:adapt, requested, %{control | changed_at: now, pressure_since: nil, quiet_since: nil}}
  end
end
