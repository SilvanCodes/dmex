defmodule DMEx.Config do
  @moduledoc """
  Every default and every bound DMEx ships, in one place.

  DMEx reads **no global application config**. A producer's settings are `use`
  options on its producer module, a subscription's are `subscribe/2` options,
  and both are resolved into a struct exactly once — at compile time or at
  subscription start — never per publish, per request, or per row.

  That is deliberate. `batch_window` and `max_batch_size` are negotiated
  per-connection (see `DMEx.Producer.Params`), so "the configured value" and
  "the effective value" are genuinely different numbers with different
  lifetimes. Keeping the configured half immutable in a struct means the
  negotiated half is the only thing that ever varies, and it varies in one
  place.

  ## Producer defaults

  | setting | default | why |
  | --- | --- | --- |
  | `batch_window` | `200` | ms; sub-perceptible added latency while capping any one connection at five reads per second. |
  | `max_batch_size` | `500` | rows; at a ~1 KB row this is a ~500 KB frame, comfortably under `frame_max_bytes`. |
  | `batch_window_floor` | `50` | ms; the floor is what bounds per-connection query rate — 20 reads/s is the most a producer will fund for a consumer that keeps asking for less. |
  | `batch_window_ceiling` | `5_000` | ms; `batch_window` is part of the advertised effective heartbeat, so an unbounded window would let a consumer push its own liveness deadline past every proxy idle timeout. |
  | `max_batch_size_floor` | `1` | rows; the adaptive controller must always have something legal to converge to. Below one row the only remaining lever is `batch_window`, which is exactly the design's fallback. |
  | `max_batch_size_ceiling` | `5_000` | rows; ten times the default. Bounds one read's memory and one frame's encode time while leaving a fast consumer real headroom for catch-up. |
  | `heartbeat_ms` | `15_000` | ms; half the most common 30 s proxy idle timeout, so two heartbeats fit inside every window. |
  | `poll_interval` | `1_000` | ms; Oban stager parity. This is the worst-case wakeup latency when a `NOTIFY` is lost, and it costs one `max(id)` index descent per app per second. |
  | `frame_max_bytes` | `1_048_576` | bytes; bounds any single outstanding write so `stall_timeout` can be tight without false-positiving on a large frame to a slow-but-alive consumer. |
  | `stall_timeout` | `30_000` | ms; the budget for any one blocking step of the send loop — a write, a read, or the wait around either. Generous on purpose: this is on by default, and killing a healthy slow consumer costs a reconnect and a fresh catch-up read, where a wedged connection lingering 30 s costs one request process. At the 1 MiB frame cap it tolerates a consumer draining a full frame at ~280 kbit/s. `:infinity` runs no watcher at all. |
  | `stall_check_interval` | `1_000` | ms; how often `DMEx.Producer.Watcher` sweeps, so a stall is reclaimed within `stall_timeout + stall_check_interval`. A sweep is one atomics read per open connection and no query, so this is cheap enough to leave at a second. |

  ## Consumer defaults

  | setting | default | why |
  | --- | --- | --- |
  | `batch_window` | `200` | ms; what this subscription asks for on connect. A preference, not a setting — the producer may clamp it. |
  | `batch_window_ceiling` | `5_000` | ms; how far this subscription will widen its own window under sustained pressure. The producer has a ceiling of its own and will clamp anyway, but asking past it would leave the controller adapting against a number that never took effect. |
  | `max_batch_size` | `500` | rows; likewise a request, echoed back as whatever the producer actually applied. |
  | `queue_limit` | `nil` | rows; `nil` means derive `2 × max_batch_size`, the smallest bound that distinguishes "a frame landed while the last one drained" from genuine pressure. |
  | `start` | `:beginning` | where a subscription begins when it has no cursor yet. Ignored once one exists. |
  | `on_failure` | `:dead_message` | what happens after local retries are exhausted. Recording the message and moving on keeps one poison message from wedging a subscription, and keeps it from vanishing either — `:crash` is there when a stuck subscription is the louder, and therefore better, failure. |
  | `max_attempts` | `3` | total attempts, so the initial call plus two retries — enough to ride out a transient deadlock, few enough that a poison message reaches its terminal action in under a second of backoff. |
  | `retry_backoff` | `100` | ms; base for `100 × 2^(attempt - 1)` with full jitter. |
  | `retry_backoff_max` | `5_000` | ms; cap on that schedule. |
  | `reconnect_backoff` | `500` | ms; base for the jittered reconnect schedule. Bounds a crash-loop's reconnect rate against a *different application's* database. |
  | `reconnect_backoff_max` | `30_000` | ms; cap on that schedule. |
  | `max_frame_bytes` | `4_194_304` | bytes; must exceed any producer's frame cap with headroom, since a consumer cannot know a third party's. Exceeding it drops the connection, which costs only a re-read. |
  | `missed_heartbeats` | `3` | silences of `heartbeat_ms + batch_window` before giving up — long enough that one GC pause on the producer is not a disconnect. |
  | `adaptive` | `true` | whether this subscription re-negotiates its pacing from measured drain rate. |
  | `ewma_alpha` | `0.2` | smoothing over service-time samples; about a ten-sample memory — responsive to a genuine handler slowdown, immune to one slow row. |
  | `min_samples` | `20` | samples before the controller may act at all. Re-parameterizing off two or three is how a control loop ends up chasing its own warm-up. |
  | `sample_staleness` | `60_000` | ms without a sample after which the estimate is kept as a starting point but must be re-warmed before it may drive a change. |
  | `recovery_windows` | `3` | consecutive near-empty windows before stepping back toward the configured pacing. Back off fast, recover slowly. |
  | `recovery_percent` | `25` | percent of the configured `max_batch_size` reclaimed per recovery step. |
  | `reparameterize_interval` | `30_000` | ms between changes. Must comfortably exceed one close-drain-reconnect cycle, or the loop chases itself. |

  ## Where these numbers came from

  The design fixes the *mechanisms* and leaves every constant to be chosen. The
  values above are those choices; each row's "why" is the reasoning, and any of
  them can be overridden per producer or per subscription. Two exist only
  because their absence was a hole rather than a default: `batch_window_ceiling`
  (an unbounded window would let a consumer push its own liveness deadline past
  every proxy idle timeout) and `max_batch_size_floor` (the adaptive law must
  always have something legal to converge to).

  Whether they are right *under real load* is still open — most have only been
  exercised against benchmarks and a deliberately slow test handler.
  """

  alias DMEx.Config.Consumer
  alias DMEx.Config.Producer

  @doc """
  Builds a validated producer config from `use DMEx.Producer` options.

  Ignores keys that are not config settings — `:repo`, `:storage`, `:app` and
  `:prefix` are the producer module's own concerns — so the caller can hand its
  whole option list over without splitting it first. Rejecting a *misspelled*
  key is therefore the caller's job, and `DMEx.Producer` does it by checking the
  option list against its own keys plus `producer_keys/0` before calling here.

  Raises `ArgumentError` on a non-integer value, a non-positive value, a floor
  above its ceiling, or a default outside its own bounds.
  """
  @spec producer!(keyword()) :: Producer.t()
  def producer!(opts) when is_list(opts) do
    %Producer{}
    |> cast!(opts, producer_keys())
    |> validate_producer!()
  end

  @doc """
  Builds a validated consumer config from `subscribe/2` options.

  Ignores keys that are not config settings — `:consumer`, `:producer`,
  `:repo`, `:storage` and `:prefix` belong to the subscription rather than to
  its pacing, and the producer, id and message types belong to the handler
  module. As with `producer!/1`, rejecting a
  misspelled key is the caller's job; `DMEx.Consumer.subscribe/2` does it.
  """
  @spec consumer!(keyword()) :: Consumer.t()
  def consumer!(opts) when is_list(opts) do
    %Consumer{}
    |> cast!(opts, consumer_keys())
    |> validate_consumer!()
  end

  @doc """
  Clamps a consumer-requested pacing value to the producer's bounds.

  A well-formed value outside the producer's range is clamped, never rejected:
  the bounds exist to protect the producer, and a consumer asking for more than
  it may have should get a working stream at the permitted rate rather than a
  `400` it has no way to interpret. A value that is not well-formed never
  reaches here — `DMEx.Producer.Params` answers that with a `400`.
  """
  @spec clamp(:batch_window | :max_batch_size, integer(), Producer.t()) :: pos_integer()
  def clamp(:batch_window, value, %Producer{} = config) when is_integer(value) do
    value |> max(config.batch_window_floor) |> min(config.batch_window_ceiling)
  end

  def clamp(:max_batch_size, value, %Producer{} = config) when is_integer(value) do
    value |> max(config.max_batch_size_floor) |> min(config.max_batch_size_ceiling)
  end

  @doc """
  The effective heartbeat interval a producer advertises, in ms.

  This is `heartbeat_ms + batch_window`, not `heartbeat_ms`, because the loop's
  throttle runs *after* the heartbeat branch — so the sum, not the interval, is
  the real upper bound on silence. It travels as SSE's `retry:` field, which
  means a generic client reads it as a reconnect delay and a DMEx consumer reads
  it as "expect a frame or a comment within this window".

  It is a per-connection value: `batch_window` is negotiated, so two connections
  to the same producer can legitimately be advertised different intervals.
  """
  @spec effective_heartbeat(pos_integer(), pos_integer()) :: pos_integer()
  def effective_heartbeat(heartbeat_ms, batch_window)
      when is_integer(heartbeat_ms) and is_integer(batch_window) do
    heartbeat_ms + batch_window
  end

  @doc """
  The default `queue_limit` for a `max_batch_size`.

  Rows arrive one frame at a time, up to `max_batch_size` at once, so a bound
  that is not comfortably larger than the arrival quantum would make the socket
  re-arm gate on ordinary operation rather than on pressure. Two frames' worth
  is the smallest bound that makes the distinction meaningful.
  """
  @spec queue_limit(Consumer.t()) :: pos_integer()
  def queue_limit(%Consumer{queue_limit: nil, max_batch_size: max}), do: 2 * max
  def queue_limit(%Consumer{queue_limit: limit}), do: limit

  @doc "Every producer setting name."
  @spec producer_keys() :: [atom()]
  def producer_keys, do: fields(%Producer{})

  @doc "Every consumer setting name."
  @spec consumer_keys() :: [atom()]
  def consumer_keys, do: fields(%Consumer{})

  defp fields(struct), do: struct |> Map.from_struct() |> Map.keys()

  @doc """
  Every default, as `%{producer: keyword(), consumer: keyword()}`.

  Exists so the documentation table above can be tested against the structs
  rather than trusted.
  """
  @spec defaults() :: %{producer: keyword(), consumer: keyword()}
  def defaults do
    %{
      producer: %Producer{} |> Map.from_struct() |> Enum.sort(),
      consumer: %Consumer{} |> Map.from_struct() |> Enum.sort()
    }
  end

  ## Casting

  defp cast!(struct, opts, fields) do
    Enum.reduce(opts, struct, fn {key, value}, acc ->
      if key in fields, do: Map.put(acc, key, value), else: acc
    end)
  end

  ## Validation

  defp validate_producer!(%Producer{} = config) do
    for field <- [
          :batch_window,
          :max_batch_size,
          :batch_window_floor,
          :batch_window_ceiling,
          :max_batch_size_floor,
          :max_batch_size_ceiling,
          :heartbeat_ms,
          :poll_interval,
          :frame_max_bytes,
          :stall_check_interval
        ] do
      positive_integer!(config, field)
    end

    validate_stall_timeout!(config.stall_timeout)

    bounded!(config, :batch_window, :batch_window_floor, :batch_window_ceiling)
    bounded!(config, :max_batch_size, :max_batch_size_floor, :max_batch_size_ceiling)

    config
  end

  defp validate_consumer!(%Consumer{} = config) do
    for field <- [
          :batch_window,
          :batch_window_ceiling,
          :max_batch_size,
          :max_attempts,
          :retry_backoff,
          :retry_backoff_max,
          :reconnect_backoff,
          :reconnect_backoff_max,
          :max_frame_bytes,
          :missed_heartbeats,
          :min_samples,
          :sample_staleness,
          :recovery_windows,
          :recovery_percent,
          :reparameterize_interval
        ] do
      positive_integer!(config, field)
    end

    validate_alpha!(config.ewma_alpha)

    unless is_nil(config.queue_limit), do: positive_integer!(config, :queue_limit)

    validate_start!(config.start)
    validate_inclusion!(:on_failure, config.on_failure, [:crash, :skip, :dead_message])
    validate_inclusion!(:adaptive, config.adaptive, [true, false])

    config
  end

  # The one setting that may be switched off rather than tuned: a producer whose
  # streams legitimately block longer than any budget worth naming would rather
  # have no watcher than a wrong one, and `:infinity` says so explicitly.
  defp validate_stall_timeout!(:infinity), do: :ok
  defp validate_stall_timeout!(value) when is_integer(value) and value > 0, do: :ok

  defp validate_stall_timeout!(value) do
    raise ArgumentError,
          ":stall_timeout must be a positive integer or :infinity, got: #{inspect(value)}"
  end

  defp positive_integer!(config, field) do
    case Map.fetch!(config, field) do
      value when is_integer(value) and value > 0 ->
        :ok

      value ->
        raise ArgumentError,
              "#{inspect(field)} must be a positive integer, got: #{inspect(value)}"
    end
  end

  defp bounded!(config, field, floor_field, ceiling_field) do
    floor = Map.fetch!(config, floor_field)
    ceiling = Map.fetch!(config, ceiling_field)
    value = Map.fetch!(config, field)

    if floor > ceiling do
      raise ArgumentError,
            "#{inspect(floor_field)} (#{floor}) must not exceed " <>
              "#{inspect(ceiling_field)} (#{ceiling})"
    end

    # A default outside its own bounds is unreachable: every connection that
    # does not name a value would be clamped away from it on arrival, which is
    # a configuration mistake rather than a negotiation.
    unless value >= floor and value <= ceiling do
      raise ArgumentError,
            "#{inspect(field)} (#{value}) must lie within " <>
              "#{inspect(floor_field)}..#{inspect(ceiling_field)} (#{floor}..#{ceiling})"
    end
  end

  # A smoothing factor outside (0, 1] is not a slow filter, it is a broken one:
  # at 0 the estimate never moves, and above 1 it overshoots every sample.
  defp validate_alpha!(alpha) when is_float(alpha) and alpha > 0 and alpha <= 1, do: :ok

  defp validate_alpha!(alpha) do
    raise ArgumentError,
          ":ewma_alpha must be a float in (0, 1], got: #{inspect(alpha)}"
  end

  defp validate_start!(start) when start in [:beginning, :latest], do: :ok
  defp validate_start!(start) when is_integer(start) and start >= 0, do: :ok

  defp validate_start!(start) do
    raise ArgumentError,
          ":start must be :beginning, :latest, or a non-negative integer id, " <>
            "got: #{inspect(start)}"
  end

  defp validate_inclusion!(field, value, allowed) do
    unless value in allowed do
      raise ArgumentError,
            "#{inspect(field)} must be one of #{inspect(allowed)}, got: #{inspect(value)}"
    end
  end
end
