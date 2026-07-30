defmodule DMEx.Config do
  @moduledoc """
  Every default DMEx ships, in one place.

  DMEx reads **no global application config**. A producer's settings are `use`
  options on its producer module, a subscription's are `subscribe/2` options,
  and both are resolved exactly once — at compile time or at subscription
  start — never per publish, per request, or per row.

  A resolved config is a plain map, and the defaults below are the whole of the
  vocabulary: `producer!/1` and `consumer!/1` copy across only keys they know
  and validate what they copied, so an unrecognised key cannot silently become
  a setting.

  Nothing is negotiated. A producer paces its own connections and a consumer
  configures its own retries and liveness, so every setting below has exactly
  one owner and exactly one lifetime — there is no "requested" value anywhere to
  drift from an "effective" one.

  ## Producer defaults

  | setting | default | why |
  | --- | --- | --- |
  | `batch_window` | `200` | ms; sub-perceptible added latency while capping any one connection at five reads per second. |
  | `max_batch_size` | `500` | rows; at a ~1 KB row this is a ~500 KB write, comfortably under `write_max_bytes`. It also has to satisfy the sizing rule in `guides/deployment.md`: one write must drain in well under `stall_timeout` at the slowest consumer's rate. |
  | `heartbeat_ms` | `15_000` | ms; half the most common 30 s proxy idle timeout, so two heartbeats fit inside every window. |
  | `poll_interval` | `1_000` | ms; Oban stager parity. This is the worst-case wakeup latency when a `NOTIFY` is lost, and it costs one `max(id)` index descent per app per second. |
  | `write_max_bytes` | `1_048_576` | bytes; bounds any single outstanding write so `stall_timeout` can be tight without false-positiving on a large write to a slow-but-alive consumer. |
  | `stall_timeout` | `30_000` | ms; the budget for any one blocking step of the send loop — a write, a read, or the wait around either. Generous on purpose: this is on by default, and killing a healthy slow consumer costs a reconnect and a fresh catch-up read, where a wedged connection lingering 30 s costs one request process. At the 1 MiB write cap it tolerates a consumer draining a full write at ~280 kbit/s. `:infinity` runs no watcher at all. |
  | `stall_check_interval` | `1_000` | ms; how often `DMEx.Producer.Watcher` sweeps, so a stall is reclaimed within `stall_timeout + stall_check_interval`. A sweep is one atomics read per open connection and no query, so this is cheap enough to leave at a second. |

  ## Consumer defaults

  | setting | default | why |
  | --- | --- | --- |
  | `start` | `:beginning` | where a subscription begins when it has no cursor yet. Ignored once one exists. |
  | `checkpoint_interval` | `1_000` | ms; the longest a coalesced run of *skipped* rows may go unpersisted. Skips run no handler, so their advance is atomic with nothing and batching it is invisible — this bounds only how much re-skipping a crash costs. A handled message still commits its own advance immediately, so this never weakens the handler/cursor guarantee. |
  | `on_failure` | `:dead_message` | what happens after local retries are exhausted. Recording the message and moving on keeps one poison message from wedging a subscription, and keeps it from vanishing either — `:crash` is there when a stuck subscription is the louder, and therefore better, failure. |
  | `max_attempts` | `3` | total attempts, so the initial call plus two retries — enough to ride out a transient deadlock, few enough that a poison message reaches its terminal action in under a second of backoff. |
  | `retry_backoff` | `100` | ms; base for `100 × 2^(attempt - 1)` with full jitter. |
  | `retry_backoff_max` | `5_000` | ms; cap on that schedule. |
  | `reconnect_backoff` | `500` | ms; base for the jittered reconnect schedule. Bounds a crash-loop's reconnect rate against a *different application's* database. |
  | `reconnect_backoff_max` | `30_000` | ms; cap on that schedule. |
  | `max_frame_bytes` | `4_194_304` | bytes; the largest unterminated frame the decoder will buffer. Must exceed any producer's `write_max_bytes` with headroom, since a consumer cannot know a third party's. Exceeding it drops the connection, which costs only a re-read. |
  | `missed_heartbeats` | `3` | silences of `heartbeat_ms + batch_window` before giving up — long enough that one GC pause on the producer is not a disconnect. |

  ## Where these numbers came from

  The design fixes the *mechanisms* and leaves every constant to be chosen. The
  values above are those choices; each row's "why" is the reasoning, and any of
  them can be overridden per producer or per subscription.

  Whether they are right *under real load* is still open — most have only been
  exercised against benchmarks and a deliberately slow test handler.
  """

  @producer_defaults [
    batch_window: 200,
    max_batch_size: 500,
    heartbeat_ms: 15_000,
    poll_interval: 1_000,
    write_max_bytes: 1_048_576,
    stall_timeout: 30_000,
    stall_check_interval: 1_000
  ]

  @consumer_defaults [
    start: :beginning,
    checkpoint_interval: 1_000,
    on_failure: :dead_message,
    max_attempts: 3,
    retry_backoff: 100,
    retry_backoff_max: 5_000,
    reconnect_backoff: 500,
    reconnect_backoff_max: 30_000,
    max_frame_bytes: 4_194_304,
    missed_heartbeats: 3
  ]

  @typedoc "A producer's resolved configuration."
  @type producer :: %{
          batch_window: pos_integer(),
          max_batch_size: pos_integer(),
          heartbeat_ms: pos_integer(),
          poll_interval: pos_integer(),
          write_max_bytes: pos_integer(),
          stall_timeout: pos_integer() | :infinity,
          stall_check_interval: pos_integer()
        }

  @typedoc "A subscription's resolved configuration."
  @type consumer :: %{
          start: :beginning | :latest | non_neg_integer(),
          checkpoint_interval: pos_integer(),
          on_failure: :crash | :skip | :dead_message,
          max_attempts: pos_integer(),
          retry_backoff: pos_integer(),
          retry_backoff_max: pos_integer(),
          reconnect_backoff: pos_integer(),
          reconnect_backoff_max: pos_integer(),
          max_frame_bytes: pos_integer(),
          missed_heartbeats: pos_integer()
        }

  @doc """
  Builds a validated producer config from `use DMEx.Producer` options.

  Ignores keys that are not config settings — `:repo`, `:storage`, `:app` and
  `:prefix` are the producer module's own concerns — so the caller can hand its
  whole option list over without splitting it first. Rejecting a *misspelled*
  key is therefore the caller's job, and `DMEx.Producer` does it by checking the
  option list against its own keys plus `producer_keys/0` before calling here.

  Raises `ArgumentError` on a value outside its setting's domain.
  """
  @spec producer!(keyword()) :: producer()
  def producer!(opts) when is_list(opts) do
    @producer_defaults
    |> cast!(opts)
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
  @spec consumer!(keyword()) :: consumer()
  def consumer!(opts) when is_list(opts) do
    @consumer_defaults
    |> cast!(opts)
    |> validate_consumer!()
  end

  @doc """
  The effective heartbeat interval a producer advertises, in ms.

  This is `heartbeat_ms + batch_window`, not `heartbeat_ms`, because the loop's
  throttle runs *after* the heartbeat branch — so the sum, not the interval, is
  the real upper bound on silence. It travels as SSE's `retry:` field, which
  means a generic client reads it as a reconnect delay and a DMEx consumer reads
  it as "expect a frame or a comment within this window".

  Both halves are producer configuration, so every connection to one producer is
  advertised the same interval.
  """
  @spec effective_heartbeat(producer()) :: pos_integer()
  def effective_heartbeat(config) do
    config.heartbeat_ms + config.batch_window
  end

  @doc "Every producer setting name."
  @spec producer_keys() :: [atom()]
  def producer_keys, do: Keyword.keys(@producer_defaults)

  @doc "Every consumer setting name."
  @spec consumer_keys() :: [atom()]
  def consumer_keys, do: Keyword.keys(@consumer_defaults)

  @doc """
  Every default, as `%{producer: keyword(), consumer: keyword()}`.

  Exists so the documentation tables above can be tested against the code rather
  than trusted.
  """
  @spec defaults() :: %{producer: keyword(), consumer: keyword()}
  def defaults do
    %{producer: Enum.sort(@producer_defaults), consumer: Enum.sort(@consumer_defaults)}
  end

  ## Casting

  # Only known keys are copied, so a caller may hand over its whole option list
  # and an unknown one cannot become a setting by accident.
  defp cast!(defaults, opts) do
    Enum.reduce(opts, Map.new(defaults), fn {key, value}, acc ->
      if Map.has_key?(acc, key), do: %{acc | key => value}, else: acc
    end)
  end

  ## Validation

  defp validate_producer!(config) do
    for field <- [
          :batch_window,
          :max_batch_size,
          :heartbeat_ms,
          :poll_interval,
          :write_max_bytes,
          :stall_check_interval
        ] do
      positive_integer!(config, field)
    end

    validate_stall_timeout!(config.stall_timeout)

    config
  end

  defp validate_consumer!(config) do
    for field <- [
          :max_attempts,
          :retry_backoff,
          :retry_backoff_max,
          :reconnect_backoff,
          :reconnect_backoff_max,
          :max_frame_bytes,
          :missed_heartbeats,
          :checkpoint_interval
        ] do
      positive_integer!(config, field)
    end

    validate_start!(config.start)
    validate_inclusion!(:on_failure, config.on_failure, [:crash, :skip, :dead_message])

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
