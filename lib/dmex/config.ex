defmodule DMEx.Config do
  @moduledoc """
  Every default DMEx ships, in one place.

  DMEx reads **no global application config**. A log's settings are `use`
  options on its log module, a subscription's are `use DMEx.Handler` or
  `DMEx.Supervisor.subscribe/2` options, and both are resolved exactly once —
  never per append, per request, or per row. `log!/1` and `handler!/1` copy across only keys they
  know and validate what they copied, so an unrecognised key cannot silently
  become a setting.

  Nothing is negotiated: every setting has exactly one owner, so there is no
  "requested" value anywhere to drift from an "effective" one.

  ## Log defaults

  | setting | default | why |
  | --- | --- | --- |
  | `max_batch_frequency` | `100` | reads per second — **how hard one connection may hammer the database**. A hundred puts added latency (`1 / 2f`) at 5 ms, low enough to be invisible next to the network hop. The cost is at the pool, not the connection: worst-case reads per second is `open streams x this`, so it is the first setting to lower when many streams share one pool. Measured, a 10-connection pool starts queueing checkouts somewhere between 50 and 200 busy streams at this rate — see ARCHITECTURE's *What has been measured* and `guides/deployment.md`. Any positive number: `0.2` is one read every five seconds. |
  | `max_batch_size` | `500` | rows per read. Bounds the **per-row** heap cost — about a kilobyte each once decoded and framed, which no byte budget can see — and bounds the running sum enforcing `max_batch_bytes`. Also subject to the sizing rule in `guides/deployment.md`: one write must drain in well under `stall_timeout` at the slowest handler's rate. |
  | `max_batch_bytes` | `1_048_576` | **payload** bytes per read, enforced inside the query (see `c:DMEx.Storage.read/5`). Bounds the half of the heap cost that scales with message size: 500 rows is half a megabyte at 1 KB each and half a gigabyte at 1 MB each. One row larger than this is still delivered on its own, since refusing it would wedge the stream permanently. |
  | `heartbeat_ms` | `15_000` | ms; half the most common 30 s proxy idle timeout, so two heartbeats fit inside every window. |
  | `poll_interval` | `1_000` | ms; Oban stager parity. The worst-case wakeup latency when a `NOTIFY` is lost, costing one `max(id)` index descent per app per second. |
  | `stall_timeout` | `30_000` | ms; the budget for any one blocking step of the send loop. Generous on purpose: killing a healthy slow handler costs a reconnect and a fresh catch-up read, where a wedged connection lingering 30 s costs one request process. At the 1 MiB write cap it tolerates a handler draining a full write at ~280 kbit/s. `:infinity` runs no watcher at all. |
  | `stall_check_interval` | `1_000` | ms; how often `DMEx.Log.Watcher` sweeps, so a stall is reclaimed within `stall_timeout + stall_check_interval`. A sweep is one atomics read per connection and no query. |

  `max_batch_size` and `max_batch_bytes` are not two spellings of one limit: a
  row costs heap both as payload, which the byte bound counts, and as a fixed
  ~1 KB of terms the database cannot see. At a 50-byte payload the 1 MiB budget
  admits ~21 000 rows whose fixed cost alone is ~22 MB, and the row bound is
  what closes that end. It is also what makes the byte bound cheap — a running
  sum needs a bounded input, or it visits every row after the cursor to find
  where to stop.

  ## Handler defaults

  | setting | default | why |
  | --- | --- | --- |
  | `last_seen` | `nil` | the cursor a subscription starts from when it has no durable one yet. Ignored once one exists. |
  | `checkpoint_interval` | `1_000` | ms; the longest a coalesced run of *skipped* rows may go unpersisted. Skips run no handler, so this bounds only how much re-skipping a crash costs. A handled message commits its own advance immediately. |
  | `on_failure` | `:dead_message` | what happens after local retries are exhausted. Recording the message and moving on keeps one poison message from wedging a subscription, and from vanishing; `:crash` is there when a stuck subscription is the better failure. |
  | `max_attempts` | `3` | total attempts — enough to ride out a transient deadlock, few enough that a poison message reaches its terminal action in under a second of backoff. |
  | `retry_backoff` | `100` | ms; base for `100 × 2^(attempt - 1)` with full jitter. |
  | `retry_backoff_max` | `5_000` | ms; cap on that schedule. |
  | `reconnect_backoff` | `500` | ms; base for the jittered reconnect schedule, and the flat jittered pause `on_failure: :crash` takes before re-raising. Bounds a crash-loop's reconnect rate against a *different application's* database. |
  | `reconnect_backoff_max` | `30_000` | ms; cap on that schedule. |
  | `max_frame_bytes` | `4_194_304` | bytes; the largest unterminated frame the decoder buffers. A frame is **one row**, so this has to exceed the largest single message any log sends, not its whole batch. Exceeding it drops the connection, which costs only a re-read. |
  | `missed_heartbeats` | `3` | silences of the log's advertised interval before giving up — long enough that one GC pause on the log is not a disconnect. |

  `retry_backoff`/`retry_backoff_max` and
  `reconnect_backoff`/`reconnect_backoff_max` are validated independently and
  deliberately not against each other: `DMEx.backoff/3` takes
  `min(max, base * 2 ^ (attempt - 1))`, so the pair is unordered and the cap
  always wins. A `max` below its `base` is not an error — it is a constant delay
  of `max`, with the base ignored.

  ### `last_seen`

  A **cursor**, in the same units and the same domain as the durable
  `dmex_cursors.last_seen` it stands in for and as the `Last-Event-ID` it goes
  out as: exclusive, so delivery starts *after* it. Two forms:

    * **a non-negative id** — deliver everything above it. `0` is the whole log,
      since ids start at 1. There is no conversion anywhere between this and
      what the wire carries — that is the point of naming it after the column.
    * **`nil`** — unset, and the default: from now on. The subscription sends no
      `last-event-id`, and the log answers an absent one with its tip, so
      nothing already appended is delivered. "Now" is the log's, resolved on
      every connect until the first message is handled — so a subscription that
      reconnects before handling anything does not see what was appended while
      it was down. That is what "from now on" means with no cursor; a
      subscription that must not miss those names a cursor instead.

  Whether these are right *under real load* is still open: most have only been
  exercised against benchmarks and a deliberately slow test handler.
  """

  @log_defaults [
    max_batch_frequency: 100,
    max_batch_size: 500,
    heartbeat_ms: 15_000,
    poll_interval: 1_000,
    max_batch_bytes: 1_048_576,
    stall_timeout: 30_000,
    stall_check_interval: 1_000
  ]

  @handler_defaults [
    last_seen: nil,
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

  @typedoc "A log's resolved configuration."
  @type log :: %{
          max_batch_frequency: number(),
          max_batch_size: pos_integer(),
          heartbeat_ms: pos_integer(),
          poll_interval: pos_integer(),
          max_batch_bytes: pos_integer(),
          stall_timeout: pos_integer() | :infinity,
          stall_check_interval: pos_integer()
        }

  @typedoc "A subscription's resolved configuration."
  @type handler :: %{
          last_seen: non_neg_integer() | nil,
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
  Builds a validated log config from `use DMEx.Log` options.

  Ignores keys that are not config settings, so a caller may hand over its whole
  option list. Rejecting a *misspelled* key is therefore the caller's job;
  `DMEx.Log` does it against its own keys plus `log_keys/0`.

  Raises `ArgumentError` on a value outside its setting's domain.
  """
  @spec log!(keyword()) :: log()
  def log!(opts) when is_list(opts) do
    @log_defaults
    |> cast!(opts)
    |> validate_log!()
  end

  @doc """
  Builds a validated handler config from `subscribe/2` options.

  Ignores keys that are not config settings. As with `log!/1`, rejecting a
  misspelled key is the caller's job; `use DMEx.Handler` and
  `DMEx.Supervisor.subscribe/2` both do it.
  """
  @spec handler!(keyword()) :: handler()
  def handler!(opts) when is_list(opts) do
    @handler_defaults
    |> cast!(opts)
    |> validate_handler!()
  end

  @doc """
  The effective heartbeat interval a log advertises, in ms.

  `heartbeat_ms + batch_interval(config)`, because the loop's throttle runs
  *after* the heartbeat branch — so the sum, not the interval, is the real upper
  bound on silence. It travels as SSE's `retry:`.
  """
  @spec effective_heartbeat(log()) :: pos_integer()
  def effective_heartbeat(config) do
    config.heartbeat_ms + batch_interval(config)
  end

  @doc """
  The minimum gap between one connection's reads, in ms.

  `max_batch_frequency` is stated as a rate because that is what an operator
  chooses; the send loop needs a period to sleep out, so this is where the
  reciprocal is taken — once per connection, never in the loop.

  Rounds to whole milliseconds, which is the honest resolution of
  `Process.sleep/1`: frequencies above ~2 000 Hz stop throttling at all.
  """
  @spec batch_interval(log()) :: non_neg_integer()
  def batch_interval(config), do: round(1_000 / config.max_batch_frequency)

  @doc "Every log setting name."
  @spec log_keys() :: [atom()]
  def log_keys, do: Keyword.keys(@log_defaults)

  @doc "Every handler setting name."
  @spec handler_keys() :: [atom()]
  def handler_keys, do: Keyword.keys(@handler_defaults)

  @doc """
  Every default, as `%{log: keyword(), handler: keyword()}`.

  So the tables above can be tested against the code rather than trusted.
  """
  @spec defaults() :: %{log: keyword(), handler: keyword()}
  def defaults do
    %{log: Enum.sort(@log_defaults), handler: Enum.sort(@handler_defaults)}
  end

  ## Casting

  # Only known keys are copied, so an unknown one cannot become a setting.
  defp cast!(defaults, opts) do
    Enum.reduce(opts, Map.new(defaults), fn {key, value}, acc ->
      if Map.has_key?(acc, key), do: %{acc | key => value}, else: acc
    end)
  end

  ## Validation

  defp validate_log!(config) do
    positive_number!(config, :max_batch_frequency)

    for field <- [
          :max_batch_size,
          :heartbeat_ms,
          :poll_interval,
          :max_batch_bytes,
          :stall_check_interval
        ] do
      positive_integer!(config, field)
    end

    validate_stall_timeout!(config.stall_timeout)

    config
  end

  defp validate_handler!(config) do
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

    validate_inclusion!(:on_failure, config.on_failure, [:crash, :skip, :dead_message])

    validate_last_seen!(config.last_seen)

    config
  end

  # The one setting that may be switched off rather than tuned: a log whose
  # streams legitimately block longer than any budget worth naming is better off
  # with no watcher than a wrong one.
  defp validate_stall_timeout!(:infinity), do: :ok
  defp validate_stall_timeout!(value) when is_integer(value) and value > 0, do: :ok

  defp validate_stall_timeout!(value) do
    raise ArgumentError,
          ":stall_timeout must be a positive integer or :infinity, got: #{inspect(value)}"
  end

  # `max_batch_frequency` is not a count, so it may be fractional: a stream
  # reading once every five seconds is 0.2 Hz and has no integer spelling.
  defp positive_number!(config, field) do
    case Map.fetch!(config, field) do
      value when is_number(value) and value > 0 ->
        :ok

      value ->
        raise ArgumentError,
              "#{inspect(field)} must be a positive number, got: #{inspect(value)}"
    end
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

  defp validate_last_seen!(nil), do: :ok
  defp validate_last_seen!(id) when is_integer(id) and id >= 0, do: :ok

  defp validate_last_seen!(value) do
    raise ArgumentError,
          ":last_seen must be a non-negative integer cursor or nil, got: #{inspect(value)}"
  end

  defp validate_inclusion!(field, value, allowed) do
    if value not in allowed do
      raise ArgumentError,
            "#{inspect(field)} must be one of #{inspect(allowed)}, got: #{inspect(value)}"
    end
  end
end
