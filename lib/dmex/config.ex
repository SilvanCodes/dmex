defmodule DMEx.Config do
  @moduledoc """
  Every default DMEx ships, in one place.

  DMEx reads **no global application config**. A log's settings are `use`
  options on its log module, a subscription's are `use DMEx.Handler` or
  `DMEx.Supervisor.subscribe/2` options, and both are resolved exactly once —
  never per append, per request, or per row.

  Nothing is negotiated: every setting has exactly one owner — ARCHITECTURE's
  "pacing has one owner".

  ## Log defaults

  | setting | default | why |
  | --- | --- | --- |
  | `max_batch_frequency` | `100` | reads per second. A hundred puts added latency (`1 / 2f`) at 5 ms, low enough to be invisible next to the network hop. The first setting to lower when many streams share one pool — [The deployment guide](deployment.md) has the pool arithmetic. Any positive number: `0.2` is one read every five seconds. |
  | `max_batch_rows` | `500` | rows per read. Bounds the **per-row** heap cost — about a kilobyte each once decoded and framed, which no byte budget can see — and bounds the running sum enforcing `max_batch_bytes`. [The deployment guide](deployment.md) sizes it against `stall_timeout`, since a write does not complete until the slowest handler has drained it. |
  | `max_batch_bytes` | `1_048_576` | **payload** bytes per read, enforced inside the query (see `c:DMEx.Storage.read/5`, which also covers the single oversized row). Bounds the half of the heap cost that scales with message size: 500 rows is half a megabyte at 1 KB each and half a gigabyte at 1 MB each. |
  | `heartbeat_interval` | `15_000` | ms; half the most common 30 s proxy idle timeout, so two heartbeats fit inside every window. |
  | `poll_interval` | `1_000` | ms; Oban stager parity. The worst-case wakeup latency when a `NOTIFY` is lost, costing one `max(id)` index descent per app per second. |
  | `stall_timeout` | `30_000` | ms; the budget for any one blocking step of the send loop. Generous on purpose: killing a healthy slow handler costs a reconnect and a fresh catch-up read, where a wedged connection lingering 30 s costs one request process. [The deployment guide](deployment.md) sizes it against link speed. `:infinity` runs no watcher at all. |
  | `stall_check_interval` | `1_000` | ms; how often `DMEx.Log.Watcher` sweeps, so a stall is reclaimed within `stall_timeout + stall_check_interval`. A sweep is one atomics read per connection and no query. |

  `max_batch_rows` and `max_batch_bytes` are not two spellings of one limit —
  ARCHITECTURE's "producer memory is one batch" has the two heap costs and the
  arithmetic. The row bound is also what makes the byte bound cheap: a running
  sum needs a bounded input, or it visits every row after the cursor to find
  where to stop.

  ## Handler defaults

  | setting | default | why |
  | --- | --- | --- |
  | `last_seen` | `nil` | the cursor a subscription starts from when it has no durable one yet. Ignored once one exists. |
  | `checkpoint_interval` | `1_000` | ms; the longest a coalesced run of *skipped* rows may go unpersisted. Skips run no handler, so this bounds only how much re-skipping a crash costs. A handled message commits its own advance immediately. |
  | `on_failure` | `:dead_message` | what happens after local retries are exhausted. Recording the message and moving on keeps one poison message from wedging a subscription, and from vanishing; `:crash` is there when a stuck subscription is the better failure. |
  | `max_attempts` | `3` | total attempts — enough to ride out a transient deadlock, few enough that a poison message reaches its terminal action in under a second of backoff. |
  | `retry_backoff_base` | `100` | ms; base for `100 × 2^(attempt - 1)` with full jitter. |
  | `retry_backoff_max` | `5_000` | ms; cap on that schedule. |
  | `reconnect_backoff_base` | `500` | ms; base for the jittered reconnect schedule, and the flat jittered pause `on_failure: :crash` takes before re-raising. Bounds a crash-loop's reconnect rate against a *different application's* database. |
  | `reconnect_backoff_max` | `30_000` | ms; cap on that schedule. |
  | `max_frame_bytes` | `4_194_304` | bytes; the largest unterminated frame the decoder buffers. A frame is **one row**, so this has to exceed the largest single message any log sends, not its whole batch. Exceeding it drops the connection, which costs only a re-read. |
  | `missed_heartbeats` | `3` | silences of the log's advertised interval before giving up — long enough that one GC pause on the log is not a disconnect. |

  `retry_backoff_base`/`retry_backoff_max` and
  `reconnect_backoff_base`/`reconnect_backoff_max` are validated independently and
  deliberately not against each other: the cap always wins in `DMEx.backoff/3`,
  so the pair is unordered. A `max` below its `base` is not an error — it is a
  constant delay of `max`, with the base ignored.

  Whether the pacing defaults are right *under real load* is an open question —
  see ARCHITECTURE's *Open questions*.

  ### `last_seen`

  A **cursor**, in the same units and the same domain as the durable
  `dmex_cursors.last_seen` it stands in for and as the `Last-Event-ID` it goes
  out as: exclusive, so delivery starts *after* it. Two forms:

    * **a non-negative id** — deliver everything above it. `0` is the whole log,
      since ids start at 1.
    * **`nil`** — unset, and the default: from now on. The subscription sends no
      `last-event-id`, and the log answers an absent one with its tip, so
      nothing already appended is delivered. "Now" is the log's, resolved on
      every connect until the first message is handled — so a subscription that
      reconnects before handling anything does not see what was appended while
      it was down. That is what "from now on" means with no cursor; a
      subscription that must not miss those names a cursor instead.
  """

  # Each setting is a default and the domain it has to stay inside, in one
  # place. Two lists — one of defaults, one of field names to check — is how a
  # setting gets added to the first and forgotten in the second, which is a
  # validator that silently does not run.
  @log_settings [
    max_batch_frequency: {100, :positive_number},
    max_batch_rows: {500, :positive_integer},
    heartbeat_interval: {15_000, :positive_integer},
    poll_interval: {1_000, :positive_integer},
    max_batch_bytes: {1_048_576, :positive_integer},
    stall_timeout: {30_000, :stall_timeout},
    stall_check_interval: {1_000, :positive_integer}
  ]

  @handler_settings [
    last_seen: {nil, :cursor},
    checkpoint_interval: {1_000, :positive_integer},
    on_failure: {:dead_message, {:in, [:crash, :skip, :dead_message]}},
    max_attempts: {3, :positive_integer},
    retry_backoff_base: {100, :positive_integer},
    retry_backoff_max: {5_000, :positive_integer},
    reconnect_backoff_base: {500, :positive_integer},
    reconnect_backoff_max: {30_000, :positive_integer},
    max_frame_bytes: {4_194_304, :positive_integer},
    missed_heartbeats: {3, :positive_integer}
  ]

  @typedoc "A log's resolved configuration."
  @type log :: %{
          max_batch_frequency: number(),
          max_batch_rows: pos_integer(),
          heartbeat_interval: pos_integer(),
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
          retry_backoff_base: pos_integer(),
          retry_backoff_max: pos_integer(),
          reconnect_backoff_base: pos_integer(),
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
    @log_settings |> cast!(opts) |> validate!(@log_settings)
  end

  @doc """
  Builds a validated handler config from `subscribe/2` options.

  Ignores keys that are not config settings. As with `log!/1`, rejecting a
  misspelled key is the caller's job; `use DMEx.Handler` and
  `DMEx.Supervisor.subscribe/2` both do it.
  """
  @spec handler!(keyword()) :: handler()
  def handler!(opts) when is_list(opts) do
    @handler_settings |> cast!(opts) |> validate!(@handler_settings)
  end

  @doc """
  The effective heartbeat interval a log advertises, in ms.

  `heartbeat_interval + batch_interval(config)`, because the loop's throttle runs
  *after* the heartbeat branch — so the sum, not the interval, is the real upper
  bound on silence. It travels as SSE's `retry:`.
  """
  @spec effective_heartbeat(log()) :: pos_integer()
  def effective_heartbeat(config), do: config.heartbeat_interval + batch_interval(config)

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
  def log_keys, do: Keyword.keys(@log_settings)

  @doc "Every handler setting name."
  @spec handler_keys() :: [atom()]
  def handler_keys, do: Keyword.keys(@handler_settings)

  @doc """
  Every default, as `%{log: keyword(), handler: keyword()}`.

  So the tables above can be tested against the code rather than trusted.
  """
  @spec defaults() :: %{log: keyword(), handler: keyword()}
  def defaults, do: %{log: sorted_defaults(@log_settings), handler: sorted_defaults(@handler_settings)}

  defp sorted_defaults(settings), do: settings |> Enum.map(fn {key, {default, _}} -> {key, default} end) |> Enum.sort()

  ## Casting

  # Only known keys are copied, so an unknown one cannot become a setting. Later
  # duplicates win, so a caller may prepend its own defaults to an option list.
  defp cast!(settings, opts) do
    defaults = Map.new(settings, fn {key, {default, _domain}} -> {key, default} end)

    Enum.reduce(opts, defaults, fn {key, value}, acc ->
      if Map.has_key?(acc, key), do: %{acc | key => value}, else: acc
    end)
  end

  ## Validation

  # Driven by the same list the defaults come from, so a setting cannot be added
  # with a default and no domain.
  defp validate!(config, settings) do
    for {key, {_default, domain}} <- settings do
      value = Map.fetch!(config, key)

      if !in_domain?(value, domain) do
        raise ArgumentError, "#{inspect(key)} must be #{describe(domain)}, got: #{inspect(value)}"
      end
    end

    config
  end

  # `max_batch_frequency` is not a count, so it may be fractional: a stream
  # reading once every five seconds is 0.2 Hz and has no integer spelling.
  defp in_domain?(value, :positive_number), do: is_number(value) and value > 0
  defp in_domain?(value, :positive_integer), do: is_integer(value) and value > 0

  # The one setting that may be switched off rather than tuned: a log whose
  # streams legitimately block longer than any budget worth naming is better off
  # with no watcher than a wrong one.
  defp in_domain?(:infinity, :stall_timeout), do: true
  defp in_domain?(value, :stall_timeout), do: in_domain?(value, :positive_integer)

  defp in_domain?(nil, :cursor), do: true
  defp in_domain?(value, :cursor), do: is_integer(value) and value >= 0

  defp in_domain?(value, {:in, allowed}), do: value in allowed

  defp describe(:positive_number), do: "a positive number"
  defp describe(:positive_integer), do: "a positive integer"
  defp describe(:stall_timeout), do: "a positive integer or :infinity"
  defp describe(:cursor), do: "a non-negative integer cursor or nil"
  defp describe({:in, allowed}), do: "one of #{inspect(allowed)}"
end
