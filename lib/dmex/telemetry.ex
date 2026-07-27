defmodule DMEx.Telemetry do
  @moduledoc """
  Every `:telemetry` event DMEx emits.

  This module has no functions. It exists so the event vocabulary is written
  down in one place and versioned with the code — renaming an event is a
  breaking change for every dashboard and alert built on it, and that should be
  visible in a diff rather than discovered in production.

  ## Producer

  ### `[:dmex, :publish, :start | :stop | :exception]`

  A `:telemetry.span/3` around the append. Metadata: `:producer`,
  `:message_type`.

  The one to alert on is `:exception` — a publish that raises is a message the
  application believes it emitted and did not.

  ### `[:dmex, :stream, :connect]`

  A consumer opened a stream. Measurements: `:cursor`, the resolved starting
  position. Metadata: `:producer`, `:batch_window`, `:max_batch_size` — the
  values actually applied after clamping, so this is where to see whether
  consumers are asking for pacing the producer refuses.

  ### `[:dmex, :stream, :frame]`

  One frame written to one connection. Measurements: `:rows`, `:bytes`, and
  `:lag` — the log's tip minus the highest id in the frame, so a persistently
  non-zero `:lag` is a consumer falling behind. Metadata: `:producer`,
  `:truncated` (whether the frame byte cap cut the batch short).

  ### `[:dmex, :stream, :heartbeat]`

  A `: hb` comment frame on an idle connection. Metadata: `:producer`. Useful
  mainly as a liveness signal for the producer's own monitoring: heartbeats
  stopping while connections remain open means loops are wedged.

  ### `[:dmex, :dispatcher, :broadcast]`

  A publish (or a poll that saw one) woke the connections. Measurements:
  `:streams`, how many were registered, and `:woken`, how many actually
  received a wakeup — the difference is coalescing at work. Metadata:
  `:producer`.

  ## Consumer

  ### `[:dmex, :consumer, :message, :start | :stop | :exception]`

  A `:telemetry.span/3` around **one attempt** at one message: the handler call
  and the cursor advance, in their transaction. Metadata: `:handler`,
  `:message_type`, `:url`, `:id`, `:attempt`.

  A retried message emits one span per attempt, so `:exception` events with
  `attempt: 1` followed by a `:stop` with `attempt: 2` is a transient failure
  recovering. The `:stop` duration is the number Stage 3's adaptive controller
  will use as its service-time sample, which is why the span covers an attempt
  rather than the whole retry sequence — backoff is not capacity.

  ### `[:dmex, :consumer, :skip]`

  A row whose `message_type` matched no declared module. Measurements: `:id`.
  Metadata: `:handler`, `:message_type`, `:url`.

  **Worth a dashboard even though it is the normal case.** In v1 a subscription
  receives the producer's entire log, so skips are expected — but a consumer
  silently discarding most of a stream and one silently discarding the single
  type it wanted look identical without this.

  ### `[:dmex, :consumer, :failed]`

  A message that exhausted its local retries. Measurements: `:id`,
  `:attempts`. Metadata: `:handler`, `:message_type`, `:url`, `:on_failure`
  (which terminal action was taken), `:reason`.

  Emitted for `:skip` and `:dead_message`. `:crash` re-raises instead, so the
  supervisor's own report is the record there.

  ### `[:dmex, :consumer, :lag]`

  Per frame received. Measurements: `:lag` (the producer's tip minus the last id
  in the frame), `:rows`, and `:utilization`. Metadata: `:handler`, `:url`.

  `:lag` is the "am I falling behind" signal, and the consumer-side twin of
  `[:dmex, :stream, :frame]`'s. **`:utilization` is what it cannot tell you**: a
  consumer keeping up with room to spare and one keeping up with none both
  report zero lag, and they are the difference between healthy and one traffic
  increment away from falling behind. It is the fraction of a recent window the
  pipeline had anything to do — near 1 with low lag is the one to watch.

  Reported, never acted on. Adaptation triggers on queue depth alone.

  ### `[:dmex, :consumer, :socket]`

  The socket was gated off or re-armed. Measurements: `:depth`. Metadata:
  `:handler`, `:url`, `:mode` (`:passive` or `:active`).

  `:passive` means the TCP receive window is closing because this consumer
  stopped re-arming — structural backpressure working, not a fault. Sustained
  time in `:passive` is what precedes a re-parameterization.

  ### `[:dmex, :consumer, :reparameterize]`

  The controller decided to ask for different pacing. Measurements:
  `:batch_window`, `:max_batch_size` (the new request) and `:depth` at the
  moment of the decision. Metadata: `:handler`, `:url`, `:mean_service_ms` —
  the measured capacity the new size was computed from.

  Each of these costs a close, a local drain and a reconnect, so a healthy
  subscription emits a handful over its lifetime. A steady stream of them is
  flapping, and the thing to look at is `mean_service_ms` — a handler whose cost
  swings wildly gives the controller a moving target.

  ### `[:dmex, :consumer, :reconnect]`

  Measurements: `:attempt`, `:delay`. Metadata: `:handler`, `:url`, `:reason`.

  A steadily climbing `:attempt` is a subscription that cannot connect;
  `:reason` distinguishes a producer restart from a poison message crash-looping
  from a stream dropped for silence.
  """
end
