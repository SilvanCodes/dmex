defmodule DMEx.Telemetry do
  @moduledoc """
  Every `:telemetry` event DMEx emits.

  This module has no functions. It exists so the event vocabulary is written
  down in one place and versioned with the code — renaming an event is a
  breaking change for every dashboard and alert built on it, and that should be
  visible in a diff rather than discovered in production.

  ## Log

  ### `[:dmex, :append, :start | :stop | :exception]`

  A `:telemetry.span/3` around the append. Metadata: `:log`,
  `:message_type`.

  The one to alert on is `:exception` — an append that raises is a message the
  application believes it emitted and did not.

  ### `[:dmex, :stream, :connect]`

  A consumer opened a stream. Measurements: `:cursor`, the resolved starting
  position, and `:matched`, how many of the log's types a selective
  connection admitted (`nil` for a whole-log one). Metadata: `:log` and
  `:selective`.

  ### `[:dmex, :stream, :write]`

  One write to one connection — a whole read's rows, encoded as one frame each
  and handed to `chunk/2` together. Measurements: `:rows` and `:bytes`.
  Metadata: `:log`, `:truncated` (whether `max_batch_bytes` cut the batch
  short, which is `false` for a lone oversized row that went out whole).

  `:rows` sitting at `max_batch_size` write after write is a connection reading
  as fast as its pacing allows, which is what catch-up looks like.

  ### `[:dmex, :stream, :heartbeat]`

  A `: hb` comment frame on an idle connection. Metadata: `:log`. Useful
  mainly as a liveness signal for the log's own monitoring: heartbeats
  stopping while connections remain open means loops are wedged.

  ### `[:dmex, :stream, :reclaim]`

  `DMEx.Log.Watcher` killed a connection that blew its deadline —
  a consumer that stopped reading, or a loop wedged in a read that never came
  back. Measurements: `:overdue_by`, how late it was in ms. Metadata:
  `:log`.

  Occasional ones are a slow consumer and cost nothing but a reconnect; a
  steady rate means either a consumer that cannot keep up or a `stall_timeout`
  tighter than the links it serves deserve. This is the counterpart to
  `:heartbeat` above: heartbeats stopping tells you loops are wedged, and this
  tells you what was done about it.

  ### `[:dmex, :dispatcher, :broadcast]`

  An append (or a poll that saw one) woke the connections. Measurements:
  `:streams`, how many were registered, and `:woken`, how many actually
  received a wakeup — the difference is coalescing at work. Metadata:
  `:log`.

  ## Handler

  Every handler event carries **both** `:subscription` and `:handler`.
  `:subscription` is the durable identifier — `"orders/order_projection"` — and
  is what a dashboard should group and alert on, since it survives renaming the
  module. `:handler` is the module, which is what a human reads in a log line
  and what points at the code to open. They are deliberately not the same
  string, and after one refactor they usually differ.

  ### `[:dmex, :handler, :message, :start | :stop | :exception]`

  A `:telemetry.span/3` around **one attempt** at one message: the handler call
  and the cursor advance, in their transaction. Metadata: `:subscription`,
  `:handler`, `:message_type`, `:url`, `:id`, `:attempt`.

  A retried message emits one span per attempt, so `:exception` events with
  `attempt: 1` followed by a `:stop` with `attempt: 2` is a transient failure
  recovering. The span covers one attempt rather than the whole retry sequence
  because the two measure different things: an attempt's duration is what a
  handler costs, and a retry sequence's is mostly backoff.

  ### `[:dmex, :handler, :skip]`

  A row whose `message_type` matched no declared module. Measurements: `:id`.
  Metadata: `:subscription`, `:handler`, `:message_type`, `:url`.

  **Worth a dashboard even though it is not a fault.** Server-side selection
  makes it rare — but filter inclusion is heuristic in one direction, and a
  handler declared `selective: false` asks for the whole log outright. A
  consumer silently discarding most of a stream and one silently discarding the
  single type it wanted look identical without this.

  ### `[:dmex, :handler, :checkpoint]`

  A coalesced run of skipped rows was written to `dmex_cursors`. Measurements:
  `:id`, the position persisted. Metadata: `:subscription`, `:handler`, `:url`.

  Emitted only when a run was actually outstanding, so on a stream whose rows
  are all handled this never fires — a handled message's own advance is
  absolute and carries the skips below it, so there is nothing left to write.
  The rate against `[:dmex, :handler, :skip]` is the coalescing ratio: one
  checkpoint per thousand skips is the mechanism working, one per skip means
  runs are being broken up by handled rows and the stream is paying full price.

  ### `[:dmex, :handler, :failed]`

  A message that exhausted its local retries. Measurements: `:id`,
  `:attempts`. Metadata: `:subscription`, `:handler`, `:message_type`, `:url`,
  `:on_failure` (which terminal action was taken), `:reason`.

  Emitted for `:dead_message` (the default) and `:skip`. `:crash` re-raises
  instead, so the supervisor's own report is the record there.

  Worth an alert either way: under the default the subscription keeps running,
  so this event and a growing `dmex_dead_messages` are the only signs that
  something is being given up on.

  ### `[:dmex, :handler, :lag]`

  Once per socket read that carried rows — not once per row. Measurements:
  `:lag`, milliseconds between the last row's `inserted_at` and now, and
  `:rows`, how many that read carried. Metadata: `:subscription`, `:handler`,
  `:url`.

  The "am I falling behind" signal. It is in **milliseconds rather than ids**
  because the wire carries no `max_id` to subtract from — and staleness is the
  question the id difference was a proxy for anyway.

  It compares the log's clock against this one, so it is only as good as
  the two machines' NTP agreement. Fine for a seconds-scale signal, and not for
  anything finer.

  Reported, never acted on: nothing on this side adapts, because the socket's
  receive window already carries the pressure.

  There is deliberately **no socket-gating event**. Backpressure is not a
  decision this side makes and could report — it is what happens when the
  process is busy in a handler and therefore not re-arming the socket. The
  log's `[:dmex, :stream, :reclaim]` is where a consumer too slow to keep up
  becomes visible.

  ### `[:dmex, :handler, :reconnect]`

  Measurements: `:attempt`, `:delay`. Metadata: `:subscription`, `:handler`,
  `:url`, `:reason`.

  A steadily climbing `:attempt` is a subscription that cannot connect;
  `:reason` distinguishes a log restart from a poison message crash-looping
  from a stream dropped for silence.

  **Two reasons deserve an alert of their own.**
  `{:sequence_gap, expected, got}` means a frame named a predecessor this
  subscription never received — a message went missing on the wire, and the
  connection was dropped rather than allowed to step over it. It repairs itself
  if the cause was transport damage; a steady rate of it means the log is
  genuinely skipping rows, and the subscription is making no progress.
  `{:unchained_frame, id}` means a frame arrived with no predecessor at all, so
  the stream cannot be checked for gaps and is refused rather than trusted.
  """
end
