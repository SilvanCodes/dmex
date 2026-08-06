defmodule DMEx.Telemetry do
  @moduledoc """
  Every `:telemetry` event DMEx emits.

  No functions — this module exists so the event vocabulary is written down in
  one place and versioned with the code. Renaming an event is a breaking change
  for every dashboard built on it, and that should be visible in a diff.

  ## Log

  ### `[:dmex, :append, :start | :stop | :exception]`

  A `:telemetry.span/3` around the append. Metadata: `:log`, `:type`.

  The one to alert on is `:exception` — an append that raises is a message the
  application believes it emitted and did not.

  ### `[:dmex, :stream, :connect]`

  A consumer opened a stream. Measurements: `:cursor`, the resolved starting
  position, and `:matched`, how many types a selective connection admitted
  (`nil` for a whole-log one) — the application's types the filter matched over
  SSE, the handler's declared types for a local reader, which needs no
  resolution and so has no `404` for an empty one. Metadata: `:log`,
  `:selective`.

  ### `[:dmex, :stream, :write]`

  One write to one connection. Measurements: `:rows`, plus `:bytes` on a
  transport where bytes are meaningful. Metadata: `:log`. `:rows` sitting at
  `max_batch_rows` write after write is what catch-up looks like.

  ### `[:dmex, :stream, :heartbeat]`

  A liveness tick on an idle connection — a `: hb` comment frame over SSE, nothing on the wire for a local reader. Metadata: `:log`. Heartbeats
  stopping while connections remain open means loops are wedged.

  ### `[:dmex, :stream, :reclaim]`

  `DMEx.Log.Watcher` killed a connection that blew its deadline. Measurements:
  `:overdue_by` in ms. Metadata: `:log`.

  Occasional ones are a slow consumer and cost nothing but a reconnect; a steady
  rate means either a consumer that cannot keep up or a `stall_timeout` tighter
  than the links it serves deserve.

  ### `[:dmex, :dispatcher, :broadcast]`

  An append (or a poll that saw one) woke the connections. Measurements:
  `:streams` registered, `:woken` — the difference is coalescing and type
  routing at work — and `:types`, how many distinct types the burst covered.
  Metadata: `:log`.

  ## Handler

  Every handler event carries `:subscription`, `:handler` and `:origin`.
  `:subscription` is the durable identifier — `"orders/order_projection"` — and
  is what a dashboard should group and alert on, since it survives renaming the
  module. `:handler` is the module, which is what a human reads in a log line.
  After one refactor they usually differ. `:origin` is where the rows came from:
  a URL, or the module of a log this application owns.

  ### `[:dmex, :handler, :message, :start | :stop | :exception]`

  A `:telemetry.span/3` around **one attempt** at one message: the handler call
  and the cursor advance, in their transaction. Extra metadata:
  `:type`, `:id`, `:attempt`.

  One span per attempt, so an `:exception` at `attempt: 1` followed by a `:stop`
  at `attempt: 2` is a transient failure recovering. Per attempt rather than per
  sequence because an attempt's duration is what a handler costs, where a
  sequence's is mostly backoff.

  ### `[:dmex, :handler, :skip]`

  A row no clause matched: either its `type` names no declared module,
  or the type is declared but a clause narrowed to a field's value turned it
  down. Neither reached a handler, so neither is inside a `:message` span.
  Measurements: `:id`. Extra metadata: `:type`.

  **Worth a dashboard even though it is not a fault**: a consumer silently
  discarding most of a stream and one silently discarding the single type it
  wanted look identical without it.

  ### `[:dmex, :handler, :checkpoint]`

  A coalesced run of skipped rows was written to `dmex_cursors`. Measurements:
  `:cursor`, the position persisted — named for what it is, since `:id` on the
  sibling handler events is a log row's own id. Never fires on a stream whose rows are all
  handled. Its rate against `[:dmex, :handler, :skip]` is the coalescing ratio:
  one per thousand skips is the mechanism working, one per skip means runs are
  being broken up by handled rows.

  ### `[:dmex, :handler, :failed]`

  A message that exhausted its local retries. Measurements: `:id`, `:attempts`.
  Extra metadata: `:type`, `:on_failure`, `:reason`.

  Emitted for `:dead_message` and `:skip`. `:crash` re-raises instead, so the
  supervisor's own report is the record there. Worth an alert either way: under
  the default the subscription keeps running, so this event and a growing
  `dmex_dead_messages` are the only signs something is being given up on.

  There is deliberately **no socket-gating event and no lag event**. Backpressure
  is not a decision this side makes, it is what happens when the process is busy
  in a handler and therefore not re-arming the socket. And "how far behind am I"
  is not a question the wire can answer — it carries no `max_id` to subtract
  from, and no clock.

  ### `[:dmex, :handler, :reconnect]`

  Measurements: `:attempt`, `:delay`. Extra metadata: `:reason`.

  A steadily climbing `:attempt` is a subscription that cannot connect;
  `:reason` distinguishes a log restart from a poison message crash-looping from
  a stream dropped for silence.

  **One reason deserves an alert of its own.** `{:no_matching_types, url}` means
  the log answered `404`: the subscription's message types match nothing that
  log knows about. It clears itself when the log deploys those types, so a
  steady rate is a subscription pointed at the wrong log.
  """
end
