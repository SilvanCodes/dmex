defmodule DMEx.Telemetry do
  @moduledoc """
  Every `:telemetry` event DMEx emits.

  No functions — the vocabulary is written down here so that renaming an event,
  which breaks every dashboard built on it, is visible in a diff.

  ## Log

  | event | measurements | metadata |
  | --- | --- | --- |
  | `[:dmex, :append, :start]` / `:stop` / `:exception` | a `:telemetry.span/3` | `:log`, `:type` |
  | `[:dmex, :stream, :connect]` | `:cursor`, `:matched` | `:log`, `:selective` |
  | `[:dmex, :stream, :write]` | `:rows`, `:bytes` where bytes are meaningful | `:log` |
  | `[:dmex, :stream, :heartbeat]` | — | `:log` |
  | `[:dmex, :stream, :reclaim]` | `:overdue_by` in ms | `:log` |
  | `[:dmex, :dispatcher, :broadcast]` | `:streams`, `:woken`, `:types` | `:log` |

  `:matched` counts the types a connection admits, as `DMEx.Log.Selection.size/1`
  counts them, and is `nil` for a whole-log connect. `:reclaim` is
  `DMEx.Log.Watcher` killing a connection that blew its deadline — [the
  deployment guide](deployment.md) sizes `stall_timeout` against its rate.

  ## Handler

  Every handler event carries `:subscription`, `:handler` and `:origin`.
  `:subscription` — `"orders/order_projection"` — is the durable identity
  (`DMEx.Handler`), so it is what a dashboard groups and alerts on; `:handler` is
  the module, which is what a human reads in a log line. After one refactor they
  usually differ. `:origin` is where the rows came from: a URL, or the module of
  a log this application owns.

  | event | measurements | extra metadata |
  | --- | --- | --- |
  | `[:dmex, :handler, :message, :start]` / `:stop` / `:exception` | a `:telemetry.span/3` | `:type`, `:id`, `:attempt`, plus `:reason` on `:exception` |
  | `[:dmex, :handler, :skip]` | `:id` | `:type` |
  | `[:dmex, :handler, :checkpoint]` | `:cursor` | — |
  | `[:dmex, :handler, :failed]` | `:id`, `:attempts` | `:type`, `:on_failure`, `:reason` |
  | `[:dmex, :handler, :reconnect]` | `:attempt`, `:delay` | `:reason` |

  One `:message` span per **attempt** at one message — the handler call and the
  cursor advance, in their transaction — so an `:exception` at `attempt: 1`
  followed by a `:stop` at `attempt: 2` is a transient failure recovering. On
  `:exception`, the handler's own raise arrives wrapped in
  `DMEx.Handler.Pipeline.Failure` (`Exception.message/1` reads through to the
  original, `.exception` is it) and anything else is storage failing, which is
  not retried and not resolved by `on_failure` — so the wrapper is what
  separates a message a handler rejected from a database that went away.

  `:skip` is a row no clause matched, outside any `:message` span. Its ratio to
  `:checkpoint`, which persists a coalesced run of skipped rows, is the
  coalescing ratio: one checkpoint per thousand skips is the mechanism working,
  one per skip means runs are being broken up by handled rows.

  `:failed` is a message that exhausted its local retries, emitted for
  `:dead_message` and `:skip` only — `:crash` re-raises, so the supervisor's
  report is the record there.

  `:reconnect`'s `{:no_matching_types, url}` reason deserves an alert of its own:
  it is the log's `404` (`DMEx.Log.Plug`), which clears itself when the log
  deploys those types, so a steady rate is a subscription pointed at the wrong
  log.

  There is deliberately **no socket-gating event and no lag event**:
  backpressure is not a decision this side makes but what happens while the
  process is in a handler (ARCHITECTURE's "the consumer has no buffer"), and
  "how far behind am I" is not answerable from a frame (ARCHITECTURE's *wire
  contract* — no `max_id`, no clock).
  """
end
