# DMEx — Architecture

> **The code is the spec of record.** This document is the map: what DMEx is,
> the wire contract a non-Elixir client would implement against, the invariants
> the design rests on, and where each of them lives. The reasoning behind any
> individual mechanism lives in that module's own documentation, and this file
> points at it rather than repeating it. See [ROADMAP.md](ROADMAP.md) for what
> is designed but deliberately not in v1.

## Overview

DMEx is a decentralized message queue: a simpler, embeddable alternative to
RabbitMQ with **no single point of failure**. There is no broker process, no
cluster to operate, and no shared infrastructure between applications. Every
application that publishes messages owns and persists its own append-only
message log, and streams it directly to consumers over plain HTTP using
Server-Sent Events.

- **Technological simplicity** — the only moving parts are a SQL table and an
  HTTP endpoint.
- **No single point of failure** — no shared message bus whose outage takes
  down every producer/consumer pair. A stream's availability depends only on
  its own application and its own database.
- **Persistent, arbitrarily replayable streams** — consumption never removes
  anything. Any consumer can start, or restart, from any point in history.

**Scope for v1.** A producer streams its whole log, and every subscription
receives every message on it. There is no server-side selective delivery: no
filters and no per-subscription subsets. A consumer that cares about only some
message types declares the modules it knows and ignores the rest locally.
Server-side selection is a real requirement with a design, deliberately
deferred; see [Message-Type Selection](ROADMAP.md#message-type-selection).

### Prior art

- [MDN: Using server-sent events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events#event_stream_format)
  — the wire format DMEx must be a valid producer and consumer of.
- [oban-bg/oban](https://github.com/oban-bg/oban) — `LISTEN`/`NOTIFY` for
  low-latency wakeup backed by belt-and-suspenders polling, and a
  Postgres/SQLite-agnostic engine abstraction.
- [commanded/commanded](https://github.com/commanded/commanded) and
  [commanded/eventstore](https://github.com/commanded/eventstore) — one module
  per message type, owned by the publishing application, over an append-only
  Postgres log.

## The log

Each application owns exactly **one** append-only log: a single, totally-ordered
stream of every message it has ever published, in one plain SQL table. See
`DMEx.Migration` for the shipped DDL.

```sql
CREATE TABLE dmex_messages (
  id            bigserial   PRIMARY KEY,        -- SQLite: INTEGER PRIMARY KEY AUTOINCREMENT
  message_type  text        NOT NULL,
  data          jsonb       NOT NULL,           -- SQLite: text CHECK (json_valid(data))
  metadata      jsonb       NOT NULL DEFAULT '{}',
  inserted_at   timestamptz NOT NULL DEFAULT now()
);
```

- `id` is the offset, and the only ordering and resumption mechanism. `bigint`,
  not `int`: a log nothing deletes from outgrows 2³¹.
- `message_type` is the publishing module's fully-qualified name, which is why
  nothing records *who* published a row — `MyApp.Messages.OrderPlaced` already
  says so, structurally, on every message.
- `data` and `metadata` are JSON. `metadata` is entirely publisher-owned; DMEx
  writes nothing of its own into it, so there are no reserved keys.
- `inserted_at` is observability only and never an ordering key.
- **The primary key is the only index.** Every read is an `id > cursor` range
  scan, which it serves directly.

**A row says what a message *is*, never who might want it — and that restraint
is load-bearing.** The table is immutable, so any grouping stamped into a row at
publish time would be immutable too: fixed message by message, unable to apply
to history, and unable to follow the readers as they change. That is a *filing*
decision, made by and for whoever reads the log, welded into the *record of what
happened*, and the two have different lifetimes.

So `message_type` is the only classification a row carries, and which messages a
given reader wants is resolved at **read time**, by that reader. See
[Message-Type Selection](ROADMAP.md#message-type-selection), where a consumer's
own message list becomes a fingerprint the producer filters against.

## The wire contract

This is the part a third-party client implements against, so it is spelled out
here rather than left to the code. `DMEx.SSE.Frame` encodes and parses it;
`DMEx.SSE.Decoder` handles chunk-boundary reassembly.

**`GET /stream`**, mounted by `DMEx.Producer.Plug`. The route is singular and
carries no application segment and no query string: a producer owns exactly one
log and v1 streams all of it, and the hostname already identifies the app. (The
post-v1 filter segment, `/stream/<blob>`, is the one thing that will ever hang
off this URL, which is why the mount is a prefix `forward`.)

**Request headers** — validated and clamped by `DMEx.Producer.Params`:

| header | meaning |
| --- | --- |
| `Last-Event-ID` | resume position: an integer id, the literal `latest` (start from the tip), or absent (the whole log). Anything else is a `400`. |
| `DMEx-Batch-Window` | preferred minimum ms between frames. |
| `DMEx-Max-Batch-Size` | preferred maximum rows per frame. |

**The URL says what the subscription *is*; headers say how it is *paced*.** A
malformed pacing header is a `400`; a well-formed value outside the producer's
bounds is **clamped, not rejected**, and the `200` echoes what was actually
applied as `DMEx-Batch-Window` / `DMEx-Max-Batch-Size` response headers — so a
clamped consumer can tell "the producer is honoring me and I still can't keep
up" from "my request never took effect". Keeping pacing out of the URL means one
stable URL per subscription across every reconnect, which matters because an
adaptive consumer re-negotiates repeatedly over its lifetime.

A browser `EventSource` sets no request headers beyond `Last-Event-ID` and so
gets the defaults. That is the deliberate trade: pacing is a tuning knob whose
defaults must be sane anyway, resumption is a correctness mechanism — and
resumption is the one the spec gave a header to.

**The response** is `text/event-stream` with `Cache-Control: no-cache`,
`X-Accel-Buffering: no` (nginx buffers by default, which would swallow both
pacing and heartbeats), and `Connection: keep-alive` on HTTP/1.1.

**Frames.** Every data frame is a batch, including one carrying a single
message — one encode path, one decode path, and no branch that could let batched
and unbatched messages drift apart:

```
id: 1042
event: batch
data: {"metadata":{"max_id":1180},"messages":[{"id":1041,"message_type":"MyApp.Messages.OrderPlaced","data":{"order_id":7,"total":1999},"metadata":{},"inserted_at":"2026-07-27T10:31:04.221Z"},{"id":1042,"message_type":"MyApp.Messages.OrderShipped","data":{"order_id":7},"metadata":{"trace_id":"abc"},"inserted_at":"2026-07-27T10:31:04.884Z"}]}

```

- `event:` is DMEx's **transport-level** framing type, always `batch`. The
  application's `message_type` lives inside `data`, so a small fixed vocabulary
  DMEx controls stays separate from the open-ended set of application types.
- `id:` is the **highest** message id in the frame, since SSE allows only one.
  Per-message ids live in `data.messages`; a DMEx consumer advances per message
  from that list, while a generic client resuming via `Last-Event-ID` lands on a
  correct position.
- `data` is **one line**, so the joined payload is exactly a JSON document a
  client can hand to its parser. Per-line framing would make it NDJSON — a
  second framing layer inside the frame that every client would have to
  implement before parsing anything.
- `data.messages` holds **serialized log rows exactly as stored**. No wrapper,
  no reshaping, no fields lifted or renamed, so the wire format has no shape of
  its own to drift from the schema.
- `data.metadata` holds facts about the *frame*: `max_id`, the log's tip when
  the frame was sent. Diffing a message's `id` against it is the "am I falling
  behind" signal.

**Heartbeats** are SSE comment frames (`: hb`) on an idle interval — ignored by
every spec-compliant client. `retry:` is sent at stream open carrying the
**effective** interval, `heartbeat_ms + batch_window`, because the send loop's
throttle runs after the heartbeat branch and that sum is the real upper bound on
silence. A generic client reads it as a reconnect delay; a DMEx consumer reads
it as "expect a frame or a comment within this window" and reconnects after
three missed ones.

Because the protocol is just HTTP and SSE, no custom client library is required.
Uniform batch framing helps: a generic client sees one frame shape rather than
branching on `event:`.

## The invariants

Everything else is detail. These are the properties the design is built to have,
and each one names where it is enforced.

**1. Appends are serialized per app.** Postgres allocates sequence values at
INSERT time but makes rows visible at COMMIT time, so two concurrent publishes
can commit out of allocation order — and a reader scanning `id > cursor ORDER BY
id` would advance straight over the row that had not committed yet and lose it
**permanently and silently**. Every read path here is such a cursor, so
`DMEx.Publisher` appends under a transaction-scoped advisory lock keyed on the
app (`DMEx.Storage.Postgres`). The cost is publish concurrency; the purchase is
a reader model with no gap-detection logic anywhere in it. (SQLite is
single-writer already.)

**2. The producer's memory per connection is one frame.** The send loop in
`DMEx.Producer.Stream` holds at most the frame it is writing. There is no
accumulator anywhere, and the frame itself is capped in bytes as well as rows.

**3. Reads are gated on sends completing.** This is the key inversion. If reads
were driven by the publisher's rate, they would be decoupled from the consumer's
drain rate — and that decoupling *is* an unbounded buffer. Here the next read
cannot happen until the previous write returns, so producer-side overload is not
detected and handled, it is unrepresentable. A stalled consumer stalls its own
loop and nothing else: it reads no more rows, allocates no more memory, and
issues no more queries.

**4. The request process *is* the stream.** No per-connection `GenServer`, no
producer-side supervisor. In both Bandit and Cowboy the connection process owns
the socket, so `chunk/2` works naturally, the socket closes when the process
dies, and reclaiming a connection means killing the process that is blocked in
the write. `DMEx.Producer.Plug` runs the loop inline.

**5. Every blocking step is taken under a deadline.** A process blocked in
`chunk/2` cannot time itself out, because it is not running — nor can one
blocked in a read against a database that stopped answering. So the loop arms a
deadline in its `DMEx.Producer.Status` before each blocking step, and
`DMEx.Producer.Watcher` kills whatever is past one. This needs no transport
configuration, and it reaches the wedges a socket timeout structurally cannot
see. See [the deployment guide](guides/deployment.md) for sizing and for the
socket options that tighten it.

**6. A wakeup is a hint, never a carrier of work.** `DMEx.Producer.Dispatcher`
broadcasts a bare atom on `NOTIFY` or on a poll that saw the tip move; each
stream reads from its own cursor to whatever is there now. Missing one costs
latency (bounded by the poll interval) and never correctness, so there is no
gap-versus-overlap reconciliation anywhere in the design. The poll is what makes
dispatch robust, exactly as Oban's stager polls even with `LISTEN`/`NOTIFY`
active.

**7. Dropping a connection is not a delivery event.** The producer holds no
consumer state, the consumer's `last_seen` is durable, and resumption is the
same `id > last_seen` read a cold connect does. That is what makes it safe to be
aggressive about reclaiming, and why there is no in-band "you are being dropped"
frame — it would have to be written to the very socket that is stalled.

**8. The consumer holds the socket off rather than buffering.** Rows land in a
bounded FIFO (`DMEx.Consumer.Queue`) and the socket is re-armed only while it
has headroom, so the TCP receive window really closes. A client that reads
eagerly into its own mailbox does not remove the backpressure problem, it
relocates it. The queue's slack exists to *measure* drain rate, not to absorb
overload.

**9. TCP backpressure is one bit, so the consumer also asks for less.** It can
say stop; it cannot say send me less at a time. `DMEx.Consumer.Control` converts
sustained queue pressure into smaller `max_batch_size` requests — sized from
measured service time (`DMEx.Consumer.Meter`), not from throughput — and
recovers additively in the reverse order. Without it, a slow consumer settles
into a reconnect loop that makes the producer work harder the slower it gets.

**10. Batching is framing, never processing.** Every de-batched row goes through
`DMEx.Consumer.Pipeline` one at a time: its own transaction, its own handler
call, its own `last_seen` advance, its own failure handling. A batch is never
processed or committed as a unit, so a mid-batch failure rolls back exactly one
message.

**11. The handler sees its message and the publisher's metadata, nothing else.**
No id, no `message_type`, no frame. A handler is a plain function of its domain
data plus whatever context the publisher attached, testable by constructing a
struct and a map.

**12. A subscription's identity is its declaration, not its module name.** The
`"producer/id"` pair in `use DMEx.Consumer.Handler` keys the cursor, so renaming
a module is free and only changing the declaration costs a migration
(`DMEx.Consumer.Cursors`). The message list is deliberately *not* part of it:
adding a clause for a new type handles it from the tip onward rather than
silently replaying the entire log.

## The process map

Everything is per-connection; nothing is pooled or shared except the small set
each producer supervises.

**Producer.** `MyApp.Producer` (via `DMEx.Producer`) supervises:

| module | role |
| --- | --- |
| `DMEx.Producer.Registry` | one entry per open stream, holding that connection's `DMEx.Producer.Status` — its wakeup flag and deadline. The entry dies with the request process, so nothing has to clean up. |
| `DMEx.Storage` children | whatever the adapter needs; Postgres puts its `LISTEN` connection here. |
| `DMEx.Producer.Dispatcher` | holds `LISTEN`, runs the poll timer, broadcasts wakeups. |
| `DMEx.Producer.Watcher` | sweeps deadlines and reclaims connections that stopped making progress. |

None of them ever holds message data on a connection's behalf. The connections
themselves are web-server request processes running `DMEx.Producer.Stream`.

**Consumer.** `DMEx.Consumer` supervises one `DMEx.Consumer.Stream` per
subscription under a `DMEx.Consumer.Supervisor`, each owning one outbound
connection, its queue, its adaptive controller and its cursor. Restart intensity
is set never to trip: a subscription that crash-loops — on a poison message
under `on_failure: :crash`, or on a repo that is down — does so on its own, at a
pace set by jittered backoff, and its siblings never notice.

**Storage.** One `DMEx.Storage` behaviour covers both halves — the producer's
log and the consumer's own `dmex_cursors` and `dmex_dead_messages` — rather than
inventing a second abstraction for bookkeeping. `DMEx.Storage.Postgres` is the
shipped adapter; SQLite is a new adapter rather than a new seam.

## Delivery semantics

**At-least-once, with duplicate suppression by id.** A row whose `id` is not
greater than the cursor is dropped before the handler runs, so transport-level
re-delivery — a reconnect overlap, a generic client resuming — never invokes a
handler twice.

**This is not exactly-once, and the gap is worth being precise about.** The
handler call and the `last_seen` advance share one local transaction, so DMEx's
bookkeeping cannot drift from the callback. But that commit is also the *only*
record that a message was handled, so a crash after the handler's side effects
land and before the transaction commits redelivers a message that is
indistinguishable from one never processed.

What follows for handler authors: **side effects through the consumer's own repo
are exactly-once for free**, because they roll back with the transaction that
would have recorded them. Effects that leave it — an HTTP call, a write to
another database — must be idempotent on something in the message or its
metadata, since the log id is deliberately not exposed. A local write plus a
separate outbox is the pattern that stays correct.

**Ordering** holds within one app's stream and never across streams.
**Retention** is unbounded: nothing is deleted as a side effect of consumption.

## Decentralization / no-SPOF

To be precise about the claim: DMEx introduces no shared broker, cluster or
coordination service whose failure would affect multiple applications at once.
It is **not** a claim of automatic high availability for any individual stream —
if a producer's database is down, that producer's stream is unavailable exactly
as any other data that application owns would be. HA for one stream is the same
problem as HA for that application's database, and out of scope here.

## What v1 deliberately does not do

- **No server-side selection** — every subscription receives the whole log and
  filters locally. See
  [Message-Type Selection](ROADMAP.md#message-type-selection).
- **No aggregate budget or admission control** — no cap on open connections, no
  `503`/`Retry-After`. Per-connection protection is complete (invariants 2, 3
  and 5); aggregate pressure from many individually-healthy connections is not
  addressed, and neither is a consumer that is pathologically slow while still
  technically progressing. See
  [Global Resource Limits](ROADMAP.md#global-resource-limits).
- **No compression** — designed, not built. See
  [Compression](ROADMAP.md#compression).
- **No SQLite adapter** — the behaviour is complete; the adapter is not written.

## What has been measured

`bench/` exists to answer design claims rather than to catch regressions. What
it has established so far:

- **The append lock is nearly free uncontended and expensive under contention** —
  ~1.13x at concurrency 1–2, ~4.8x at 8, ~5.8x at 32, with locked throughput
  *falling* as concurrency rises. Publish concurrency buys an application
  nothing; batch work into fewer, larger messages instead.
- **Per-connection pacing holds exactly as designed** — 5.2 frames/s against a
  200 ms window regardless of publish rate, and an idle log costs one query per
  poll interval per *app* (1/s flat at both 10 and 200 connections), which is
  the thundering-herd guard working.
- **No pool queueing appeared at 200 concurrent streams** on a developer
  machine, so the read-amplification ceiling is somewhere above that.
- **The consumer is commit-bound, and it is the bottleneck.** A client that
  reads and discards sustains ~2 400 rows/s; a real consumer with a no-op
  handler manages ~155 rows/s, and one skipping *every* row only ~250 rows/s.
  Both are paying for one transaction commit per row. At ~155 rows/s a
  million-row log takes roughly two hours to replay, which is the figure the
  "arbitrarily replayable" claim should be quoted with.

## Open questions

- **Auth / access control** — who may read a producer's endpoint, and how is
  that enforced? Nothing in v1 answers this.
- **Sharing message modules across independent applications.** `DMEx.Message`
  assumes the consumer has the *same* Elixir module the producer published
  from, so it derives the same `message_type` and decodes into the same struct.
  That is a given inside one umbrella and an open problem between separate
  teams, repos and languages. The options — a shared package, a self-describing
  schema endpoint, a loose map-plus-type-string decode, an external registry —
  are not mutually exclusive, and
  [`GET /stream/stats`](ROADMAP.md#stream-stats-get-streamstats) is the small
  first step of the second. One constraint to carry into the decision: dropping
  `origin` rests on `message_type` being app-prefixed, so any option that leads
  to shared, non-app-prefixed module names (`MyOrg.Messages.OrderPlaced`
  published by several apps) reopens the question of how a consumer knows which
  application a message came from.
- **Cross-node clustering** — if a producer application itself runs as a
  multi-node OTP cluster, how are the log's single-writer property and the
  `LISTEN`/`NOTIFY` dispatcher kept consistent across nodes?
- **Payload byte fidelity** — `jsonb` normalizes key order, drops duplicate
  keys and loses whitespace, and the wire envelope is re-serialized from
  storage, so the bytes a consumer receives are not the bytes the publisher
  encoded. This is visible on the wire, not theoretical, and there is an
  integration test pinning it. Open: document it as a hard caveat (no
  end-to-end signing of raw payload bytes), or add an exact-bytes column?
- **Retention / compaction** — logs grow forever. What operator-facing controls
  exist, and how do they interact with "arbitrarily replayable"?
  `dmex_dead_messages` grows the same way and is the easier half: its
  `inserted_at` is when a row landed there, so "abandoned more than N days ago"
  is expressible. Whether DMEx should ship the pruning or leave it to a `DELETE`
  is open.
- **Whether the adaptive constants are right under real load.** The shape is
  settled and the numbers are in `DMEx.Config`, but they have only been
  exercised against a deliberately slow test handler.
- **Coalescing consecutive skips into one cursor advance.** The measurement
  above says a mostly-skipped stream is commit-bound on rows nothing was done
  with. It is safe on its face — no handler ran — but it makes `last_seen`
  advance in variable-size jumps, and it is the first place a batch would mean
  something to processing rather than only to framing. Note that
  [Message-Type Selection](ROADMAP.md#message-type-selection) removes the
  motivation entirely, so the real question is which lands first.
- **Where the read-amplification ceiling actually is**, and whether the pool
  wants partitioning so stream reads cannot starve publish writes.
- **Adaptation-induced reconnect churn** — every re-parameterization is a
  reconnect, and a correlated event (a producer deploy slowing every consumer at
  once) is damped only by jitter. Should adaptation be rate-limited over a
  longer window?
- **A byte bound on the consumer queue** — `queue_limit` is a row count, which
  says nothing about memory when payloads vary by three orders of magnitude. A
  second threshold the controller must reason about, or is documenting the
  product enough?
- **Third-party client backpressure** — invariant 8 is a property of *DMEx's*
  client. Any SSE-capable client can consume a stream, and one that bounds
  neither its socket nor its queue is indistinguishable to the producer from a
  genuinely fast one. Documented compatibility caveat, or something detectable?
- **When to build server-side selection**, and what triggers reopening
  [aggregate limits](ROADMAP.md#global-resource-limits) — a connection-count
  threshold, an observed incident, or memory pressure the per-connection bounds
  demonstrably failed to contain? Worth deciding the tripwire before
  rediscovering the need in production.
- **`:skip` visibility** — `:skip` drops a message with no DMEx-side record
  beyond telemetry, unlike `:dead_message`. Intentional as a "bring your own
  observability" mode, or should it always leave a trace?
