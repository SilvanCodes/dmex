# DMEx — Architecture

> **The code is the spec of record.** This document is the map: what DMEx is,
> the wire contract a non-Elixir client would implement against, the invariants
> the design rests on, and where each of them lives. The reasoning behind any
> individual mechanism lives in that module's own documentation, and this file
> points at it rather than repeating it. See [ROADMAP.md](ROADMAP.md) for what
> is designed but deliberately not built yet.

## Overview

DMEx is a decentralized message queue: a simpler, embeddable alternative to
RabbitMQ with **no single point of failure**. There is no broker process, no
cluster to operate, and no shared infrastructure between applications. Every
application that appends messages owns and persists its own append-only
message log, and streams it directly to consumers over plain HTTP using
Server-Sent Events.

- **Technological simplicity** — the only moving parts are a SQL table and an
  HTTP endpoint.
- **No single point of failure** — no shared message bus whose outage takes
  down every producer/consumer pair. A stream's availability depends only on
  its own application and its own database.
- **Persistent, arbitrarily replayable streams** — consumption never removes
  anything. Any consumer can start, or restart, from any point in history.

**Scope.** A producer streams its log, and a subscription receives the message
types its handler can decode — resolved server-side, so unselected rows are
never read, framed or sent. That selection introduces **no grouping vocabulary
of its own**: nothing is stamped on the log, nothing is named beside it, and a
consumer declares nothing it had not already declared. The modules its
`handle_message/2` clauses match on are the selection, encoded at compile time
into the request path. See [Message-type selection](#message-type-selection).

A bare `GET /stream` still streams the whole log, which is what a browser
`EventSource`, `curl`, or anything written before selection existed asks for.

### Prior art

- [MDN: Using server-sent events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events#event_stream_format)
  — the wire format DMEx must be a valid producer and consumer of.
- [oban-bg/oban](https://github.com/oban-bg/oban) — `LISTEN`/`NOTIFY` for
  low-latency wakeup backed by belt-and-suspenders polling, and a
  Postgres/SQLite-agnostic engine abstraction.
- [commanded/commanded](https://github.com/commanded/commanded) and
  [commanded/eventstore](https://github.com/commanded/eventstore) — one module
  per message type, owned by the appending application, over an append-only
  Postgres log.

## The log

Each application owns exactly **one** append-only log: a single, totally-ordered
stream of every message it has ever appended, in one plain SQL table. See
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
- `message_type` is the appending module's fully-qualified name, which is why
  nothing records *who* appended a row — `MyApp.Messages.OrderPlaced` already
  says so, structurally, on every message.
- `data` and `metadata` are JSON. `metadata` is entirely publisher-owned; DMEx
  writes nothing of its own into it, so there are no reserved keys.
- `inserted_at` is observability only and never an ordering key.
- **Two indexes, one per read shape.** The primary key serves a whole-log read —
  an `id > cursor` range scan — directly. `(message_type, id)` serves a
  selective one, and the planner takes it when the selection is rare enough to
  be worth it (measured: at 0.1% of a log yes, at 1% and above it prefers the
  primary key, which is the cheaper plan there). Both branches are correct and
  nothing forces the choice.

**A row says what a message *is*, never who might want it — and that restraint
is load-bearing.** The table is immutable, so any grouping stamped into a row at
append time would be immutable too: fixed message by message, unable to apply
to history, and unable to follow the readers as they change. That is a *filing*
decision, made by and for whoever reads the log, welded into the *record of what
happened*, and the two have different lifetimes.

So `message_type` is the only classification a row carries, and which messages a
given reader wants is resolved at **read time**, by that reader — see
[Message-type selection](#message-type-selection), where a consumer's own
message set becomes a fingerprint the producer resolves against its own types.

## Message-type selection

A subscription receives the message types its handler can decode, and nothing
new is declared anywhere to make that happen.

**The selection is the handler's clauses.** `DMEx.Handler` already
derives a subscription's message set at compile time from the structs
`handle_message/2` matches on — it has to, or a consumer could not turn a
`message_type` back into a struct at all. That set gains a second job. A name
standing for a set of types would be a second vocabulary somebody has to define,
append, version, discover and keep in sync, and every one of those costs is
paid to describe something the consumer has already said. Worse, the two can
disagree: naming a type the handler cannot decode, or omitting one it can, both
silently. Derived, that state is unrepresentable.

**It travels as an XOR filter in the path.** Module names run 25–40 bytes each,
so spelling them out exhausts a conservative request line at around sixty types.
A 16-bit XOR filter encodes the same set at ~2.5 bytes per type, which puts the
ceiling near 2 400. The blob is built at compile time and is a pure function of
the message set, so it is a constant on `__dmex__/0` and the same handler
compiles to the same URL everywhere. `guides/fingerprint-spec.md` is the
normative encoding; `DMEx.Fingerprint` implements it.

The path rather than a header, because a browser `EventSource` sets no request
headers but owns its URL entirely — that is what gives a bare `EventSource` real
server-side selection with no client library. The path rather than `?filter=`,
because caches and proxies routinely drop or key-strip query strings and nothing
does that to a path; losing the selection silently converts a selective
subscription into a whole-log one, which is a bandwidth incident rather than an
error.

**Errors point one way, and that is the whole reason it works.** There are no
false negatives, so every selected type is delivered. A false positive admits
one extra type, which the consumer's ordinary skip path drops — so the skip path
is a defence rather than dead code. Measured over 45 660 membership tests: zero
false negatives, and a false-positive rate of 14.65 ± 1.85 per million against
2⁻¹⁶'s expected 15.26.

**The producer resolves it against its own compiled modules**, not against what
its log contains. `SELECT DISTINCT message_type` goes stale silently — a
producer can know a type it has not yet emitted, and first emission involves no
restart and no reconnect — while the module set changes only on deploy, and a
deploy takes every SSE connection with it. `DMEx.Log.Messages` scans the
producer's OTP application and every loaded application depending on `:dmex`,
which is exactly the set whose modules could contain a `use DMEx.Message`. The
result only has to be a *superset* of what the producer appends: an extra type
never matches, a missing one is silent loss.

**Not a partitioning.** There is still one totally-ordered log and one id
sequence. A selective subscription sees a sparse subset of that same id space,
never a space of its own, so every consumer's `last_seen` is directly comparable
with every other's regardless of what it selected. The
visible consequence is that a selective cursor lags the log by however much of
it went to somebody else, which costs nothing to resume from: `(message_type,
id)` makes the next read an index seek, the same price whether the next matching
row is one id away or ten million.

**Appending is untouched.** `append/3` keeps its signature and writes the same
columns; message modules still take no options. A publisher does not know, and
does not need to know, who is selecting its types.

**The escape hatch is on the handler, not the call site.** `selective: false` in
`use DMEx.Handler` requests the whole log and filters locally, the way
delivery worked before this existed. It is there to tell two failures apart when
a subscription receives nothing — a producer or encoding mismatch, which
whole-log delivery fixes immediately, from a producer that has appended nothing,
which it does not. It lives on the module for the same reason the producer, the
id and the message set do: the module is the complete declaration of what a
subscription *is*, and nothing at a `subscribe/2` call site should be able to
vary that.

**A filter matching nothing is a `404`.** An empty expansion is
indistinguishable on the wire from a healthy stream on a quiet log, so the
alternative is a connection that opens and says nothing forever. It is not proof
of a client bug — a consumer deployed ahead of the producer that will emit its
types is legitimately in that state — so DMEx's consumer retries it rather than
stopping the way it stops on a `400`.

## The wire contract

This is the part a third-party client implements against, so it is spelled out
here rather than left to the code. `DMEx.Transport.SSE.Frame` encodes and parses it;
`DMEx.Transport.SSE.Decoder` handles chunk-boundary reassembly.

**`GET /stream`** and **`GET /stream/<blob>`**, both mounted by
`DMEx.Log.Plug` through one prefix `forward`. The bare form is the whole
log; the blob narrows it to the message types a consumer's filter admits (see
[Message-type selection](#message-type-selection) and
`guides/fingerprint-spec.md`). There is still no application segment and no
query string: a producer owns exactly one log, and the hostname already
identifies the app.

`/stream/...` is a namespace with a closed reserved list matched *before* any
blob decode. `stats` is reserved and not yet implemented, answered `404`. A
segment that is neither reserved nor decodable is a `400`.

**Request headers** — validated by `DMEx.Log.Params`:

| header | meaning |
| --- | --- |
| `Last-Event-ID` | resume position: `previous/current` as echoed from a frame's `id:`, a bare integer id, the literal `latest` (start from the tip), or absent (the whole log). Anything else is a `400`. Only `current` is used — the predecessor is a consumer-side check and means nothing to a producer. |

**That is the whole list. DMEx defines no request headers of its own.** Pacing
is producer configuration — `max_batch_frequency`, `max_batch_size` and
`max_batch_bytes` on the producer module — so there is nothing for a consumer to ask for, nothing to clamp, and
nothing to echo back. The direct consequence is that **DMEx's own consumer and a
browser's `EventSource` send byte-identical requests** for the same
subscription, and everything that distinguishes one subscription from another is
in the request line.

That also settles where the selection goes. A selection is *identity* — a
different selection is a different resource — so it belongs in the path, and
three things follow:

- **A browser `EventSource` can select.** It sets no request headers beyond
  `Last-Event-ID`, but the URL is entirely its own.
- **Intermediaries treat paths as load-bearing and query strings as
  negotiable.** Caches and proxies routinely drop, reorder or key-strip query
  strings; nothing does that to a path. Losing a selection silently converts a
  selective subscription into a whole-log one — a bandwidth incident rather than
  an error.
- **A selection is stable, so the URL is stable.** A message set changes when
  somebody ships code, so one URL per deployment is a grouping that survives
  long enough to be worth having in a proxy log, a cache key or a dashboard.

On a selective stream the `200` additionally carries
**`DMEx-Matched-Messages`**, the count of the producer's own types the filter
admitted. It is the only DMEx header anywhere in the contract, and it is
diagnostic rather than functional: the blob is opaque and cannot be reversed, so
this is the one thing that makes a subscription legible from outside — a count
of `0` where ten types were selected is an encoding mismatch, visible at connect
rather than through silence.

**The response** is `text/event-stream` with `Cache-Control: no-cache`,
`X-Accel-Buffering: no` (nginx buffers by default, which would swallow both
pacing and heartbeats), and `Connection: keep-alive` on HTTP/1.1.

**Frames.** One frame per log row. A `dmex_messages` row has five columns and an
SSE frame has five slots, so the mapping is direct and there is no envelope:

```
id: 1041/1042
event: MyApp.Messages.OrderShipped
data: {"data":{"order_id":7},"metadata":{"trace_id":"abc"},"inserted_at":"2026-07-27T10:31:04.884Z"}

```

| column | goes to | why there |
| --- | --- | --- |
| `id` | `id:`, as `previous/current` | it *is* the resume position, so a generic client's `Last-Event-ID` works with no DMEx knowledge — and the predecessor makes a *missing* frame detectable |
| `message_type` | `event:` | it *is* the event name, so `addEventListener("MyApp.Messages.OrderShipped", …)` works in a browser |
| `data` | inside `data:` | the publisher's payload, verbatim |
| `metadata` | inside `data:` | the publisher's metadata, verbatim |
| `inserted_at` | inside `data:` | append time, which is what the consumer's lag signal is measured against |

- **There is no batch envelope and no `max_id`.** Batching is still real — the
  producer encodes a whole read's rows and hands them to `chunk/2` in one call —
  but it is a *write* batch rather than a wire construct, so nothing on the
  consumer side has to know it happened. That is what lets `id:` be the row's own
  id rather than "the highest in this frame", and what makes a frame mean the
  same thing whether one row or five hundred went out together.
- **`data:` is one line**, so the payload is exactly a JSON document a client can
  hand to its parser. JSON's grammar forbids raw control characters inside
  strings, which is what makes it safe to splice stored `jsonb` text straight in
  with no escaping pass.
- **`message_type` is constrained by the schema**, because it goes on an
  `event:` line and SSE field lines have no escaping. `dmex_messages` carries a
  CHECK constraint restricting it to `[A-Za-z0-9_.]+` — exactly an Elixir module
  name's charset — so a type that could split a frame cannot be appended.
- **How far behind is `now - inserted_at`**, in milliseconds, rather than an id
  difference against a tip the wire no longer carries. It compares two machines'
  clocks, so it is an NTP-accurate seconds-scale signal and not a finer one.
- **`id:` is a link, not a number.** `1041/1042` reads "the row before me was
  1041, I am 1042", and a consumer refuses a frame that does not claim the row
  it last saw. It cannot be inferred as `id - 1`, because a selective stream's
  ids are legitimately sparse and only the producer knows which gaps it made on
  purpose. The first frame of a connection links back to the cursor the read was
  issued against, so the chain also proves the producer resumed where it was
  asked to. See [invariant 13](#the-invariants).

The cost of putting the type on `event:` is that a browser's
`EventSource.onmessage` stops firing — named events need `addEventListener` —
which is the ordinary way to write an `EventSource` against a typed stream.
`curl -N` is unaffected, and reads better than it did: the type is right there
on its own line.

**Heartbeats** are SSE comment frames (`: hb`) on an idle interval — ignored by
every spec-compliant client. `retry:` is sent at stream open carrying the
**effective** interval, `heartbeat_ms` plus one batch interval, because the send loop's
throttle runs after the heartbeat branch and that sum is the real upper bound on
silence. Both halves are producer configuration, so every connection to one
producer is told the same number. A generic client reads it as a reconnect
delay; a DMEx consumer reads it as "expect a frame or a comment within this
window" and reconnects after three missed ones.

Because the protocol is just HTTP and SSE, no custom client library is required.

## The invariants

Everything else is detail. These are the properties the design is built to have,
and each one names where it is enforced.

**1. Appends are serialized per app.** Postgres allocates sequence values at
INSERT time but makes rows visible at COMMIT time, so two concurrent appends
can commit out of allocation order — and a reader scanning `id > cursor ORDER BY
id` would advance straight over the row that had not committed yet and lose it
**permanently and silently**. Every read path here is such a cursor, so
`DMEx.Log` appends under a transaction-scoped advisory lock keyed on the
app (`DMEx.Storage.Postgres`). The cost is append concurrency; the purchase is
a reader model with no gap-detection logic anywhere in it. (SQLite is
single-writer already.)

**2. The producer's memory per connection is one batch, bounded in rows *and*
bytes.** The send loop in `DMEx.Log.Stream` holds at most the rows it is
writing, and there is no accumulator anywhere. Two bounds rather than one,
because a row costs heap in two independent ways: its payload, which
`max_batch_bytes` counts, and a fixed cost of roughly a kilobyte for the map,
the `%DateTime{}`, the frame's iodata cells and the timestamp binary — which no
budget expressed in database octets can see. At a 1 KB payload the byte bound
binds and the fixed cost is around a megabyte; at a 50-byte payload the same
1 MiB budget admits about twenty-one thousand rows whose fixed cost alone is
~22 MB, and `max_batch_size` is what closes that end.

**Both bounds are applied inside the query** (`c:DMEx.Storage.read/5`), which is
the whole point: a read that returns `max_batch_size` rows and lets the caller
trim them has already spent the memory the bound exists to cap. The Postgres
adapter carries a running sum over the *limited* batch and keeps rows while the
inclusive total stays under budget, with one deliberate exception — a single row
larger than the whole budget is still returned alone, because refusing it would
wedge the subscription on it forever.

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
the write. `DMEx.Log.Plug` runs the loop inline.

**5. Every blocking step is taken under a deadline.** A process blocked in
`chunk/2` cannot time itself out, because it is not running — nor can one
blocked in a read against a database that stopped answering. So the loop arms a
deadline in its `DMEx.Log.Status` before each blocking step, and
`DMEx.Log.Watcher` kills whatever is past one. This needs no transport
configuration, and it reaches the wedges a socket timeout structurally cannot
see. See [the deployment guide](guides/deployment.md) for sizing and for the
socket options that tighten it.

**6. A wakeup is a hint, never a carrier of work.** `DMEx.Log.Dispatcher`
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

**8. The consumer has no buffer, and that is what makes the backpressure
exact.** `DMEx.Handler.Stream` runs each row through the pipeline **inline, on
the socket path**, as it is decoded. There is no queue, no drain loop and no
explicit socket gate: `Mint.HTTP.stream/2` re-arms `active: :once` itself, so
while the process is inside a handler nothing is re-armed, the TCP receive
window closes, and the producer's `chunk/2` blocks on it. The most that can be
in flight is one decoded chunk in hand plus one socket message already in the
mailbox.

A buffer would only have relocated the problem: a few hundred rows of slack
absorb nothing at a sustained mismatch, and every row of slack is one more row
the socket keeps accepting before it says stop. Nothing accumulates, so there is
nothing to bound and no depth to observe — which is also why there is no
consumer-side controller. The window closing *is* the signal, and the producer's
loop is already gated on it (invariant 3).

**9. Pacing has exactly one owner, and it is stated as a rate.**
`max_batch_frequency`, `max_batch_size` and `max_batch_bytes` are the producer's
configuration and appear nowhere else — not in a request, not in a response, not
in a subscription's options. What that buys is the absence of a whole class of
question: there is no "requested" value to drift from an "effective" one, no
clamping, no echo to interpret, and no state machine re-negotiating anything
mid-subscription.

`max_batch_frequency` is **how hard one connection may hammer the database**,
and it is a ceiling rather than a reaction to load: a hundred connections at the
default five reads per second is five hundred short range scans a second against
the pool, whatever the append rate is doing. That number is knowable in advance
precisely because it does not depend on how much anyone appends. The producer
is the party whose database, memory and request processes all three settings
protect, so it is the party that chooses them. See
[the deployment guide](guides/deployment.md) for the one sizing rule that ties a
producer's `max_batch_size` to consumers it does not control.

**10. Batching is a write batch, never a processing unit.** The producer encodes
a read's rows as one frame each and writes them together; the consumer sees a
plain run of one-row frames and runs each through `DMEx.Handler.Pipeline`
separately — its own transaction, its own handler call, its own `last_seen`
advance, its own failure handling. A mid-batch failure rolls back exactly one
message, and nothing on the wire records that a batch happened.

**11. The handler sees its message and the publisher's metadata, nothing else.**
No id, no `message_type`, no frame. A handler is a plain function of its domain
data plus whatever context the publisher attached, testable by constructing a
struct and a map.

**12. A gap in delivery is detected, never stepped over.** Every frame states
its predecessor and `DMEx.Transport.SSE` refuses one that does not claim the
row it last saw, before duplicate suppression gets a chance to hide it. Without
this the cursor model has a hole it cannot see: the cursor is the only record of
what was processed, so a message lost between two that arrived would have its id
put permanently behind the cursor by the next row's advance — never re-read,
never recorded, and indistinguishable from never having been appended.

TCP does not silently drop bytes mid-stream, so what this actually defends
against is a mangling intermediary, a third-party producer whose read skips
rows, and DMEx's own decoder, which discards any frame block missing `id:`,
`event:` or `data:`. A break drops the connection and re-reads from the durable
cursor: transport damage repairs itself, and a producer that really does skip
rows becomes a visible reconnect loop making no progress, which is the correct
failure where silently advancing was not.

**13. A subscription's identity is its declaration, not its module name.** The
`"log/id"` pair in `use DMEx.Handler` keys the cursor, so renaming
a module is free and only changing the declaration costs a migration
(`DMEx.Handler.Cursors`). The message set is deliberately *not* part of it:
adding a clause for a new type handles it from the tip onward rather than
silently replaying the entire log.

## The process map

Everything is per-connection; nothing is pooled or shared except the small set
each log supervises.

**The log.** `MyApp.Log` (via `DMEx.Log`) supervises:

| module | role |
| --- | --- |
| `DMEx.Log.Registry` | one entry per open stream, holding that connection's `DMEx.Log.Status` — its wakeup flag and deadline. The entry dies with the request process, so nothing has to clean up. |
| `DMEx.Storage` children | whatever the adapter needs; Postgres puts its `LISTEN` connection here. |
| `DMEx.Log.Dispatcher` | holds `LISTEN`, runs the poll timer, broadcasts wakeups. |
| `DMEx.Log.Watcher` | sweeps deadlines and reclaims connections that stopped making progress. |

None of them ever holds message data on a connection's behalf. The connections
themselves are processes running `DMEx.Log.Stream`: a web-server request process
when the reader is remote, and the reader process `DMEx.Transport.Local` starts
when it is in this VM. Both register in the same registry, so wakeups, pacing
and stall reclaim treat them identically.

**Subscriptions.** `DMEx.Supervisor` supervises one `DMEx.Handler.Stream` per
subscription under a `DMEx.Handler.Supervisor`, each owning one cursor and one
`DMEx.Transport`, and running `DMEx.Handler.Pipeline` inline on the rows that
transport yields. The transport is inferred from the log's binding — a module is
read in this VM, a URL over HTTP — and nothing configures one. There is nothing else in a subscription — no queue process, no
controller, no drain loop. Restart intensity is set never to trip: a subscription that crash-loops — on a poison message
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

**No-loss is checked, not assumed.** At-least-once is only meaningful if
"at least once" is verifiable, and the cursor cannot verify it: a message lost
between two that arrived leaves no trace, because the next row's advance puts
its id behind the cursor forever. So each frame states its predecessor and the
consumer refuses a stream that skips one (invariant 12). That converts the one
silent, permanent loss in the design into a loud reconnect loop.

**Ordering** holds within one app's stream and never across streams.
**Retention** is unbounded: nothing is deleted as a side effect of consumption.

## Decentralization / no-SPOF

To be precise about the claim: DMEx introduces no shared broker, cluster or
coordination service whose failure would affect multiple applications at once.
It is **not** a claim of automatic high availability for any individual stream —
if a producer's database is down, that producer's stream is unavailable exactly
as any other data that application owns would be. HA for one stream is the same
problem as HA for that application's database, and out of scope here.

## What DMEx deliberately does not do

- **No narrowing a selection below the handler's message set** — a consumer
  that can decode twenty types but only wants five receives fifteen it
  discards, or splits into two handlers and therefore two cursors. "Your module
  list is your subscription" is the simpler rule; the override is an open
  question, not a missing feature.
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
  *falling* as concurrency rises. Append concurrency buys an application
  nothing; batch work into fewer, larger messages instead.
- **Per-connection pacing holds exactly as designed** — 5.2 writes/s against a
  200 ms window regardless of append rate, and an idle log costs one query per
  poll interval per *app* (1/s flat at both 10 and 200 connections), which is
  the thundering-herd guard working.
- **No pool queueing appeared at 200 concurrent streams** on a developer
  machine, so the read-amplification ceiling is somewhere above that.
- **The consumer is commit-bound on the rows it *handles*, and no longer on the
  rows it skips.** A client that reads and discards sustains ~2 500 rows/s. A
  real consumer with a no-op handler manages ~265 rows/s, and that is the
  transaction it shares with the handler — the guarantee, not overhead. A
  consumer skipping *every* row used to manage ~260 rows/s, paying the same
  commit for rows nothing was done with; coalescing those advances took it to
  ~2 515 rows/s, which is (a) — the skip path now has no measurable cost beyond
  the wire. 20 000 skipped rows cost 7 cursor writes rather than 20 000.

  At ~265 rows/s a million-row log of entirely handled messages takes roughly an
  hour to replay, and that is the figure the "arbitrarily replayable" claim
  should be quoted with. A mostly-skipped log replays at wire speed.
- **The selection filter behaves as the design assumes it does.**
  `mix bench.fingerprint` sweeps 86 set sizes over 1–2000:
  **zero false negatives** across 45 660 membership tests, a false-positive rate
  of **14.65 ± 1.85 per million** against 2⁻¹⁶'s expected 15.26, and
  **2.49 bytes per type** raw (3.33 after base64url) against the 2.46 the sizing
  argument claims. `recovered / encoded` is a flat 1.0 at 84 of 86 points
  against a producer knowing 10× the types selected, worst case 1.00069.

  That flatness is the evidence for 16-bit fingerprints rather than 8. Since
  membership is tested per *type* and not per row, a false positive admits one
  entire message type for the life of a connection — at 8 bits the same universe
  would admit a spurious type at roughly every other point, and one unlucky draw
  is a connection receiving all of the highest-volume type in the system.

  It also puts a number on the request-line ceiling the URL placement depends
  on: **~2 400 message types** at an 8 KB limit.
- **The `(message_type, id)` index is taken exactly when it pays.** `EXPLAIN` on
  a 500k-row skewed log (one type at 98.9%, one at 1%, one at 0.1%): selecting
  the 0.1% type uses the index; selecting the 1% type or anything larger uses
  the primary key and filters, at fewer buffers than the index plan would have
  cost. A hand-written per-type `LATERAL` seek — the usual loose-index-scan
  workaround — was measured against it and is *worse* everywhere except the
  most extreme selectivity, because each branch fetches a whole `LIMIT` per type
  and discards the surplus. So the plain `message_type = ANY($1)` is the right
  query and the planner is left to choose.

## Open questions

- **Auth / access control** — who may read a producer's endpoint, and how is
  that enforced? Nothing here answers this.
- **Sharing message modules across independent applications.** `DMEx.Message`
  assumes the consumer has the *same* Elixir module the producer appended
  from, so it derives the same `message_type` and decodes into the same struct.
  That is a given inside one umbrella and an open problem between separate
  teams, repos and languages. The options — a shared package, a self-describing
  schema endpoint, a loose map-plus-type-string decode, an external registry —
  are not mutually exclusive, and
  [`GET /stream/stats`](ROADMAP.md#stream-stats-get-streamstats) is the small
  first step of the second. One constraint to carry into the decision: dropping
  `origin` rests on `message_type` being app-prefixed, so any option that leads
  to shared, non-app-prefixed module names (`MyOrg.Messages.OrderPlaced`
  appended by several apps) reopens the question of how a consumer knows which
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
- **Whether the pacing defaults are right under real load.** They are in
  `DMEx.Config` with the reasoning beside each one, but they have only been
  exercised against benchmarks and a deliberately slow test handler. The one
  that interacts with something a producer does not control is `max_batch_size`:
  a write does not complete until the consumer has drained it, so
  `max_batch_size / slowest-consumer-rows-per-second` has to stay well under
  `stall_timeout`. See [the deployment guide](guides/deployment.md).
- ~~**Coalescing consecutive skips into one cursor advance.**~~ **Settled, and
  built.** A skip runs no handler, so its advance is atomic with nothing;
  deferring it into a pending marker and writing it on a `checkpoint_interval`
  timer costs a crash only the re-skipping of rows no code ran for. `last_seen`
  does now advance in variable-size jumps, which was the stated reservation —
  in practice nothing reads it as a per-row counter, and the two paths that
  could have drifted (a reconnect and a clean stop) both flush first. A handled
  message needs no flush of its own because
  `advance/3` is absolute and subsumes the pending run below it, so the
  handler/cursor transaction is untouched. See `DMEx.Handler.Pipeline`,
  section 2a. Server-side selection has since removed most of the motivation —
  a selective stream carries few rows it does not want — but the skip path is
  still live for filter false positives and `selective: false` handlers, so this stays
  worth having.
- **Where the read-amplification ceiling actually is**, and whether the pool
  wants partitioning so stream reads cannot starve append writes. Type-routed
  wakeups moved the ceiling rather than settling it: an append now wakes only
  the connections that selected its type, so the amplification is per-type
  rather than per-connection, and where *that* binds has not been measured.
- **Whether a slow consumer needs anything beyond the receive window.** With no
  consumer-side buffer, a consumer too slow for its producer's pacing simply
  stops reading and is eventually reclaimed (invariant 5), reconnects, and does
  it again. That is correct and costs a re-read each time, but it is a loop, and
  the only lever is the producer's `max_batch_size` — which one slow consumer
  cannot ask to be lowered for it alone. Is a steady
  `[:dmex, :stream, :reclaim]` rate an adequate signal to an operator, or does
  the protocol eventually need a way for a consumer to say "less at a time"?
- **Third-party client backpressure** — invariant 8 is a property of *DMEx's*
  client. Any SSE-capable client can consume a stream, and one that bounds
  neither its socket nor its queue is indistinguishable to the producer from a
  genuinely fast one. Documented compatibility caveat, or something detectable?
- **What triggers reopening
  [aggregate limits](ROADMAP.md#global-resource-limits)** — a connection-count
  threshold, an observed incident, or memory pressure the per-connection bounds
  demonstrably failed to contain? Worth deciding the tripwire before
  rediscovering the need in production.
- **Whether `max_batch_bytes` wants to be adaptive per connection.** It is a
  producer-wide constant, so one connection serving a consumer on a slow link
  gets the same budget as one on a fast one. That is the same "one owner" trade
  invariant 9 makes deliberately, and the same question would reopen it.
- **Whether a selection should be narrowable below the handler's message set.**
  A handler that can decode twenty types but only wants five must either receive
  fifteen it discards or split into two handlers and two cursors. "Your module
  list is your subscription" is the simpler rule and it is what ships; an
  explicit override on `subscribe/2` is the escape hatch for a decode vocabulary
  genuinely wider than its interest.
- **Whether a selective cursor wants a second cursor**, and it is probably
  answered by the index rather than open. With a sparse subset the last-*sent*
  id no longer implies the rows in between were examined and rejected, so
  `last_seen` lags the log tip by however much of it went to somebody else.
  `(message_type, id)` makes resuming from it an index seek costing the same
  whether the next matching row is one id away or ten million — so the lag is
  visible but not expensive. Worth revisiting only if a real deployment shows
  otherwise.
- **A rolling multi-node producer deploy.** Expansion is refreshed at connect
  and the module set changes only on deploy, which is safe because a deploy
  takes every connection with it — on one node. A connection pinned to a node
  running old code can outlive a node running new code that has begun
  appending a type the old node's expansion does not know. That belongs to the
  cross-node clustering question above, not to selection.
- **`:skip` visibility** — `:skip` drops a message with no DMEx-side record
  beyond telemetry, unlike `:dead_message`. Intentional as a "bring your own
  observability" mode, or should it always leave a trace?
