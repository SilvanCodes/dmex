# DMEx — Architecture

> The code is the spec of record, except for the wire encodings in `guides/`,
> which are normative. This is the map: the wire contract a third-party client
> implements against, the invariants the design rests on, and where each lives.
> Module docs describe their own mechanism; the reasoning that spans modules is
> here. [ROADMAP.md](ROADMAP.md) covers what is designed but not built.

## Overview

A decentralized message queue with **no single point of failure**. Every
application that appends owns and persists its own append-only log — one SQL
table — and streams it to consumers over HTTP with Server-Sent Events. No
broker, no cluster, no shared infrastructure.

- **Simple** — a SQL table and an HTTP endpoint.
- **No SPOF** — a stream's availability depends only on its own application and
  its own database.
- **Replayable** — consumption removes nothing; any consumer can start or
  restart anywhere in history.

A subscription receives the message types its handler can decode, resolved
server-side, so unselected rows are never read, framed or sent. A bare
`GET /stream` still streams the whole log, which is what a browser
`EventSource` or `curl` asks for.

**Prior art.** [MDN: server-sent events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events#event_stream_format)
for the wire format; [Oban](https://github.com/oban-bg/oban) for
`LISTEN`/`NOTIFY` backed by polling and an engine abstraction;
[Commanded](https://github.com/commanded/commanded) for one module per message
type over an append-only Postgres log.

## The log

One append-only log per application: a totally-ordered stream of every message
it appended, in one table. `DMEx.Migration` ships the DDL.

```sql
CREATE TABLE dmex_messages (
  id            bigserial   PRIMARY KEY,
  message_type  text        NOT NULL CHECK (message_type ~ '^[A-Za-z0-9_.]+$'),
  data          jsonb       NOT NULL,
  metadata      jsonb       NOT NULL DEFAULT '{}',
  inserted_at   timestamptz NOT NULL DEFAULT now()
);
```

- `id` is the offset and the only ordering and resumption mechanism. `bigint`
  because a log nothing deletes from outgrows 2³¹.
- `message_type` is the appending module's fully-qualified name, which is why
  nothing records *who* appended a row.
- `metadata` is entirely publisher-owned; DMEx writes nothing into it, so there
  are no reserved keys.
- `inserted_at` is observability only, never an ordering key.
- `message_type` is constrained to `[A-Za-z0-9_.]+` — an Elixir module name's
  charset — because it goes on an SSE `event:` line, and SSE field lines have no
  escaping.

**Two indexes, one per read shape.** The primary key serves a whole-log
`id > cursor` range scan; `(message_type, id)` serves a selective one, and the
planner takes whichever is cheaper (see *What has been measured*).

**A row says what a message *is*, never who might want it.** A grouping stamped
in at append time would be immutable with the table: fixed message by message,
unable to apply to history or to follow readers as they change. So
`message_type` is the only classification a row carries, and who wants what is
resolved at read time.

## Message-type selection

**The selection is the handler's clauses.** `DMEx.Handler` already derives a
subscription's message set at compile time from the structs `handle_message/2`
matches on — it has to, or a consumer could not turn a `message_type` back into
a struct. That set is also the selection. A name standing for a set of types would
be a second vocabulary to define, version, discover and keep in sync, and the
two can disagree: naming a type the handler cannot decode, or omitting one it
can, both silently. Derived, that state is unrepresentable.

**It travels as an XOR filter in the path.** Module names run 25–40 bytes each,
so spelling them out exhausts a conservative request line at around sixty types.
A 16-bit XOR filter encodes the same set at ~2.46 bytes per type, putting the
ceiling near 2 400. The blob is built at compile time and is a pure function of
the message set, so it is a constant on `__dmex__/0` and the same handler
compiles to the same URL everywhere. `guides/fingerprint-spec.md` is the
normative encoding; `DMEx.Fingerprint` implements it.

**Errors point one way, and that is the whole reason it works.** There are no
false negatives, so every selected type is delivered. A false positive admits
one extra type, which the consumer's ordinary skip path drops — so the skip path
is a defence rather than dead code.

**The producer resolves it against its own compiled modules**, not against what
its log contains. `SELECT DISTINCT message_type` goes stale silently: a producer
can know a type it has not yet emitted, and first emission involves no restart
and no reconnect — while the module set changes only on deploy, and a deploy
takes every SSE connection with it. `DMEx.Log.Messages` scans the producer's OTP
application and every loaded application depending on `:dmex`, which is exactly
the set whose modules could contain a `use DMEx.Message`. The result only has to
be a *superset* of what the producer appends: an extra type never matches, a
missing one is silent loss.

**Not a partitioning.** There is still one totally-ordered log and one id
sequence. A selective subscription sees a sparse subset of that same id space,
so every consumer's `last_seen` is comparable with every other's. A selective
cursor lags the log by however much of it went to somebody else, which costs
nothing to resume from: `(message_type, id)` makes the next read an index seek,
the same price whether the next matching row is one id away or ten million.

**Selection does not reach the append path.** A publisher does not know, and
does not need to know, who selects its types.

**The escape hatch is on the handler.** `selective: false` requests the whole
log and filters locally. It exists to tell two failures apart when a
subscription receives nothing — a producer or encoding mismatch, which whole-log
delivery fixes immediately, from a producer that has appended nothing, which it
does not. It lives on the module for the same reason the producer, the id and
the message set do: the module is the complete declaration of what a
subscription *is*.

**A filter matching nothing is a `404`.** An empty expansion is
indistinguishable on the wire from a healthy stream on a quiet log, so the
alternative is a connection that opens and says nothing forever. It is not proof
of a client bug — a consumer deployed ahead of the producer that will emit its
types is legitimately in that state — so DMEx's consumer retries it rather than
stopping the way it stops on a `400`.

## The wire contract

`DMEx.Transport.SSE.Frame` encodes and parses it; `DMEx.Transport.SSE.Decoder`
handles chunk-boundary reassembly.

**`GET /stream`** and **`GET /stream/<blob>`**, both mounted by `DMEx.Log.Plug`
through one prefix `forward`. The bare form is the whole log; the blob narrows
it. There is no application segment and no query string: one mount serves
exactly one log, and the mount path plus hostname identify it.

`/stream/...` is a namespace with a closed reserved list matched *before* any
blob decode. `stats` is reserved and not yet implemented, answered `404`. A
segment that is neither reserved nor decodable is a `400`. A trailing slash is
the same resource as the bare mount. Any method other than `GET` is a `405`
with an `Allow: GET` header.

**Request headers**, validated by `DMEx.Log.Params`:

| header | meaning |
| --- | --- |
| `Last-Event-ID` | resume position: `previous/current` as echoed from a frame's `id:`, a bare integer id, the literal `latest` (start from the tip), or absent (the whole log). Anything else is a `400`. Only `current` is used — the predecessor is a consumer-side check. |

**That is the whole list. DMEx defines no request headers of its own.** Pacing
is producer configuration, so there is nothing for a consumer to ask for, clamp,
or echo back. The consequence is that **DMEx's own consumer and a browser's
`EventSource` send byte-identical requests**, and everything distinguishing one
subscription from another is in the request line.

That also settles where the selection goes. A selection is *identity* — a
different selection is a different resource — so it belongs in the path:

- **A browser `EventSource` can select.** It sets no request headers beyond
  `Last-Event-ID`, but the URL is entirely its own.
- **Intermediaries treat paths as load-bearing and query strings as
  negotiable.** Caches and proxies routinely drop or key-strip query strings;
  nothing does that to a path. Losing a selection silently converts a selective
  subscription into a whole-log one — a bandwidth incident rather than an error.
- **A selection is stable, so the URL is stable.** One URL per deployment is a
  grouping worth having in a proxy log, a cache key or a dashboard.

**The response** is `text/event-stream` with `Cache-Control: no-cache`,
`X-Accel-Buffering: no` (nginx buffers by default, which would swallow both
pacing and heartbeats), and `Connection: keep-alive` on HTTP/1.1. A selective
stream additionally carries **`dmex-matched-messages`**, the count of the
producer's own types the filter admitted. It is the only DMEx header anywhere,
and it is diagnostic: the blob cannot be reversed, so this is the one thing that
makes a subscription legible from outside — a count of `0` where ten types were
selected is an encoding mismatch, visible at connect rather than through
silence.

**Frames.** One frame per log row. A row has five columns and an SSE frame has
five slots, so the mapping is direct and there is no envelope:

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
| `inserted_at` | inside `data:` | append time, which the consumer's lag signal is measured against |

- **No batch envelope and no `max_id`.** Batching is a *write* batch, not a wire
  construct, so nothing on the consumer side has to know it happened. That lets
  `id:` be the row's own id, and makes a frame mean the same thing whether one
  row or five hundred went out together.
- **`data:` is one line**, so the payload is exactly a JSON document a client
  hands to its parser. JSON's grammar forbids raw control characters inside
  strings, which is what makes it safe to splice stored `jsonb` text in with no
  escaping pass.
- **How far behind is `now - inserted_at`**, in milliseconds: the wire carries no
  tip to subtract from. It compares two machines'
  clocks, so it is an NTP-accurate seconds-scale signal and not a finer one.
- **`id:` is a link, not a number.** `1041/1042` reads "the row before me was
  1041, I am 1042", and a consumer refuses a frame that does not claim the row
  it last saw. It cannot be inferred as `id - 1`, because a selective stream's
  ids are legitimately sparse and only the producer knows which gaps it made on
  purpose. The first frame links back to the cursor the read was issued against,
  so the chain also proves the producer resumed where it was asked to.

The cost of putting the type on `event:` is that `EventSource.onmessage` stops
firing — named events need `addEventListener`, which is the ordinary way to
write an `EventSource` against a typed stream. `curl -N` shows the type on its own line.

**Heartbeats** are SSE comment frames (`: hb`) on an idle interval, ignored by
every spec-compliant client. `retry:` is sent at stream open carrying the
**effective** interval — `heartbeat_ms` plus one batch interval, because the
send loop's throttle runs after the heartbeat branch and that sum is the real
upper bound on silence. A generic client reads it as a reconnect delay; a DMEx
consumer reads it as "expect a frame or a comment within this window" and
reconnects after three missed ones.

Because the protocol is just HTTP and SSE, no custom client library is required.

## The invariants

The properties the design is built to have, and where each is enforced.

**1. Appends are serialized per app.** Postgres allocates sequence values at
INSERT time but makes rows visible at COMMIT time, so two concurrent appends can
commit out of allocation order — and a reader scanning `id > cursor ORDER BY id`
would advance straight over the row that had not committed yet and lose it
**permanently and silently**. Every read path is such a cursor, so `DMEx.Log`
appends under a transaction-scoped advisory lock keyed on the app
(`DMEx.Storage.Postgres`). The cost is append concurrency; the purchase is a
reader model with no gap-detection logic in it. (SQLite is single-writer
already.) Appending a *list* of messages is one lock, one INSERT and one COMMIT,
which is both how a producer emits messages that only make sense together and
how it stops a bulk import from paying the lock once per row.

**2. Producer memory per connection is one batch, bounded in rows *and*
bytes.** The send loop in `DMEx.Log.Stream` holds at most the rows it is
writing; there is no accumulator. Two bounds because a row costs heap two ways:
its payload, which `max_batch_bytes` counts, and a fixed ~1 KB for the map, the
`%DateTime{}`, the frame's iodata cells and the timestamp binary — which no
budget in database octets can see. At a 1 KB payload the byte bound binds; at a
50-byte payload the same 1 MiB budget admits ~21 000 rows whose fixed cost alone
is ~22 MB, and `max_batch_size` closes that end.

Both bounds are applied **inside the query** (`c:DMEx.Storage.read/5`): a read
that returns `max_batch_size` rows and lets the caller trim them has already
spent the memory the bound exists to cap. One deliberate exception — a single
row larger than the whole budget is returned alone, because refusing it would
wedge the subscription forever.

**3. Reads are gated on sends completing.** The key inversion. If reads were
driven by the publisher's rate, that decoupling *is* an unbounded buffer. Here
the next read cannot happen until the previous write returns, so producer-side
overload is not detected and handled, it is unrepresentable. A stalled consumer
stalls its own loop and nothing else: no more rows, no more memory, no more
queries.

**4. The request process *is* the stream.** No per-connection `GenServer`, no
producer-side supervisor. In both Bandit and Cowboy the connection process owns
the socket, so `chunk/2` works naturally, the socket closes when the process
dies, and reclaiming a connection means killing the process blocked in the
write. `DMEx.Log.Plug` runs the loop inline.

**5. Every blocking step is taken under a deadline.** A process blocked in
`chunk/2` cannot time itself out, because it is not running — nor can one
blocked in a read against a database that stopped answering. So the loop arms a
deadline in its `DMEx.Log.Status` before each blocking step, and
`DMEx.Log.Watcher` kills whatever is past one. This needs no transport
configuration and reaches wedges a socket timeout structurally cannot see. See
[the deployment guide](guides/deployment.md).

**6. A wakeup is a hint, never a carrier of work.** `DMEx.Log.Dispatcher`
broadcasts a bare atom on `NOTIFY` or on a poll that saw the tip move; each
stream reads from its own cursor to whatever is there now. Missing one costs
latency (bounded by the poll interval) and never correctness, so there is no
gap-versus-overlap reconciliation anywhere.

**7. Dropping a connection is not a delivery event.** The producer holds no
consumer state, the consumer's `last_seen` is durable, and resumption is the
same `id > last_seen` read a cold connect does. That is what makes it safe to be
aggressive about reclaiming, and why there is no in-band "you are being dropped"
frame — it would have to be written to the very socket that is stalled.

**8. The consumer has no buffer, and that is what makes the backpressure
exact.** `DMEx.Handler.Stream` runs each row through the pipeline **inline, on
the socket path**, as it is decoded. `Mint.HTTP.stream/2` re-arms `active:
:once` itself, so while the process is inside a handler nothing is re-armed, the
TCP receive window closes, and the producer's `chunk/2` blocks on it. The most
that can be in flight is one decoded chunk plus one socket message in the
mailbox. A buffer would only relocate the problem: a few hundred rows of slack
absorb nothing at a sustained mismatch, and every row of slack is one more the
socket accepts before it says stop.

**9. Pacing has exactly one owner, and it is stated as a rate.**
`max_batch_frequency`, `max_batch_size` and `max_batch_bytes` are producer
configuration and appear nowhere else — not in a request, not in a response, not
in a subscription's options. There is no "requested" value to drift from an
"effective" one, no clamping, no echo to interpret, no mid-subscription
renegotiation. `max_batch_frequency` is **how hard one connection may hammer the
database**, and it is a ceiling rather than a reaction to load: a hundred
connections at the default five reads per second is five hundred short range
scans a second, whatever the append rate does. The producer is the party whose
database, memory and request processes all three protect.

**10. Batching is a write batch, never a processing unit.** The producer encodes
a read's rows as one frame each and writes them together; the consumer sees a
plain run of one-row frames and runs each through `DMEx.Handler.Pipeline`
separately — its own transaction, handler call, cursor advance and failure
handling. A mid-batch failure rolls back exactly one message.

**11. The handler sees its message and the publisher's metadata, nothing else.**
No id, no `message_type`, no frame. A handler is a plain function of its domain
data plus whatever context the publisher attached, testable by constructing a
struct and a map.

**12. A gap in delivery is detected, never stepped over.** Every frame states
its predecessor and `DMEx.Transport.SSE` refuses one that does not claim the row
it last saw, before duplicate suppression can hide it. Without this the cursor
model has a hole it cannot see: the cursor is the only record of what was
processed, so a message lost between two that arrived would have its id put
permanently behind the cursor by the next row's advance.

TCP does not silently drop bytes mid-stream, so what this defends against is a
mangling intermediary, a third-party producer whose read skips rows, and
`DMEx.Transport.SSE.Frame.parse/1`, which discards any block missing `id:`,
`event:` or `data:`.
A break drops the connection and re-reads from the durable cursor: transport
damage repairs itself, and a producer that really does skip rows becomes a
visible reconnect loop making no progress.

**13. A subscription's identity is its declaration, not its module name.** The
`"log/id"` pair in `use DMEx.Handler` keys the cursor, so renaming a module is
free and only changing the declaration costs a migration
(`DMEx.Handler.Cursors`). The message set is deliberately *not* part of it:
adding a clause for a new type handles it from the tip onward rather than
silently replaying the entire log.

## The process map

Everything is per-connection; nothing is pooled or shared except the small set
each log supervises.

**The log.** `MyApp.Log` (via `DMEx.Log`) supervises:

| module | role |
| --- | --- |
| `DMEx.Log.Registry` | one entry per open stream, holding that connection's `DMEx.Log.Status` (wakeup flag and deadline) and its selection. The entry dies with the request process. |
| `DMEx.Storage` children | whatever the adapter needs; Postgres puts its `LISTEN` connection here. |
| `DMEx.Log.Dispatcher` | subscribes to the adapter's notification connection, runs the poll timer, broadcasts wakeups. |
| `DMEx.Log.Watcher` | sweeps deadlines and reclaims connections that stopped making progress. |

None ever holds message data on a connection's behalf. The connections
themselves are processes running `DMEx.Log.Stream`: a web-server request process
when the reader is remote, and the reader process `DMEx.Transport.Local` starts
when it is in this VM. Both register in the same registry, so wakeups, pacing
and stall reclaim treat them identically.

**Subscriptions.** `DMEx.Supervisor` supervises one `DMEx.Handler.Stream` per
subscription under a `DMEx.Handler.Supervisor`, each owning one cursor and one
`DMEx.Transport`, running `DMEx.Handler.Pipeline` inline on the rows that
transport yields. The transport is inferred from the log's binding — a module is
read in this VM, a URL over HTTP. There is nothing else in a subscription: no
queue process, no controller, no drain loop. Restart intensity is set never to
trip, so a subscription that crash-loops does so on its own, at a pace set by
jittered backoff, and its siblings never notice.

**Storage.** One `DMEx.Storage` behaviour covers both halves — the producer's
log and the consumer's `dmex_cursors` and `dmex_dead_messages` — rather than
inventing a second abstraction for bookkeeping. `DMEx.Storage.Postgres` is the
shipped adapter; SQLite would be a new adapter rather than a new seam.

## Delivery semantics

**At-least-once, with duplicate suppression by id.** A row whose `id` is not
greater than the cursor is dropped before the handler runs, so transport-level
re-delivery never invokes a handler twice.

**This is not exactly-once.** The handler call and the `last_seen` advance share
one local transaction, so DMEx's bookkeeping cannot drift from the callback. But
that commit is also the *only* record that a message was handled, so a crash
after the handler's side effects land and before the transaction commits
redelivers a message indistinguishable from one never processed.

What follows for handler authors: **side effects through the consumer's own repo
are exactly-once for free**, because they roll back with the transaction that
would have recorded them. Effects that leave it — an HTTP call, a write to
another database — must be idempotent on something in the message or its
metadata, since the log id is deliberately not exposed. A local write plus a
separate outbox is the pattern that stays correct.

**No-loss is checked, not assumed.** At-least-once is only meaningful if it is
verifiable, and the cursor cannot verify it. So each frame states its
predecessor and the consumer refuses a stream that skips one (invariant 12).
That converts the one silent, permanent loss in the design into a loud reconnect
loop.

**Ordering** holds within one app's stream and never across streams.
**Retention** is unbounded: nothing is deleted as a side effect of consumption.

## Decentralization / no-SPOF

DMEx introduces no shared broker, cluster or coordination service whose failure
would affect multiple applications at once. It is **not** a claim of automatic
high availability for any individual stream — if a producer's database is down,
that producer's stream is unavailable exactly as any other data that application
owns would be. HA for one stream is HA for that application's database, and out
of scope.

## What DMEx deliberately does not do

- **No narrowing a selection below the handler's message set** — a consumer that
  can decode twenty types but only wants five receives fifteen it discards, or
  splits into two handlers and two cursors. An explicit override on
  `subscribe/2` is open.
- **No aggregate budget or admission control** — no cap on open connections, no
  `503`/`Retry-After`. Per-connection protection is complete (invariants 2, 3,
  5); aggregate pressure from many individually-healthy connections is not
  addressed, and neither is a consumer that is pathologically slow while still
  technically progressing. See
  [Global Resource Limits](ROADMAP.md#global-resource-limits).
- **No compression** — designed, not built. See
  [Compression](ROADMAP.md#compression).
- **No SQLite adapter** — the behaviour is complete; the adapter is not written.

## What has been measured

`bench/` answers design claims rather than catching regressions.

- **The append lock is nearly free uncontended and expensive under contention** —
  ~1.13× at concurrency 1–2, ~4.8× at 8, ~5.8× at 32, with locked throughput
  *falling* as concurrency rises. Append concurrency buys an application
  nothing; batch work into fewer, larger messages instead.
- **Per-connection pacing holds exactly as designed** — 5.2 writes/s against a
  200 ms window regardless of append rate, and an idle log costs one query per
  poll interval per *app* (1/s flat at both 10 and 200 connections).
- **No pool queueing at 200 concurrent streams** on a developer machine, so the
  read-amplification ceiling is somewhere above that.
- **The consumer is commit-bound on the rows it *handles*, not on the rows it
  skips.** A client that reads and discards sustains ~2 500 rows/s; a real
  consumer with a no-op handler manages ~265 rows/s, and that is the transaction
  it shares with the handler — the guarantee, not overhead. A fully-skipping consumer sustains
  ~2 515 rows/s, because coalescing turns 20 000 skipped rows into 7 cursor
  writes.

  At ~265 rows/s a million-row log of entirely handled messages takes roughly an
  hour to replay, and that is the figure "arbitrarily replayable" should be
  quoted with. A mostly-skipped log replays at wire speed.
- **The selection filter behaves as the design assumes.** `mix bench.fingerprint`
  sweeps 86 set sizes over 1–2000: **zero false negatives** across 45 660
  membership tests, a false-positive rate of **14.65 ± 1.85 per million** against
  2⁻¹⁶'s expected 15.26, and **2.49 bytes per type** raw (3.33 after base64url)
  against the 2.46 the sizing argument claims. `recovered / encoded` is a flat
  1.0 at 84 of 86 points against a producer knowing 10× the types selected,
  worst case 1.00069.

  That flatness is the evidence for 16-bit fingerprints rather than 8. Since
  membership is tested per *type* and not per row, a false positive admits one
  entire message type for the life of a connection — at 8 bits the same universe
  would admit a spurious type at roughly every other point, and one unlucky draw
  is a connection receiving all of the highest-volume type in the system.

  It also puts a number on the request-line ceiling: **~2 400 message types** at
  an 8 KB limit.
- **The `(message_type, id)` index is taken exactly when it pays.** `EXPLAIN` on
  a 500k-row skewed log (one type at 98.9%, one at 1%, one at 0.1%): the 0.1%
  type uses the index; the 1% type or larger uses the primary key and filters, at
  fewer buffers than the index plan would cost. A hand-written per-type `LATERAL`
  seek — the usual loose-index-scan workaround — is *worse* everywhere except the
  most extreme selectivity, because each branch fetches a whole `LIMIT` per type
  and discards the surplus. So plain `message_type = ANY($1)` is the right query
  and the planner is left to choose.

## Open questions

- **Auth / access control** — who may read a producer's endpoint, and how is
  that enforced? Nothing here answers this.
- **Sharing message modules across independent applications.** `DMEx.Message`
  assumes the consumer has the *same* Elixir module the producer appended from.
  That is a given inside one umbrella and an open problem between separate teams,
  repos and languages. The options — a shared package, a self-describing schema
  endpoint, a loose map-plus-type-string decode, an external registry — are not
  mutually exclusive, and [`GET /stream/stats`](ROADMAP.md#stream-stats-get-streamstats)
  is the small first step of the second. One constraint: dropping `origin` rests
  on `message_type` being app-prefixed, so any option leading to shared,
  non-app-prefixed module names reopens how a consumer knows which application a
  message came from.
- **Cross-node clustering** — if a producer runs as a multi-node OTP cluster, how
  are the log's single-writer property and the `LISTEN`/`NOTIFY` dispatcher kept
  consistent across nodes? A rolling multi-node deploy belongs here too:
  expansion is refreshed at connect and the module set changes only on deploy,
  which is safe because a deploy takes every connection with it — on one node. A
  connection pinned to a node running old code can outlive a node running new
  code that has begun appending a type the old node's expansion does not know.
- **Payload byte fidelity** — `jsonb` normalizes key order, drops duplicate keys
  and loses whitespace, and the wire envelope is re-serialized from storage, so
  the bytes a consumer receives are not the bytes the publisher encoded. An
  integration test pins this. Open: document it as a hard caveat (no end-to-end
  signing of raw payload bytes), or add an exact-bytes column?
- **Retention / compaction** — logs grow forever. What operator-facing controls
  exist, and how do they interact with "arbitrarily replayable"?
  `dmex_dead_messages` is the easier half: its `inserted_at` is when a row landed
  there, so "abandoned more than N days ago" is expressible. Whether DMEx should
  ship the pruning is open.
- **Whether the pacing defaults are right under real load.** They are in
  `DMEx.Config` with reasoning beside each, but have only been exercised against
  benchmarks and a deliberately slow test handler. The one interacting with
  something a producer does not control is `max_batch_size`: a write does not
  complete until the consumer drains it, so
  `max_batch_size / slowest-consumer-rows-per-second` has to stay well under
  `stall_timeout`. See [the deployment guide](guides/deployment.md).
- **Where the read-amplification ceiling actually is**, and whether the pool
  wants partitioning so stream reads cannot starve append writes. Amplification is per-type rather than
  per-connection, and where it binds has not been measured.
- **Whether a slow consumer needs anything beyond the receive window.** With no
  consumer-side buffer, a consumer too slow for its producer's pacing stops
  reading, is reclaimed, reconnects, and does it again. That is correct and costs
  a re-read each time, but it is a loop, and the only lever is the producer's
  `max_batch_size` — which one slow consumer cannot ask to be lowered for it
  alone. Is a steady `[:dmex, :stream, :reclaim]` rate an adequate operator
  signal, or does the protocol need a way to say "less at a time"?
- **Third-party client backpressure** — invariant 8 is a property of *DMEx's*
  client. Any SSE-capable client can consume a stream, and one that bounds
  neither its socket nor its queue is indistinguishable to the producer from a
  genuinely fast one. Documented caveat, or something detectable?
- **`:skip` visibility** — `:skip` drops a message with no DMEx-side record
  beyond telemetry, unlike `:dead_message`. Intentional as a "bring your own
  observability" mode, or should it always leave a trace?
