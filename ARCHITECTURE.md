# DMEx — Architecture

> The code is the spec of record, except for the wire encodings in `guides/`,
> which are normative. This is the map: the wire contract a third-party client
> implements against, the invariants the design rests on, and where each lives.
> Module docs describe their own mechanism; the reasoning that spans modules is
> here. [ROADMAP.md](ROADMAP.md) covers what is designed but not built.

## Overview

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
it appended, in one table. `DMEx.Storage.Migration` runs it from a host
migration; the DDL itself is the adapter's, and `DMEx.Storage.Postgres.Migration`
is the authoritative column-by-column account — every type, default and
constraint, with the reasoning beside it. `DMEx.Storage.SQLite.Migration` is the
same schema on the other engine.

What belongs here is what the columns *mean*:

- **`id` is the offset**, and the only ordering and resumption mechanism there
  is.
- **`type` is the string the appending module declared as its `type:`**,
  independent of what that module is called, so a producer can reorganise its
  message modules without any of it reaching this column. `DMEx.Message` owns the
  charset and why it is narrow — the short version is that the string goes onto
  an SSE `event:` line, which has no escaping.
- **`metadata` is entirely publisher-owned.** DMEx writes nothing into this
  column, nothing into what goes on the wire, and nothing into the map a handler
  receives.
- **`inserted_at` is never an ordering key** — `id` is, and nothing in DMEx reads
  this column. It is observability, never delivered.

**Two indexes, one per read shape.** The primary key serves a whole-log
`id > cursor` range scan; `(type, id)` serves a selective one, and the
planner takes whichever is cheaper — `DMEx.Storage.Postgres.Migration` records
the `EXPLAIN` session that measured the crossover.

**A row says what a message *is*, never who might want it.** A grouping stamped
in at append time would be immutable with the table: fixed message by message,
unable to apply to history or to follow readers as they change. So
`type` is the only classification a row carries, and who wants what is
resolved at read time.

## Message-type selection

**The selection is the handler's clauses.** `DMEx.Handler` already derives a
subscription's message set at compile time from the structs `handle_message/2`
matches on — it has to, or a consumer could not turn a `type` back into
a struct. That set is also the selection. A name standing for a set of types would
be a second vocabulary to define, version, discover and keep in sync, and the
two can disagree: naming a type the handler cannot decode, or omitting one it
can, both silently. Derived, that state is unrepresentable.

**It travels as an XOR filter in the path**, and the trade is worth stating
precisely, because it is not free at every size. What travels is the declared
`type:` strings, not module names, and the filter has a fixed floor: `ceil(1.23n) +
32` slots rounded up to a multiple of three, at 2 bytes each plus an 8-byte seed,
so a *one-type* handler encodes to 107 base64url characters. Against a comma-joined sorted list of
ordinary type names (`acme.orders.event_n`, 19 characters):

| types | filter | literal list |
| ---: | ---: | ---: |
| 1 | 107 | 19 |
| 5 | 115 | 99 |
| 8 | 123 | 159 |
| 20 | 163 | 410 |
| 60 | 299 | 1 250 |
| 1 000 | 3 379 | 21 892 |

So the literal list wins below about six types and the filter wins from about
eight, unboundedly: at ~2.5 bytes per type it puts the ceiling near 2 400 types
against a conservative 8 KB request line, where a literal list reaches that line at
a few hundred. The filter is chosen for the shape of that curve rather than for any
one point on it — a URL whose length is a function of the selection rather than of
the producer's naming taste — and `mix bench.fingerprint` measures the bytes per
type it rests on.

The blob is built at compile time and is a pure function of the message set, so it
is a constant on `__dmex_handler__/0` and the same handler compiles to the same URL
everywhere. `guides/fingerprint-spec.md` is the normative encoding;
`DMEx.Handler.Fingerprint` implements it.

**Errors point one way, and that is the whole reason it works.** There are no
false negatives, so every selected type is delivered. A false positive admits
one extra type, which the consumer's ordinary skip path drops — so the skip path
is a defence rather than dead code.

**A compile-time edge is what keeps the baked set honest.** A handler embeds its
types twice over — the fingerprint it requests and its `__dmex_cast__/3` clauses
both come from one compile-time read of `c:DMEx.Message.type/0` — so a `type:`
that changed underneath them would leave a subscription selecting nothing and
skipping whatever arrived anyway. Matching on a struct is only an *export*
dependency, and a `type:` is a function body, which an export digest does not
cover. `DMEx.Handler` therefore emits a call per message module into the
handler's own body, making the dependency a compile one: editing a `type:`
recompiles every handler that named it, within a project and across a dependency
alike. Nothing re-checks at runtime, because after that edge only three things
can desynchronise the two, and none of them is a compile:

  * **A module replaced without one** — defined or reloaded through
    `Code.eval_string/2` and friends, a partially hot-loaded release, or a build
    assembled from two compilations. Rebuilding from clean is the fix.
  * **Skew between deployments** — a producer renamed a type while a consumer
    built earlier is still running. No check inside one VM can see this: the
    consumer's own modules and its own baked fingerprint agree with each other.
    If *every* declared type went missing it is the `404` above; if only some
    did, those rows are simply not delivered until the consumer is rebuilt.
  * **The rollout window of such a rename**, where both versions are live at
    once and each is internally consistent.

The last two are why changing a `type:` is a **migration, not a rename**: rows
already appended keep the old string forever, so a consumer resuming from an old
cursor cannot select its own history either, and dead messages recorded under it
can never be replayed. The path is `retired:` — declare the new string as
`type:` and the old one alongside it, so the module answers for both. Producers
append under the new one from that deploy; consumers select and decode both, so
history stays readable across the cut and through the rollout window where two
versions are live. The declaration is permanent, because the rows are: dropping
it later re-orphans exactly what it was added for. `DMEx.Message` has the rules.

**The producer resolves it against its own compiled modules**, not against what
its log contains. `SELECT DISTINCT type` goes stale silently: a producer
can know a type it has not yet emitted, and first emission involves no restart
and no reconnect — while the module set changes only on deploy, and a deploy
takes every SSE connection with it. `DMEx.Message.types/0` scans the producer's OTP
application and every loaded application depending on `:dmex`, which is exactly
the set whose modules could contain a `use DMEx.Message`. The result only has to
be a *superset* of what the producer appends: an extra type never matches, a
missing one is silent loss.

**Not a partitioning.** There is still one totally-ordered log and one id
sequence. A selective subscription sees a sparse subset of that same id space,
so every consumer's `last_seen` is comparable with every other's. A selective
cursor lags the log by however much of it went to somebody else, which costs
nothing to resume from: `(type, id)` makes the next read an index seek,
the same price whether the next matching row is one id away or ten million.

**Selection does not reach the append path.** A publisher does not know, and
does not need to know, who selects its types.

**The escape hatch is a clause.** A `handle_message/2` clause whose first
argument names no type — a plain variable, or `%module{}` — builds no
fingerprint, so the subscription requests the whole log and filters locally. It
also decodes more than it declared: a type no clause named resolves through the
message modules the consumer's application knows, so such a handler receives
everything it *could* decode and skips only what nothing here can. It exists to tell two
failures apart when a subscription receives nothing — a producer or encoding
mismatch, which whole-log delivery fixes immediately, from a producer that has
appended nothing, which it does not. There is no option for it, for the same
reason there is none for the message set: the clauses are the declaration, and a
second place to say it could disagree with them.

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

The status vocabulary for a malformed segment, an unmatched filter, a trailing
slash and a non-`GET` method is
[fingerprint-spec §11](guides/fingerprint-spec.md#11-the-stream-namespace).

**Request headers**, validated by `DMEx.Log.Params`:

| header | meaning |
| --- | --- |
| `Last-Event-ID` | resume position: one non-negative integer, delivery starting after it — which is exactly a frame's `id:` echoed back. `0` is the whole log. Absent means from the log's tip at connect, so a consumer with nothing to resume from sees only what is appended next. Anything else is a `400`. There is no tip token, because the header's absence is one. |

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
stream additionally carries **`dmex-matched-types`**, the count of the
producer's own types the filter admitted. It is the only DMEx header anywhere,
and it is diagnostic: the blob cannot be reversed, so this is the one thing that
makes a subscription legible from outside — a count of `0` where ten types were
selected is an encoding mismatch, visible at connect rather than through
silence.

**Frames.** One frame per log row, in three slots, with no envelope:

```
id: 1042
event: order_shipped
data: {"data":{"order_id":7},"metadata":{"trace_id":"abc"}}

```

| column | goes to | why there |
| --- | --- | --- |
| `id` | `id:` | it *is* the resume position, so a generic client's `Last-Event-ID` works with no DMEx knowledge |
| `type` | `event:` | it *is* the event name, so `addEventListener("order_shipped", …)` works in a browser |
| `data` | inside `data:` | the publisher's payload, verbatim |
| `metadata` | inside `data:` | the publisher's metadata, verbatim |
| `inserted_at` | nowhere | observability only, and a field on every frame for a signal most consumers never read |

Inside `data:`, **both slots are mandatory and both are JSON objects**, at worst
`{}`: a payload missing one, or carrying `null` or a non-object, is a malformed
message and the transport rejects it. That is what lets a consumer-side row be
total — nothing below a transport defaults or re-checks either field.

- **No batch envelope and no `max_id`.** Batching is a *write* batch, not a wire
  construct, so nothing on the consumer side has to know it happened. That lets
  `id:` be the row's own id, and makes a frame mean the same thing whether one
  row or five hundred went out together.
- **`id:` is a number and the position vocabulary is one number.** A frame's
  `id:`, a `Last-Event-ID`, `dmex_cursors.last_seen` and a subscription's
  `last_seen:` option are the same thing in the same units — exclusive, so
  delivery starts above it. The round trip is an echo: no parsing, no
  arithmetic, and no second spelling of any position anywhere in DMEx.
- **`data:` is one line**, so the payload is exactly a JSON document a client
  hands to its parser. JSON's grammar forbids raw control characters inside
  strings, which is what makes it safe to splice stored `jsonb` text in with no
  escaping pass.
- **Nothing DMEx owns rides inside `data:`.** No clock is on the wire anywhere:
  a position is an id, and only the producer's ids order the log. How far behind
  a consumer is is therefore not answerable from a frame.

The cost of putting the type on `event:` is that `EventSource.onmessage` stops
firing — named events need `addEventListener`, which is the ordinary way to
write an `EventSource` against a typed stream. `curl -N` shows the type on its own line.

**Heartbeats** are SSE comment frames (`: hb`) on an idle interval, ignored by
every spec-compliant client. `retry:` is sent at stream open carrying the
**effective** interval — `heartbeat_interval` plus one batch interval, because the
send loop's throttle runs after the heartbeat branch and that sum is the real
upper bound on silence. A generic client reads it as a reconnect delay; a DMEx
consumer reads it as "expect a frame or a comment within this window" and
reconnects after three missed ones.

Because the protocol is just HTTP and SSE, no custom client library is required.

## The invariants

The properties the design is built to have, and where each is enforced. They are
named rather than numbered, because everything that cites one cites it by name —
inserting a thirteenth renumbers nothing.

### Appends are serialized per app

Postgres allocates sequence values at
INSERT time but makes rows visible at COMMIT time, so two concurrent appends can
commit out of allocation order — and a reader scanning `id > cursor ORDER BY id`
would advance straight over the row that had not committed yet and lose it
**permanently and silently**. Every read path is such a cursor, so `DMEx.Log`
appends under a transaction-scoped advisory lock keyed on the app
(`DMEx.Storage.Postgres`). The cost is append concurrency; the purchase is a
reader model with no gap-detection logic in it. The requirement is *assignment
order equals commit order*, and the lock is only Postgres's way of getting
there: SQLite has one writer database-wide, so a second writer cannot draw a
rowid until the first commits, and `DMEx.Storage.SQLite` takes no lock at all.

### Producer memory is one batch, bounded in rows and bytes

The send loop in `DMEx.Log.SendLoop` holds at most the rows it is
writing; there is no accumulator. Two bounds because a row costs heap two ways:
its payload, which `max_batch_bytes` counts, and a fixed ~1 KB for the map, the
`%DateTime{}` and the frame's iodata cells — which no budget in database octets
can see. At a 1 KB payload the byte bound binds; at a 50-byte payload the same
1 MiB budget admits ~21 000 rows whose fixed cost alone is ~22 MB, and
`max_batch_rows` closes that end.

Both bounds are applied **inside the query** (`c:DMEx.Storage.read/5`): a read
that returns `max_batch_rows` rows and lets the caller trim them has already
spent the memory the bound exists to cap. One deliberate exception — a single
row larger than the whole budget is returned alone, because refusing it would
wedge the subscription forever.

### Reads are gated on sends

The key inversion. If reads were
driven by the publisher's rate, that decoupling *is* an unbounded buffer. Here
the next read cannot happen until the previous write returns, so producer-side
overload is not detected and handled, it is unrepresentable. A stalled consumer
stalls its own loop and nothing else: no more rows, no more memory, no more
queries.

### The request process is the stream

No per-connection `GenServer`, no
producer-side supervisor. In both Bandit and Cowboy the connection process owns
the socket, so `chunk/2` works naturally, the socket closes when the process
dies, and reclaiming a connection means killing the process blocked in the
write. `DMEx.Log.Plug` runs the loop inline.

### Every blocking step has a deadline

A process blocked in
`chunk/2` cannot time itself out, because it is not running — nor can one
blocked in a read against a database that stopped answering. So the loop arms a
deadline in its `DMEx.Log.Status` before each blocking step, and
`DMEx.Log.Watcher` kills whatever is past one. This needs no transport
configuration and reaches wedges a socket timeout structurally cannot see. See
[the deployment guide](guides/deployment.md).

### A wakeup is a hint

Never a carrier of work. `DMEx.Log.Dispatcher`
broadcasts a bare atom on `NOTIFY` or on a poll that saw the tip move; each
stream reads from its own cursor to whatever is there now. Missing one costs
latency (bounded by the poll interval) and never correctness, so there is no
gap-versus-overlap reconciliation anywhere.

### Dropping a connection is not a delivery event

The producer holds no
consumer state, the consumer's `last_seen` is durable, and resumption is the
same `id > last_seen` read a cold connect does. That is what makes it safe to be
aggressive about reclaiming, and why there is no in-band "you are being dropped"
frame — it would have to be written to the very socket that is stalled.

### The consumer has no buffer

And that is what makes the backpressure
exact. A transport's reader hands each decoded batch to
`DMEx.Handler.Subscription` with `DMEx.Transport.deliver/2` — a call that
returns only once the pipeline has run every row — and reads again only on its
return. So while the subscription is inside a handler the reader is blocked in
the call, `Mint.HTTP.stream/2` is not called again, the TCP receive window
closes, and the producer's `chunk/2` blocks on it. The most that can be in
flight is one decoded batch plus one socket message in the reader's mailbox. A
buffer would only relocate the problem: a few hundred rows of slack absorb
nothing at a sustained mismatch, and every row of slack is one more the socket
accepts before it says stop.

### Pacing has one owner

And it is stated as a rate.
`max_batch_frequency`, `max_batch_rows` and `max_batch_bytes` are producer
configuration and appear nowhere else — not in a request, not in a response, not
in a subscription's options. There is no "requested" value to drift from an
"effective" one, no clamping, no echo to interpret, no mid-subscription
renegotiation. `max_batch_frequency` is **how hard one connection may hammer the
database**, and it is a ceiling rather than a reaction to load: a hundred
connections at the default hundred reads per second is ten thousand short range
scans a second, whatever the append rate does. The producer is the party whose
database, memory and request processes all three protect — and at that default
the pool, not the query load, is what gives first, so an application expecting
many concurrent streams should lower it.

### Batching is a write batch

Never a processing unit. The producer encodes
a read's rows as one frame each and writes them together; the consumer sees a
plain run of one-row frames and runs each through `DMEx.Handler.Pipeline`
separately — its own transaction, handler call, cursor advance and failure
handling. A mid-batch failure rolls back exactly one message.

### A handler sees only its message

Its message and a metadata map, nothing else. No frame,
no id, no `type` — the map is the publisher's, byte for byte. A handler
stays a plain function of its domain data plus context, testable by constructing
a struct and a map.

### An identity is its declaration

Not its module name. The `"log/id"` pair
in `use DMEx.Handler` keys the cursor and the `type:` in `use DMEx.Message` keys
the row, so renaming either module is free and only changing the declaration
costs anything — a migration for a cursor (`DMEx.Handler.Cursors`), and for a
message type, rows nothing can resolve any more. The message set is deliberately
*not* part of a subscription's identity:
adding a clause for a new type handles it from the tip onward rather than
silently replaying the entire log.

## The process map

Everything is per-connection; nothing is pooled or shared except the small set
each log supervises.

**The log.** `MyApp.Log` (via `DMEx.Log`) supervises:

| module | role |
| --- | --- |
| `DMEx.Log.Streams` | one entry per open stream, holding that connection's `DMEx.Log.Status` (wakeup flag and deadline) and its selection. The entry dies with the request process. |
| `DMEx.Notifier` children | whatever the notifier needs; `DMEx.Notifier.Postgres` puts its `LISTEN` connection here, `DMEx.Notifier.Beam` a `:pg` scope. |
| `DMEx.Log.Dispatcher` | subscribes to the notifier, runs the poll timer, broadcasts wakeups. |
| `DMEx.Log.Watcher` | sweeps deadlines and reclaims connections that stopped making progress. |

None ever holds message data on a connection's behalf. The connections
themselves are processes running `DMEx.Log.SendLoop`: a web-server request process
when the reader is remote, and the reader process `DMEx.Transport.Local` starts
when it is in this VM. Both register in the same registry, so wakeups, pacing
and stall reclaim treat them identically.

**Subscriptions.** `DMEx.Supervisor` supervises one `DMEx.Handler.Subscription` per
subscription under a `DMEx.Handler.Subscriptions`, each owning one cursor and one
linked reader process its `DMEx.Transport` starts, running
`DMEx.Handler.Pipeline` inline on the batches that reader hands over. The
transport is inferred from the log's binding — a module is read in this VM, a
URL over HTTP — and everything that can go wrong inside a reader reaches the
subscription as one exit signal. There is nothing else in a subscription: no
queue process, no controller, no drain loop. Restart intensity is set never to
trip, so a subscription that crash-loops does so on its own, at a pace set by
jittered backoff, and its siblings never notice.

**Storage.** One `DMEx.Storage` behaviour covers both halves — the producer's
log and the consumer's `dmex_cursors` and `dmex_dead_messages` — rather than
inventing a second abstraction for bookkeeping. `DMEx.Storage.Postgres` and
`DMEx.Storage.SQLite` are the shipped adapters, and one conformance suite pins
the behaviour's contracts against both.

**Notification is a separate axis.** `DMEx.Notifier` answers "how does a live
stream learn about an append", which is not the same question as "where do rows
live": Postgres has `LISTEN`/`NOTIFY`, SQLite has nothing, and a Postgres
deployment behind a transaction-pooling PgBouncer has Postgres and still cannot
use it. So a log names a storage adapter and a notifier independently, the
adapter merely defaults the notifier. `DMEx.Notifier.Beam` carries wakeups as
BEAM messages over `:pg`, which reaches this node always and the cluster when
there is one; what it does not reach — another OS process writing the same SQLite
file — the poll does.

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
metadata — the log id does not reach a handler at all, so idempotency has to
rest on something the publisher put there. A local write plus a separate outbox
is the pattern that stays correct.

**No-loss rests on the producer.** The consumer cannot verify it: a position is
one id, so a row lost *between* two that arrived is indistinguishable from a
selective stream's legitimate sparseness, and the next row's advance puts the
missing id permanently behind the cursor. What defends against it is on the
producer's side — id assignment order equal to commit order ([appends are serialized](#appends-are-serialized-per-app)), and
one read per connection issued against the cursor it was handed.

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
  `503`/`Retry-After`. Per-connection protection is complete ([producer memory is one batch](#producer-memory-is-one-batch-bounded-in-rows-and-bytes),
  [reads are gated on sends](#reads-are-gated-on-sends),
  [every blocking step has a deadline](#every-blocking-step-has-a-deadline)); aggregate pressure from many individually-healthy connections is not
  addressed, and neither is a consumer that is pathologically slow while still
  technically progressing. See
  [Global Resource Limits](ROADMAP.md#global-resource-limits).
- **No per-endpoint pacing, and no two log modules on one table** — a log's
  settings are the log's, one set per table, with no mount-level override and
  nothing negotiable ([pacing has one owner](#pacing-has-one-owner)). An in-VM consumer subscribes through
  `DMEx.Transport.Local` and never reaches `DMEx.Log.Plug`, so every HTTP consumer
  is a remote application: one advertised heartbeat, one stall budget and one
  batch size is the right number rather than a compromise. Two log modules
  resolving to one `{repo, prefix}` is currently representable and buys only that
  override; making it unrepresentable is
  [One Log, One Table](ROADMAP.md#one-log-one-table).
- **No compression** — a stream is an ordinary chunked HTTP response, so
  compressing it is the web server's to negotiate and configure. DMEx neither
  asks for it on the consumer side nor does it on the producer side.
- **No proxying — mirror instead, and own your ids.** `DMEx.Log.Plug` serves a
  log module, never a `bindings:` name, so a mount can never re-serve someone else's
  stream. A proxy has no storage, and everything else follows from that: it
  would forward the *origin's* id space while being unable to answer a
  `Last-Event-ID` behind its live tail, which is the silent gap described under
  [Delivery semantics](#delivery-semantics); and it would expand a consumer's
  filter against its own compiled message modules, a *subset* rather than the
  superset [selection](#message-type-selection) requires, which is a false
  negative in the one direction that must never produce one. Both failures are
  undetectable at the consumer. Pacing would gain a second owner, against
  [pacing has one owner](#pacing-has-one-owner), and the stall watcher's budgets
  ([every blocking step has a deadline](#every-blocking-step-has-a-deadline)) assume a local read
  loop rather than an unbounded upstream wait.

  What the proxy is wanted for has an answer that needs no new machinery: bind
  both — `bindings: [upstream: "https://origin/stream", mine: MyApp.Log]` — run a
  handler that appends what it receives into your own log, and mount that. The
  mirror gets its own id space, so a resume position means something it can
  honour; its own storage and retention; its own message modules to resolve
  selections against; its own pacing. Its costs — duplicated storage, a second
  cursor, and ordering guaranteed only within the mirror — are visible rather
  than silent. A pure network hop is an ordinary HTTP reverse proxy's job, which
  [deployment](guides/deployment.md) already assumes is there.

## What has been measured

`bench/` answers design claims rather than catching regressions. Each file
states the claim it exists to answer and carries what it last measured, next to
the code that would refute it — so the numbers are not restated here, where
nothing regenerates them.

- **The append lock is nearly free uncontended and expensive under contention,
  and locked throughput falls as concurrency rises** —
  `bench/e2e/append_throughput.exs`. Append concurrency buys an application
  nothing; batch work into fewer, larger messages instead.
- **Per-connection pacing holds, an idle log is flat in C, and pool checkout
  queueing is where fanout binds** — `bench/e2e/fanout_reads.exs`. It binds well
  before 200 connections on a 10-connection pool, which is why
  `max_batch_frequency` is the first setting to lower when many streams share
  one, and the argument for partitioning the pool so stream reads cannot starve
  append writes.
- **The consumer is commit-bound on the rows it *handles*, not on the rows it
  skips**, and coalescing is what buys the skip path its speed —
  `bench/e2e/catchup_drain.exs`. A log of entirely handled messages replays two
  orders of magnitude slower than a mostly-skipped one, and that is the figure
  "arbitrarily replayable" should be quoted with.
- **An idle stream is woken immediately, and a busy one waits half a batch
  interval** — `bench/e2e/end_to_end_latency.exs`. It is also the only place two
  otherwise silent defects show: a latency near the poll interval means the
  notification path is dead and the poller is quietly covering for it, and one
  near a whole batch interval means the throttle is timed from the wrong instant.
- **Framing a row is flat in payload bytes** — `bench/micro/frame_encode.exs`.
  The payload is copied once, when the socket flattens the iodata, so the id and
  the timestamp are the log's whole per-row cost.
- **A consumer's decode ceiling is orders of magnitude above what its per-row
  commit allows** — `bench/micro/frame_decode.exs`, which also shows the SSE
  framing scan, not the payload's JSON, to be the larger half of it.
- **Splicing stored JSON text beats decoding and re-encoding it by enough to pay
  for `data::text` in the storage contract** — `bench/micro/row_splice.exs`. The
  gap is paid once per connection, which is what makes it worth a contract.
- **Precomputed field-name strings are not worth generating** —
  `bench/micro/message_cast.exs`, against `DMEx.Message.cast_default/2`'s
  `Atom.to_string/1` per field per row.
- **The decoder's cost is flat in chunk size** — `bench/micro/sse_decode.exs`,
  where the naive `buffer <> chunk` it exists to avoid is not.
- **The selection filter behaves as the design assumes** — `mix bench.fingerprint`
  sweeps 86 set sizes over 1–2000 and checks no false negatives, a
  false-positive rate near 2⁻¹⁶, and the bytes per type the sizing argument
  rests on. It also puts a number on the request-line ceiling.
- **The `(type, id)` index is taken exactly when it pays** — a one-off `EXPLAIN`
  session rather than a `bench/` artifact, recorded where the index is created in
  `DMEx.Storage.Postgres.Migration`.

Every figure is developer-machine and small-sample. The shapes are reproduced;
the constants are not portable.

## Open questions

- **Auth / access control** — who may read a producer's endpoint, and how is
  that enforced? Nothing here answers this.
- **Sharing message modules across independent applications.** `DMEx.Message`
  assumes the consumer has the *same* Elixir module the producer appended from.
  That is a given inside one umbrella and an open problem between separate teams,
  repos and languages. The options — a shared package, a self-describing schema
  endpoint, a loose map-plus-type-string decode, an external registry — are not
  mutually exclusive. One constraint: dropping `origin` rests
  on `type` being unique across producers, which a declared `type:` makes
  a producer's own choice rather than a property of the module name. Nothing
  outside a single node can detect two producers declaring one type, so any
  option here has to say how a consumer knows which application a message came
  from.
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
  something a producer does not control is `max_batch_rows`: a write does not
  complete until the consumer drains it, so
  `max_batch_rows / slowest-consumer-rows-per-second` has to stay well under
  `stall_timeout`. See [the deployment guide](guides/deployment.md).
- **Where the read-amplification ceiling actually is**, and whether the pool
  wants partitioning so stream reads cannot starve append writes. Amplification is per-type rather than
  per-connection, and where it binds has not been measured.
- **Whether a slow consumer needs anything beyond the receive window.** With no
  consumer-side buffer, a consumer too slow for its producer's pacing stops
  reading, is reclaimed, reconnects, and does it again. That is correct and costs
  a re-read each time, but it is a loop, and the only lever is the producer's
  `max_batch_rows` — which one slow consumer cannot ask to be lowered for it
  alone. Is a steady `[:dmex, :stream, :reclaim]` rate an adequate operator
  signal, or does the protocol need a way to say "less at a time"?
- **Third-party client backpressure** — [the consumer has no buffer](#the-consumer-has-no-buffer)
  is a property of *DMEx's*
  client. Any SSE-capable client can consume a stream, and one that bounds
  neither its socket nor its queue is indistinguishable to the producer from a
  genuinely fast one. Documented caveat, or something detectable?
- **`:skip` visibility** — `:skip` drops a message with no DMEx-side record
  beyond telemetry, unlike `:dead_message`. Intentional as a "bring your own
  observability" mode, or should it always leave a trace?
