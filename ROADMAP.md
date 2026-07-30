# DMEx — Roadmap

> Status: design direction, not shipped behaviour. Everything still in this
> document is deliberately **not built**. It is recorded so the decisions
> already made stay compatible with it and so the reasoning isn't rediscovered
> later. The code is what DMEx does and [ARCHITECTURE.md](ARCHITECTURE.md) is
> the map of it; this file is the spec of intent for what it is expected to
> grow.

Three things are planned:

- **[Stream Stats](#stream-stats-get-streamstats)** — `GET /stream/stats`, a
  plain JSON description of the message types a log actually contains and the
  id range each one spans.
- **[Global Resource Limits](#global-resource-limits)** — an aggregate
  connection budget and admission control, seated on the watcher that already
  runs per producer.
- **[Compression](#compression)** — `Content-Encoding: gzip` on the stream,
  producer-side opt-in with explicitly-sized zlib parameters.

## Message-Type Selection — **shipped**

Server-side selective delivery landed. `GET /stream/<blob>` carries a
consumer's derived message list as an XOR filter, and the producer resolves it
against its own compiled message modules and reads only matching rows. The
design as built is described in
[ARCHITECTURE.md](ARCHITECTURE.md#message-type-selection); the wire encoding is
normative in [guides/fingerprint-spec.md](guides/fingerprint-spec.md).

**What the design got right and this file previously argued for**, kept because
the reasoning is what makes the shape defensible rather than arbitrary: no
grouping vocabulary of its own; the derived message list doing both jobs so it
cannot drift; the filter rather than the names, because module names exhaust a
request line at around sixty types; the path rather than a header, because a
browser `EventSource` owns its URL and sets no headers; a path segment rather
than `?filter=`, because intermediaries rewrite query strings and never paths;
expansion over compiled modules rather than log contents, because the latter
goes stale silently and the deploy boundary is what makes the former safe.

**Open questions this closed:**

- *Whether the producer expands the filter or tests row by row.* It expands, at
  connect, which is what lets the read become an indexed
  `message_type = ANY($1)`. `(message_type, id)` is now a required index.
- *Wakeup routing.* Both signals route. `pg_notify` carries the `message_type`
  as a routing key, and the poll folds `array_agg(DISTINCT message_type)` into
  the query it was already issuing. The registry entry carries the connection's
  expanded type set and the check is one `MapSet.member?/2`, so routing removes
  work rather than adding an iteration — `Registry.dispatch/3` was already
  `O(C)`. Keying the registry on `message_type` was considered and rejected: it
  multiplies registration and cleanup by the size of a selection to answer a
  question a set membership test already answers.
- *Whether the producer echoes the resolved type list.* It echoes a **count**,
  as `DMEx-Matched-Messages`. Bounded, always on, and it catches the failure
  that matters — a filter resolving to 0 where ten types were selected.
- *The default fingerprint width, and whether consumers may choose it.* Pinned
  at 16, not selectable. A leading width byte is indistinguishable from a seed
  starting with that value unless framed as carefully as the hash construction,
  and the sweep (below) shows 16 is comfortably right.
- *Filter construction failure.* Construction moved to compile time, in
  `DMEx.Consumer.Handler`, so a filter that cannot peel within 8 derived seeds
  is a **compile error naming the handler**. There is no runtime fallback path
  to specify, and the blob is a constant rather than something rebuilt per
  connect.
- *Request-line limits.* Measured rather than estimated: **~2 400 message
  types** at an 8 KB limit, at 3.33 base64url characters per type.

**Two things this file argued for and got wrong**, corrected rather than marked
shipped:

- *"A selective stream should only ever carry decodable types, so an unknown one
  now indicates a real mismatch and is worth surfacing."* **Wrong**, and this
  document's own sizing argument is why: filter inclusion is heuristic, and a
  false positive admits one entire message type for the whole life of a
  connection. Unknown types are an expected consequence of that, not a fault.
  `DMEx.Consumer.Pipeline`'s skip path is unchanged — it still advances the
  cursor and invokes nothing — and `[:dmex, :consumer, :skip]` already carries
  `message_type`, which is what makes a false-positive type nameable without
  turning it into an error.
- *"No rejecting a filter that matches nothing."* **Reversed.** An empty
  expansion is indistinguishable on the wire from a healthy stream on a quiet
  log, so a silent empty stream is the worst of the options; it is a `404`. The
  case this document raised against that is real, though — a consumer deployed
  ahead of the producer that will emit its types is legitimately in that state —
  so the `404` is **retryable**: `DMEx.Consumer.Stream` reconnects under the
  ordinary backoff and logs a warning, rather than stopping the way it stops on
  a `400`. Producer-side validation is otherwise still absent, and nothing
  consumer-supplied is atomized.

**What stayed open:** narrowing a selection below the derived message list,
whether a sparse cursor wants a second cursor, and rolling multi-node producer
deploys. All three are recorded in
[ARCHITECTURE.md](ARCHITECTURE.md#open-questions).

**Measured.** `mix bench.fingerprint`, 86 set sizes over 1–2000: zero false
negatives in 45 660 membership tests; a false-positive rate of 14.65 ± 1.85 per
million against 2⁻¹⁶'s expected 15.26; 2.49 bytes per type raw against the 2.46
claimed below.

## Stream Stats (`GET /stream/stats`)

A second, non-streaming endpoint under the same mount: **`GET /stream/stats`
returns the message types the producer's log actually contains, and for each
one the lowest and highest id it appears at.** Plain JSON, one request, no
subscription, no consumer state.

```
GET /stream/stats
```

```json
{
  "max_id": 1180,
  "message_types": [
    {"message_type": "MyApp.Messages.OrderPlaced",  "min_id": 1,   "max_id": 1180},
    {"message_type": "MyApp.Messages.OrderShipped", "min_id": 4,   "max_id": 1177},
    {"message_type": "MyApp.Messages.OrderRefunded","min_id": 902, "max_id": 1101}
  ]
}
```

`max_id` at the top level is the log's own latest id. Nothing on the stream
carries it — the wire has no tip and a consumer measures how far behind it is
from `inserted_at` (see
[The wire contract](ARCHITECTURE.md#the-wire-contract)) — so this endpoint is
the only place it is available, which strengthens rather than weakens the case
for it: one request answers "how big is this log" as well as "what is in it".
`message_types` is sorted by `message_type`, so two fetches of an unchanged
log compare cleanly.

**Why it earns a place.** Four questions currently have no answer short of
reading the whole log, and one endpoint answers all of them:

- **Where to rewind to after adding a message type.** This is the concrete
  payoff, and it repairs the sharpest edge in
  [message-type selection](ARCHITECTURE.md#message-type-selection): adding a module selects
  that type from the cursor forward, so a consumer that wants the history has
  to move its cursor back — and without stats its only safe choice is *the
  beginning of the log*, replaying every already-processed message of every
  other type on the way. `min_id` for the newly-added type is exactly how far
  back it needs to go, and nothing further.
- **Whether the producer publishes the type at all.** Selection is
  deliberately unvalidated (see
  [message-type selection](ARCHITECTURE.md#message-type-selection)) — a filter naming a type the
  producer has never emitted matches nothing, silently, which is correct
  behaviour and an awful debugging experience. Stats is where "I am selecting a
  type this producer does not publish" becomes visible, out of band, without
  weakening the no-validation rule on the stream itself.
- **What the type vocabulary even is, for a non-Elixir consumer.** A consumer
  in another language has to know the fully-qualified module-name strings to
  build a filter at all, and today it learns them out-of-band or not at all.
  Stats is the smallest thing that makes a DMEx producer self-describing, and
  it stops well short of the full schema endpoint discussed under
  [Open questions](ARCHITECTURE.md#open-questions) — it says what types exist
  and where they live, never what fields they have.
- **Whether `start: :latest` is about to skip something.** An operator pointing
  a new consumer at a producer can see how much history exists per type before
  deciding between `:beginning`, `:latest`, and an explicit id (see
  [the invariants](ARCHITECTURE.md#the-invariants)).

**What it is not.** Three limits, and the first is load-bearing:

- **It is not a source for filter expansion.** Stats reports what the *log
  contains*; expansion must resolve against what the producer's *code knows*,
  and the two differ exactly when a deployed type has not been published yet.
  Using stats as the expansion source reintroduces the staleness failure
  `DMEx.Producer.Messages` exists to avoid — silent false negatives, the one
  failure mode the whole selection design is built to exclude. The endpoint is
  for humans and for consumer-side tooling, never for the producer's own read
  path.
- **It is not a schema.** No field shapes, no JSON Schema, no versioning of
  payloads. Those belong to the separate schema-endpoint question, which stats
  is compatible with and does not pre-empt: `GET /stream/schema` could land
  later beside it under the same namespace rule.
- **It is not live.** A point-in-time snapshot, computed per request. There is
  no streaming variant and no push; a caller that wants a current answer asks
  again. This keeps it entirely outside the frame/heartbeat/backpressure
  machinery — it is an ordinary JSON `200` with a body and an end, not a
  stream, which is also why nothing about SSE applies to it.

**Cost, and why it still needs deciding rather than assuming.** The query is
`SELECT message_type, min(id), max(id) FROM dmex_messages GROUP BY
message_type`. **The index it wants already exists**: `(message_type, id)`
landed with selection (see [The log](ARCHITECTURE.md#the-log)), and min and max
of `id` within each type are the two ends of a contiguous range in it. That
turns what would have been a sequential scan of the entire log into a
per-distinct-type index probe — but only if the planner takes it. Postgres does
this well with a skip/loose scan and poorly without one, and which it picks is
worth `EXPLAIN`ing on a real log rather than assuming.

Caching is the other half, and unchanged: the answer moves only when a publish
introduces a new type or extends a range, and callers are humans and
deploy-time tooling rather than a hot path, so a short producer-side TTL is
almost free and removes the question of a consumer hammering it.

Given that, **stats is expected to ship with selection rather than before it**,
Selection shipped first and brought the index with it, so the coupling that
argument described has already been paid — stats is now the cheaper half of a
pair rather than the one that has to justify a new index. The `stats` name is
reserved in the `/stream` namespace and answers `404` until it is built.

### Open, if and when stats lands

- **Whether it is on by default.** The endpoint publishes the producer's type
  vocabulary and, through id ranges, a rough shape of its traffic — to anyone
  who can reach the mount. That is strictly less than the stream itself
  discloses, so on a mount that is already open it leaks nothing new; on a
  mount behind auth it is one more thing to authorize. It belongs with the
  wider [auth question](ARCHITECTURE.md#open-questions), and the likely answer
  is an `init_opts` flag defaulting to on, since a producer that needs it off
  probably needs the stream off too.
- **Counts, and how far the aggregate should go.** `count(*)` per type is the
  obvious next column and the expensive one: min and max are index endpoints,
  a count is every row. It may be worth having behind an explicit opt-in, an
  estimate from planner statistics, or not at all.
- **Timestamps as well as ids.** `min(inserted_at)`/`max(inserted_at)` per type
  would let an operator reason in wall-clock terms ("when did we start
  publishing this?"), which is what a human actually wants when choosing a
  rewind point. The counter-argument is that `inserted_at` is explicitly
  observability-only and never an ordering key (see
  [The log](ARCHITECTURE.md#the-log)), and putting it in an endpoint
  whose output feeds cursor decisions invites exactly the confusion that rule
  exists to prevent. If it lands, the ids must stay the operative values and
  the timestamps be plainly labelled as descriptive.
- **Whether stats accepts a filter.** `/stream/<blob>/stats` would report only
  the selected types, which is tidy and symmetric — and immediately raises
  whether false positives should show up in the answer (they are, after all,
  what the connection would actually receive). Reporting the unfiltered truth
  and letting the caller intersect is simpler and probably right.
- **Empty-log and fresh-producer shape.** A log with no rows has no types and
  no `max_id`; `{"max_id": null, "message_types": []}` versus omitting `max_id`
  is a small decision that clients will nonetheless depend on, so it wants
  pinning in the spec rather than falling out of the implementation.
- **Cache semantics on the wire.** If the producer caches internally, does it
  say so — `Cache-Control: max-age=<ttl>`, an `ETag` over the payload, or
  nothing at all and the caching stays a private detail? An `ETag` is cheap
  here and makes polling by tooling nearly free.

## Global Resource Limits

v1 defends a producer **per connection** and not at all in aggregate. The send
loop makes unbounded memory growth unrepresentable, and `DMEx.Producer.Watcher`
reclaims any connection that stops making progress (see
[the invariants](ARCHITECTURE.md#the-invariants)). What neither
touches is many individually-healthy connections adding up to more than the
host application can afford — and the corollary, that a producer under that
pressure has no way to say no.

The reason this is a roadmap entry rather than a new design is that the seat
for it already exists. The watcher runs one process per producer, sweeps the
live set on an interval, and holds every connection's deadline. A budget is a
counter and a policy on a process that is already there; `DMEx.Producer.Plug`
is already the single place a connect attempt passes through before
registering. Nothing about v1 has to move to make room.

### What would land

- **A connection budget.** `max_streams` per producer, checked at connect. The
  plug asks the watcher for admission before registering; over budget, the
  answer is a `503` with a `Retry-After` rather than a stream. This is the
  first thing to build, because a count is the one aggregate that is exact,
  free to maintain, and immediately actionable.
- **`Retry-After` must be jittered per response**, for the same reason
  reconnect backoff is (see
  [The wire contract](ARCHITECTURE.md#the-wire-contract)). A single advertised
  delay applied literally by every rejected consumer converts a load spike into
  a synchronized retry storm, which is precisely what admission control is
  supposed to prevent.
- **An aggregate budget beyond count.** Bytes in flight is the honest quantity
  — reads are capped in bytes per connection, so `open × max_batch_bytes` is
  already a bound, and a budget denominated the same way composes with it. Summing
  process memory per sweep is the tempting alternative and probably too coarse
  and too expensive to be worth it.
- **A shedding policy.** Once there is a budget there has to be an answer to
  "which connection goes", and the watcher can rank by deadline because it
  already holds every one — the connection closest to its budget is the one
  least likely to be doing useful work. Oldest-first and slowest-first are the
  other candidates.

### What stays true regardless

**Dropping a connection is not a delivery event.** The producer holds no
consumer state, `last_seen` is durable on the consumer, and resumption is the
same `id > last_seen` read a cold connect does (see
[the invariants](ARCHITECTURE.md#the-invariants)). That is what makes
shedding a capacity decision rather than a correctness one, and it is why this
can be added later without revisiting anything about delivery semantics.

### Open, if and when it lands

- **Whether admission is per producer or per host.** The budget that actually
  matters is the application's, and one application may mount several
  producers. A per-producer cap is the natural fit for where the watcher sits
  and the wrong denominator for what is being protected.
- **What a rejected consumer does with a `503`.** DMEx's own client should back
  off and retry, which is what it already does for any failed connect — but a
  producer at capacity and a producer that is down are different situations,
  and it is worth deciding whether the client distinguishes them (a rejection
  is cheap and instant; a dead producer is neither).
- **Whether shedding is worth having at all next to admission.** Refusing new
  connections is simple, predictable, and never surprises an established
  consumer. Shedding an existing one is a strictly harsher action taken against
  a client that did nothing wrong, and it may be that a budget enforced only at
  the door is the whole of what a producer should do.
- **How an operator sees the ceiling coming.** A budget that is only observable
  by being hit is a bad budget. `[:dmex, :stream, :connect]` plus a live
  registry count is most of a utilization signal already; whether that wants
  its own periodic telemetry event from the watcher's sweep is a small decision
  worth making deliberately.

## Compression

Designed, not implemented. It is orthogonal to everything else and its cost is
paid in the same currency as the producer's memory bound, so it lands as its own
stage rather than riding along with something else.

Negotiated the ordinary HTTP way — the consumer sends `Accept-Encoding: gzip`,
the producer answers with `Content-Encoding: gzip` and compresses the chunk
stream. A browser `EventSource` gets it for free, which is notable given how
little else it can negotiate.

It pays unusually well here, and **write batching is why**: one `chunk/2` call
carries up to `max_batch_size` frames with identical key names, a small set of
repeating `message_type` strings and near-identical timestamps, so the
redundancy is *within* one write and available to the compressor immediately
rather than depending on cross-write dictionary state.

Note that this is the one place where the unbatched wire format costs something
rather than saving it: the per-row `id:`/`event:`/`data:` scaffolding repeats
where a batch envelope stated it once. It is also exactly the kind of repetition
deflate is best at, so the *compressed* size should be close to a batched
frame's — but that is an expectation to measure rather than assume.

Three things have to be got right, and the third decides the default:

- **It must flush per write, not per frame.** Left to itself, deflate buffers
  until it has a block worth emitting, which would hold a write on the wire
  until the *next* one pushed it out — unbounded added latency on a quiet
  stream, and a
  heartbeat arriving only once it is no longer needed, which is precisely the
  failure heartbeats exist to prevent. Every `chunk/2` must end in a sync flush.
  If the web server compresses automatically, that behaviour has to be verified
  rather than assumed; if it does not flush per chunk, DMEx compresses itself.
- **Flushing costs ratio, and that is the trade.** A sync flush ends the block
  and emits a few bytes of overhead, and a small write compresses worse than a
  large one — so a producer paced to small batches for latency also gets worse
  compression. That is a real tension between two of the producer's own
  settings, and it is now visible in one place rather than split across a
  negotiation.
- **Per-connection compressor state is the catch**, and it lands squarely on the
  property the producer's send loop was built to have. A zlib deflate stream at
  defaults (`windowBits` 15, `memLevel` 8) holds on the order of 256 KB for the
  connection's whole life; at a thousand connections that is ~256 MB, dwarfing
  "memory per connection is one frame" and quietly reintroducing the
  unbounded-growth class of failure that property exists to exclude.

So compression would be **producer-side opt-in with explicitly-sized zlib
parameters** — `windowBits` 12 / `memLevel` 4, about 32 KB per connection — and
`max_batch_bytes` measured *before* compression, which is simpler and bounds
the producer's own buffers. The consumer carries an inflate stream too, but one
per subscription rather than one per connection served, so it is not the same
question.

Worth recording rather than resolving: compressing a response that mixes
attacker-influenced content with content the attacker should not learn leaks
through length (the BREACH/CRIME family). The setting here is unlike the classic
one — no per-request reflection, no secret to extract token by token — and it
requires an adversary who can both publish into a producer's log and observe the
sizes of someone else's stream. That is a question about who may publish and who
may read, and it belongs with
[auth](ARCHITECTURE.md#open-questions).
