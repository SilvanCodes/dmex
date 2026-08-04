# DMEx — Roadmap

> Everything here is deliberately **not built**. It is recorded so decisions
> already made stay compatible with it. The code is what DMEx does and
> [ARCHITECTURE.md](ARCHITECTURE.md) is the map of it.

Three things are planned:

- **[Stream Stats](#stream-stats-get-streamstats)** — `GET /stream/stats`, a
  plain JSON description of the message types a log contains and the id range
  each spans.
- **[Global Resource Limits](#global-resource-limits)** — an aggregate
  connection budget and admission control, seated on the watcher that already
  runs per producer.
- **[Compression](#compression)** — `Content-Encoding: gzip`, producer-side
  opt-in with explicitly-sized zlib parameters.

## Stream Stats (`GET /stream/stats`)

A second, non-streaming endpoint under the same mount: **the message types the
log actually contains, and for each one the lowest and highest id it appears
at.** Plain JSON, one request, no subscription, no consumer state.

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

Top-level `max_id` is the log's own latest id. Nothing on the stream carries it
— the wire has no tip — so this is the only place it is available.
`message_types` is sorted, so two fetches of an unchanged log compare cleanly.

**Why it earns a place.** Four questions have no answer short of reading the
whole log, and one endpoint answers all of them:

- **Where to rewind to after adding a message type.** This is the concrete
  payoff, and it repairs the sharpest edge in
  [selection](ARCHITECTURE.md#message-type-selection): adding a module selects
  that type from the cursor forward, so a consumer wanting the history has to
  move its cursor back — and without stats its only safe choice is the beginning
  of the log, replaying every already-processed message of every other type on
  the way. `min_id` for the new type is exactly how far back it needs to go.
- **Whether the producer appends the type at all.** A filter naming a type the
  producer never emits matches nothing, silently — correct behaviour and an
  awful debugging experience. Stats makes it visible out of band, without
  weakening the no-validation rule on the stream itself.
- **What the type vocabulary is, for a non-Elixir consumer.** A consumer in
  another language has to know the fully-qualified module-name strings to build
  a filter at all. Stats is the smallest thing that makes a producer
  self-describing, and stops well short of a schema endpoint.
- **Whether `stream_from: :current` is about to skip something.** How much
  history exists per type, before choosing between `1`, `:current`, an explicit
  id, and an instant.

**What it is not.** Three limits, and the first is load-bearing:

- **Not a source for filter expansion.** Stats reports what the *log contains*;
  expansion must resolve against what the producer's *code knows*, and the two
  differ exactly when a deployed type has not been appended yet. Using stats as
  the expansion source reintroduces the staleness failure `DMEx.Log.Messages`
  exists to avoid. The endpoint is for humans and consumer-side tooling, never
  for the producer's own read path.
- **Not a schema.** No field shapes, no versioning of payloads. Those belong to
  the separate schema-endpoint question, which stats does not pre-empt.
- **Not live.** A point-in-time snapshot, computed per request — an ordinary
  JSON `200` with a body and an end, entirely outside the
  frame/heartbeat/backpressure machinery.

**Cost.** The query is `SELECT message_type, min(id), max(id) FROM dmex_messages
GROUP BY message_type`. The index it wants already exists: `(message_type, id)`,
where min and max of `id` within each type are the two ends of a contiguous
range. That turns a sequential scan into a per-distinct-type index probe — but
only if the planner takes it, which Postgres does well with a skip/loose scan
and poorly without one, and is worth `EXPLAIN`ing on a real log. A short
producer-side TTL is almost free, since the answer moves only when an append
introduces a new type or extends a range and callers are humans and deploy-time
tooling.

The `stats` name is reserved in the `/stream` namespace and answers `404` until
it is built.

### Open, if and when it lands

- **Whether it is on by default.** The endpoint exposes the type vocabulary and,
  through id ranges, a rough shape of traffic — strictly less than the stream
  itself discloses, so on an open mount it leaks nothing new; behind auth it is
  one more thing to authorize. Belongs with the wider
  [auth question](ARCHITECTURE.md#open-questions); the likely answer is an
  `DMEx.Log.Plug` option defaulting to on.
- **Counts.** `count(*)` per type is the obvious next column and the expensive
  one: min and max are index endpoints, a count is every row. Maybe behind an
  opt-in, maybe an estimate from planner statistics, maybe not at all.
- **Timestamps as well as ids.** `min/max(inserted_at)` would let an operator
  reason in wall-clock terms, which is what a human wants when choosing a rewind
  point. The counter-argument is that `inserted_at` is explicitly
  observability-only and never an ordering key, and putting it in an endpoint
  whose output feeds cursor decisions invites exactly the confusion that rule
  prevents. If it lands, ids must stay the operative values.
- **Whether stats accepts a filter.** `/stream/<blob>/stats` is tidy and
  symmetric, and immediately raises whether false positives should show up in
  the answer. Reporting the unfiltered truth and letting the caller intersect is
  simpler and probably right.
- **Empty-log shape.** `{"max_id": null, "message_types": []}` versus omitting
  `max_id` is a small decision clients will nonetheless depend on, so it wants
  pinning in the spec.
- **Cache semantics on the wire.** `Cache-Control: max-age=<ttl>`, an `ETag`, or
  nothing and the caching stays private? An `ETag` is cheap and makes polling by
  tooling nearly free.

## Global Resource Limits

DMEx defends a producer **per connection** and not at all in aggregate. The send
loop makes unbounded memory growth unrepresentable, and `DMEx.Log.Watcher`
reclaims any connection that stops making progress. What neither touches is many
individually-healthy connections adding up to more than the host application can
afford — and the corollary, that a producer under that pressure cannot say no.

This is a roadmap entry rather than a new design because the seat already
exists. The watcher runs one process per producer, sweeps the live set on an
interval, and holds every connection's deadline. A budget is a counter and a
policy on a process that is already there; `DMEx.Log.Plug` is already the single
place a connect attempt passes through before registering.

### What would land

- **A connection budget.** `max_streams` per producer, checked at connect. The
  plug asks the watcher for admission before registering; over budget, a `503`
  with a `Retry-After`. First to build, because a count is the one aggregate that
  is exact, free to maintain, and immediately actionable.
- **`Retry-After` must be jittered per response**, for the same reason reconnect
  backoff is. A single advertised delay applied literally by every rejected
  consumer converts a load spike into a synchronized retry storm, which is
  precisely what admission control is supposed to prevent.
- **An aggregate budget beyond count.** Bytes in flight is the honest quantity —
  reads are capped in bytes per connection, so `open × max_batch_bytes` is
  already a bound, and a budget denominated the same way composes with it.
  Summing process memory per sweep is the tempting alternative and probably too
  coarse and too expensive.
- **A shedding policy.** Once there is a budget there has to be an answer to
  "which connection goes", and the watcher can rank by deadline because it holds
  every one. Oldest-first and slowest-first are the other candidates.

**What stays true regardless.** Dropping a connection is not a delivery event
(ARCHITECTURE invariant 7), which makes shedding a capacity decision rather than
a correctness one — so this can be added later without revisiting delivery
semantics.

### Open, if and when it lands

- **Whether admission is per producer or per host.** The budget that matters is
  the application's, and one application may mount several producers. A
  per-producer cap fits where the watcher sits and is the wrong denominator for
  what is being protected.
- **What a rejected consumer does with a `503`.** DMEx's client should back off
  and retry, which it already does for any failed connect — but a producer at
  capacity and a producer that is down are different situations, and whether the
  client distinguishes them is worth deciding.
- **Whether shedding is worth having at all next to admission.** Refusing new
  connections is simple, predictable, and never surprises an established
  consumer. Shedding an existing one is strictly harsher, against a client that
  did nothing wrong.
- **How an operator sees the ceiling coming.** A budget only observable by being
  hit is a bad budget. `[:dmex, :stream, :connect]` plus a live registry count is
  most of a utilization signal already.
- **What triggers building this at all** — a connection-count threshold, an
  observed incident, or memory pressure the per-connection bounds demonstrably
  failed to contain? Worth deciding the tripwire in advance.

## Compression

Negotiated the ordinary HTTP way — the consumer sends `Accept-Encoding: gzip`,
the producer answers with `Content-Encoding: gzip` and compresses the chunk
stream. A browser `EventSource` gets it for free, which is notable given how
little else it can negotiate.

It pays unusually well here, and **write batching is why**: one `chunk/2` call
carries up to `max_batch_size` frames with identical key names, a small set of
repeating `message_type` strings and near-identical timestamps, so the redundancy
is *within* one write rather than depending on cross-write dictionary state.
This is also the one place the unbatched wire format costs something: the per-row
`id:`/`event:`/`data:` scaffolding repeats where a batch envelope stated it once
— exactly the kind of repetition deflate is best at, so the *compressed* size
should be close to a batched frame's, but that is to measure rather than assume.

Three things have to be got right, and the third decides the default:

- **Flush per write, not per frame.** Left to itself, deflate buffers until it
  has a block worth emitting, holding a write on the wire until the *next* one
  pushes it out — unbounded added latency on a quiet stream, and a heartbeat
  arriving only once it is no longer needed. Every `chunk/2` must end in a sync
  flush. If the web server compresses automatically, that has to be verified
  rather than assumed; if it does not flush per chunk, DMEx compresses itself.
- **Flushing costs ratio.** A sync flush ends the block and emits a few bytes of
  overhead, and a small write compresses worse than a large one — so a producer
  paced to small batches for latency also gets worse compression. A real tension
  between two of the producer's own settings, visible in one place rather than
  split across a negotiation.
- **Per-connection compressor state is the catch.** A zlib deflate stream at
  defaults (`windowBits` 15, `memLevel` 8) holds on the order of 256 KB for the
  connection's whole life; at a thousand connections that is ~256 MB, dwarfing
  "memory per connection is one batch" and quietly reintroducing the
  unbounded-growth class of failure that property exists to exclude.

So compression would be **producer-side opt-in with explicitly-sized zlib
parameters** — `windowBits` 12 / `memLevel` 4, about 32 KB per connection — and
`max_batch_bytes` measured *before* compression, which is simpler and bounds the
producer's own buffers. The consumer carries an inflate stream too, but one per
subscription rather than one per connection served.

Worth recording rather than resolving: compressing a response that mixes
attacker-influenced content with content the attacker should not learn leaks
through length (the BREACH/CRIME family). The setting here is unlike the classic
one — no per-request reflection, no secret to extract token by token — and it
requires an adversary who can both append into a producer's log and observe the
sizes of someone else's stream. That is a question about who may append and who
may read, and it belongs with [auth](ARCHITECTURE.md#open-questions).
