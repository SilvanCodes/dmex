# DMEx — Roadmap

> Everything here is deliberately **not built**. It is recorded so decisions
> already made stay compatible with it. The code is what DMEx does and
> [ARCHITECTURE.md](ARCHITECTURE.md) is the map of it.

Two things are planned:

- **[Global Resource Limits](#global-resource-limits)** — an aggregate
  connection budget and admission control, seated on the watcher that already
  runs per producer.
- **[Compression](#compression)** — `Content-Encoding: gzip`, producer-side
  opt-in with explicitly-sized zlib parameters.

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
