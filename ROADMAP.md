# DMEx — Roadmap

> Everything here is deliberately **not built**. It is recorded so decisions
> already made stay compatible with it. The code is what DMEx does and
> [ARCHITECTURE.md](ARCHITECTURE.md) is the map of it.

Two things are planned:

- **[Global Resource Limits](#global-resource-limits)** — an aggregate
  connection budget and admission control, seated on the watcher that already
  runs per producer.
- **[Compression](#compression)** — letting a subscription accept it. The
  producer half is already the web server's, and configured, not built.

## Global Resource Limits

DMEx defends a producer **per connection** and not at all in aggregate. The send
loop makes unbounded memory growth unrepresentable, and `DMEx.Log.Watcher`
reclaims any connection that stops making progress. What neither touches is many
individually-healthy connections adding up to more than the host application can
afford — and the corollary, that a producer under that pressure cannot say no.

This is a roadmap entry rather than a new design because the seat already
exists. The watcher runs one process per producer, sweeps the live set on an
interval, and can already read every connection's deadline out of the registry. A budget is a counter and a
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

**The producer half is already done, and DMEx did not do it.** A stream is a
chunked response, the web server negotiates and compresses those, and Bandit ends
every chunk in a sync flush — which is the one thing that had to be got right,
since a compressor left to buffer holds a write until the next one pushes it out.
Window size is its configuration too. So there is nothing to build there; there
is something to *say*, and it is said in
[the deployment guide](guides/deployment.md), which also carries the measured
numbers for everything below.

It pays unusually well, and **write batching is why**: one `chunk/2` call carries
a whole read's frames, with identical key names, a small set of repeating
`message_type` strings and adjacent ids, so the redundancy is *within* one write
rather than depending on cross-write dictionary state. A 500-row write of small
messages measures about 18x, and a small window beats a large one — there is
nothing to reach back to. The per-row `id:`/`event:`/`data:` scaffolding that an
unbatched wire format repeats is exactly what deflate is best at, so a compressed
stream lands close to where a batch envelope would have.

### What would land

Only the consumer's side of the negotiation, which is why this is small:

- **`Accept-Encoding` on connect**, opt-in per subscription. `DMEx.Handler.Request`
  sends `accept` and `last-event-id` and nothing else today.
- **Reading the response's `content-encoding`.** `DMEx.Transport.SSE` currently
  drops `{:headers, ...}` through its catch-all, so it cannot see what it was
  answered with.
- **An inflate stream per subscription**, feeding `DMEx.Transport.SSE.Decoder`.
  One per subscription rather than one per connection served, so the memory
  question that dominates the producer's side barely arises here.

**Inflation needs its own bound.** `max_frame_bytes` bounds what the *decoder*
buffers, and inflation happens before the decoder sees a byte — so a hostile or
broken producer's compression bomb is unbounded ahead of the only limit the
consumer has. `:zlib.safeInflate/2` is the bounded form, and this is a
consumer-side constraint that did not exist while this was framed as a
producer-side feature.

### Open, if and when it lands

- **Whether the browser case is worth chasing.** A browser negotiates itself
  into identity: Bandit streams deflate only — its gzip and zstd paths are for
  one-shot bodies — while an `EventSource` advertises gzip first. Naming
  `deflate` as the server's only response encoding fixes it, and that is a
  whole-server decision taken for one mount.
- **What it does to backpressure.** A stalled consumer is felt as `chunk/2`
  blocking on a full socket (ARCHITECTURE invariant 8); 18x fewer bytes means it
  blocks that much later. Producer memory is still one batch, so nothing breaks —
  but more rows sit in kernel buffers before anything pushes back, and the
  invariant's honest statement changes.
- **Whether it is worth it below a metered link.** It is bandwidth and nothing
  else: no latency win, a small loss on idle streams, where each heartbeat's
  flush costs more than the heartbeat.

Worth recording rather than resolving: compressing a response that mixes
attacker-influenced content with content the attacker should not learn leaks
through length (the BREACH/CRIME family). The setting here is unlike the classic
one — no per-request reflection, no secret to extract token by token — and it
requires an adversary who can both append into a producer's log and observe the
sizes of someone else's stream. That is a question about who may append and who
may read, and it belongs with [auth](ARCHITECTURE.md#open-questions).
