# DMEx — Roadmap

> Everything here is deliberately **not built**. It is recorded so decisions
> already made stay compatible with it. The code is what DMEx does and
> [ARCHITECTURE.md](ARCHITECTURE.md) is the map of it.

One thing is planned:

- **[Global Resource Limits](#global-resource-limits)** — an aggregate
  connection budget and admission control, seated on the watcher that already
  runs per producer.

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
