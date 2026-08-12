# DMEx — Roadmap

> Everything here is deliberately **not built**. It is recorded so decisions
> already made stay compatible with it. The code is what DMEx does and
> [ARCHITECTURE.md](ARCHITECTURE.md) is the map of it.

Two things are planned:

- **[One Log, One Table](#one-log-one-table)** — a log declares a `name:`, that
  name is its schema and its application-internal name, and two log modules can
  no longer name one table.
- **[Global Resource Limits](#global-resource-limits)** — an aggregate
  connection budget and admission control, seated on the watcher that already
  runs per producer.

## One Log, One Table

A log module is a handle on a `dmex_messages` table, and nothing stops two of
them naming the same one. A table is identified by `{repo, prefix}`, both
default, so two `use DMEx.Log` modules in one application with neither set are
one log wearing two names. DMEx already forbids the obvious spelling of this —
starting one log module twice fails on its registered names, which is what
`DMEx.Log.__start__/1`'s deadline is for — and misses this one entirely.

Nothing worth having is on the other side of it:

- **The notifier does not follow the table.** `DMEx.Notifier.Beam` scopes on
  `Module.concat(log, "Notifications")`, per module. Two logs sharing a table
  under it never wake each other, and every cross-log append degrades silently to
  the poll interval — the one place `DMEx.Log`'s "sharing follows the table and
  cannot be got wrong" is not true.
- **Everything else is duplicated waste**: a second dispatcher polling one table,
  a second `LISTEN` connection, a second stream registry, and each dispatcher
  waking for the other's appends to read rows it does not route.
- **The one thing it can express is a non-goal.** Two logs on one table differ
  only in pacing over the same rows. An in-VM consumer subscribes through
  `DMEx.Transport.Local` and never reaches the mount, so every HTTP consumer is a
  remote application: one heartbeat, one stall budget and one batch size is the
  right number, not a compromise.

### What would land

- **`name:` becomes a required log option and the log's identity.**
  `use DMEx.Log, repo: MyApp.Repo, name: "orders"`. Required, with no default —
  the default `public` is precisely what lets two logs collide by saying nothing.
  An atom or a string, normalized to a string: `DMEx.validate_identifier!/3`
  already does exactly this and is already what a `logs:` key and a handler's
  `name:` pass through, so the rule stays written once. Its `[a-z][a-z0-9_]*` is
  a strict subset of `DMEx.Storage.Postgres`'s `^[A-Za-z_][A-Za-z0-9_$]*$`, so
  one validation covers both uses and no intersection has to be spelled out.
- **The name is the schema.** The tables live in `dmex_orders`, and `:prefix` is
  superseded and removed. `scope` in `DMEx.Log.__config__/3` *is* the prefix
  today, so it becomes `"dmex_" <> name` and the schema is that string. The
  channel is then the scope **verbatim** rather than `"dmex_" <> scope`: the
  channel and the schema are one name rather than `dmex_dmex_orders`, the lock
  still `phash2`es it, and one bound covers both — `@scope_max_bytes` is 58, so a
  name fits 53 bytes. Table names stay fixed, so `dmex_messages` stays a constant
  and hand-written operator queries survive.
- **The name is also the application-internal name.** `logs:` on
  `DMEx.Supervisor` becomes `remote_logs:`, name → URL only. Local logs are
  discovered the way handlers already are — a log module answers `__dmex__/0`,
  and `DMEx.Handler.discover/1` is the pattern — so adding a log module is the
  whole of adding a log. `transport!/3` stops sniffing `is_atom(target)`: the
  transport follows from which source a name resolved out of.
- **Two logs declaring one name is a boot error**, as is a name declared both
  locally and remotely. `DMEx.Supervisor.__startable__!/2` is the seat: already a
  pure function of the declarations, already where the cross-handler checks live.
- **On SQLite the file is the unit.** `prefix:` raises there already, and the
  error says a database file "is what a schema would have bought". The name stays
  the internal name, placement ignores it, and the boot check groups on the repo.
- **Consumer tables are unaffected.** `dmex_cursors` and `dmex_dead_messages` are
  consumer-side, keyed by subscription id across every log, and keep taking the
  supervisor's storage prefix. `DMEx.Storage.Migration` already splits this way,
  and the producer half takes the log rather than a retyped schema:
  `up(log: MyApp.Log)` for the producer, `up(tables: [:cursors, :dead_messages])`
  for the consumer. `log:` implies both halves of what it replaces — a log module
  is the producer side, so the tables are `[:messages]` and the schema is the
  one `__dmex__/0` already carries. Passing `log:` with `tables:` or `prefix:`
  raises rather than being reconciled, which keeps the schema derived in exactly
  one place and a migration unable to drift from the log it migrates.

**What stays true regardless.** The wire contract is untouched — none of this is
observable to a consumer. The lock and channel still key on the schema and
nothing else — the channel loses its `"dmex_"` because the schema now carries
one, which is a spelling and not a rekeying; what changes is the derivation's
justification, because once the invariant holds, keying on the table and
keying on the log module are the same key and `DMEx.Log`'s "two logs on one table
would take different locks" describes a shape that can no longer be built. N
nodes running one log module against one table stays legal, which is why the
duplicate check is node-local. Binding one local log to two names
(`logs: [orders: MyApp.Log, mirror: MyApp.Log]`) is lost, and buys only a second
subscription-id namespace that distinct handler `name:`s already give.

### Open, if and when it lands

- **Discovery needs an exclusion, and it is per boot rather than per module.**
  Sixteen `use DMEx.Log` modules live in `test/`, several deliberately
  misconfigured for compile-error tests, and with `logs:` gone there is no way to
  say "start only this one". The test suite is the loudest case but not the
  shape-defining one: a **log module arriving through a dependency** starts a
  dispatcher, a `LISTEN` connection and a watcher in every application that
  depends on it, and `DMEx.discovery/1` scanning every application declaring
  `:dmex` is exactly what makes a shared package work; and **one codebase built
  into several releases** has only `logs:` today to say that the web release
  binds the log and the worker release does not. Both are the same compiled
  module in different deployments, so a compile-time `start: false` cannot
  express either and a runtime allowlist on `DMEx.Supervisor` can express all
  three. Neither is a correctness problem — N nodes on one log module stays legal
  — and handlers carry the same exposure today. Undecided.
- **The test suite is where the bill lands.** Those sixteen logs share one repo
  with no prefix — the suite is the largest existing consumer of the shape being
  forbidden, and gets away with it because nothing appends across them
  concurrently. Each real one needs its own schema and a `CREATE SCHEMA` plus
  `:messages` migration.
- **One existing test is the forbidden shape on purpose.**
  `DMEx.Test.CoTenantLog` is a second log module on `DMEx.Test.IntegrationLog`'s
  table, and `DMEx.Integration.AppendOrderingTest` exists to watch the two
  exclude each other — which is the thing keying the lock on the table rather
  than the module buys, and the thing that becomes unbuildable. Whether the
  property survives as a test at all is open: N nodes on one log module is the
  only legal spelling left, and a node-local registered name means one node
  cannot stand in for two.
- **Whether the boot check needs a runtime backstop.** A log placed directly in a
  host's tree via `DMEx.Log.child_spec/1` never passes through
  `__startable__!/2`. A node-local name claim on the schema, checked with
  `whereis` before start, would cover it and fail immediately with both module
  names instead of spending `__start__/1`'s deadline on a duplicate that never
  dies.

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
