# DMEx — Roadmap

> Everything here is deliberately **not built**. It is recorded so decisions
> already made stay compatible with it. The code is what DMEx does and
> [ARCHITECTURE.md](ARCHITECTURE.md) is the map of it.

Three things are planned:

- **[One Log, One Table](#one-log-one-table)** — a log declares a `name:`, that
  name is its schema and its application-internal name, and two log modules can
  no longer name one table.
- **[Global Resource Limits](#global-resource-limits)** — an aggregate
  connection budget and admission control, seated on the watcher that already
  runs per producer.
- **[One Cursor, One Reader](#one-cursor-one-reader)** — two nodes of one
  consumer application run every handler twice, silently. The only correctness
  gap recorded here.

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
  An atom or a string, normalized to a string: `DMEx.Handler.identifier!/3`
  already does exactly this and is already what a `bindings:` key and a handler's
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
- **The name is also the application-internal name.** `bindings:` on
  `DMEx.Supervisor` narrows to name → URL only. Local logs are
  discovered the way handlers already are — a log module answers `__dmex_log__/0`,
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
  one `__dmex_log__/0` already carries. Passing `log:` with `tables:` or `prefix:`
  raises rather than being reconciled, which keeps the schema derived in exactly
  one place and a migration unable to drift from the log it migrates.

**What stays true regardless.** None of it is observable to a consumer, the lock
and channel still key on the schema
([appends are serialized per app](ARCHITECTURE.md#appends-are-serialized-per-app)),
and N nodes running one log module against one table stays legal — which is why
the duplicate check is node-local. Binding one local log to two names is lost,
and buys only a second subscription-id namespace that distinct handler `name:`s
already give.

### Open, if and when it lands

- Whether discovery needs a per-boot exclusion: a dependency's log module starts
  a dispatcher, a `LISTEN` connection and a watcher in every application that
  depends on it, and one codebase built into several releases has only
  `bindings:` to say which release binds the log.
- The log modules in `test/support/log.ex` would all need naming, and with local
  logs out of `bindings:` there is no way to say "start only this one".
- Whether `DMEx.Test.CoTenantLog`'s append-ordering property survives as a test,
  once a second log module on one table is unbuildable.
- Each existing log needs its own schema and a `CREATE SCHEMA` plus `:messages`
  migration.
- Whether the boot check needs a runtime backstop: a log placed in a host's tree
  via `DMEx.Log.child_spec/1` never passes through `__startable__!/2`.

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

**What stays true regardless.**
[Dropping a connection is not a delivery event](ARCHITECTURE.md#dropping-a-connection-is-not-a-delivery-event),
which makes shedding a capacity decision rather than a correctness one — so this
can be added later without revisiting delivery semantics.

### Open, if and when it lands

- Whether admission is per producer or per host — one application may mount
  several producers, so the watcher is the wrong denominator.
- Whether shedding is worth having next to admission, given that refusing a new
  connection never surprises an established consumer and shedding one does.
- How an operator sees the ceiling coming, since a budget only observable by
  being hit is a bad budget.

## One Cursor, One Reader

A cursor has exactly one reader by assumption and nothing enforces it. Two nodes
of one consumer application both start every discovered handler, both read
`last_seen` **once at connect** (`DMEx.Handler.Pipeline.new/1`; `DMEx.Handler.Cursors`
states the once-at-connect rule), and both suppress against that in-memory
`:cursor`. `c:DMEx.Storage.advance_cursor/3` sets `last_seen` absolutely, so the
two writers never conflict: they write the same values in the same order and
every handler runs twice. Nothing raises, nothing is logged, and no telemetry
distinguishes it from one node consuming normally. Scaling a consumer
application from one replica to two is enough to cause it.

**Competing consumers on one cursor is not the answer, and is a non-goal.**
Serializing per row — a locked read-back before each handler call — is the only
way to make two readers on one cursor safe, and it buys nothing: both nodes still
stream the whole log, so producer reads double while throughput stays one node's,
and the loser's work is discarded. Worse, it puts a contended lock acquisition on
the socket path, where *the consumer has no buffer* has the handler running
inline with the receive window closed, so a slow owner walks its standby into the
producer's stall deadline (*every blocking step has a deadline*) and gets it
reclaimed. One cursor is one
serialization point; the answer to consumer throughput is more cursors —
today, more handlers, each with its own cursor and socket — or the outbox
that *Delivery semantics* already recommends, where one reader enqueues and
work distribution is a job queue's problem.

**The property to enforce is "at most one *committer* per cursor", not "one
process".** One process globally is not achievable without consensus and is not
what is needed. A committer is bounded by the database every commit already
needs, which is why the enforcement belongs there and not in a process registry:
a node that cannot reach its repo cannot advance a cursor, so making it also
unable to hold the claim costs nothing and needs no cluster. `:global`, Horde and
the leader-election libraries all fail here twice over — they require the
distributed Erlang DMEx exists to not need, their failure domain is
*anti-correlated* with the work (a node that holds the name and has lost its
database can neither work nor be replaced), and on a partition they yield one
committer per side, which is the duplicate execution being prevented, resolved
only after the heal.

### What would land

- **The cursor row records its owner, and that is the whole of the first step.**
  An `owner` column — `node()` plus a per-boot random id, since a node name
  repeats across a rolling deploy — written on the advance that already writes
  the row, so the write is free. Read back at connect and in
  `DMEx.Handler.Pipeline.flush/1`, which already runs on the
  `checkpoint_interval` timer: one read per interval rather than per row. An
  owner that is not ours means a second reader exists, and the bug becomes loud
  within a bounded time. First to build, because it is cheap, needs no new
  connection, and is what says whether anyone deploys two replicas at all.
- **Exclusive ownership, opt-in, on a session-scoped advisory lock.**
  `pg_try_advisory_lock` keyed on the subscription id — the same `"log/id"` string
  that keys the cursor, so a multi-log handler claims one per log, as it owns one
  cursor per log. `try` rather than the blocking form: a standby is a supervised
  process on a retry timer that can shut down cleanly, not a connection blocked
  in the kernel. Failover comes with it, unasked: the lock is released by the
  connection dying, so there is no lease, no TTL and no clock anywhere in it.
- **A reserved `classid`, distinct from the log's append lock.** This is a
  hazard rather than hygiene. An application that both appends and consumes uses
  one repo, and an ownership lock colliding with `DMEx.Storage.Postgres`'s
  `lock: {classid, objid}` would block every append in that application for as
  long as a consumer holds it. The `objid` is a hash of the subscription id, and
  a collision between two subscriptions is a stall rather than a wrong answer —
  the cursor table knows every subscription name, so it is checkable at boot and
  should raise there.
- **A dedicated connection, outside the repo's pool.** A session-scoped lock
  survives check-in and would leak to whichever caller received that connection
  next. `DMEx.Notifier.Postgres` is the precedent for holding one and the seat for
  how; EventStore spends a connection per subscription on exactly this and
  monitors it so the process dies with the connection. The cost is one
  mostly-idle connection per subscription per node, and it is the reason this is
  opt-in.
- **On SQLite the question does not arise.** A SQLite consumer's database is
  local to its node, so two nodes are two consumers with two cursors by
  construction. The callback grants unconditionally and the reasoning is
  documented rather than implemented.

**What stays true regardless.** The wire contract is untouched: ownership is
consumer-side, and a producer cannot tell how many nodes a consumer runs. The
residual window — an owner whose connection dies inside `handle_message/2` loses
the lock but **cannot commit**: its advance needs the dead connection, and
pipeline section 4 lets that failure through untagged, killing the subscription
with the cursor still below the message — is bit-for-bit the case
[Delivery semantics](ARCHITECTURE.md#delivery-semantics) already documents, so
nothing here adds a failure mode. N nodes running one *log* module is argued
separately under [One Log, One Table](#one-log-one-table).

### Open, if and when it lands

- Whether detection (owner stamping) is enough on its own, and whether a foreign
  owner warns or crashes the subscription.
- Whether exclusive ownership stays opt-in on Postgres.
- What paces a standby's retry, and how a deliberately idle standby stays
  distinguishable from a broken one.
- Whether `DMEx.Handler.DeadMessages.replay!/2` and
  `DMEx.Handler.Cursors.move!/3` take the lock too.
- Whether ownership keys on the handler instead of the subscription.
- What `DMEx.Transport.Local` means for it.
