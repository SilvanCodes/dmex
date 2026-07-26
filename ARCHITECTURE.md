# DMEx — Architecture

> Status: rough draft, first pass. Captures the core mechanics agreed on so far;
> see [Open Questions](#open-questions) for what's still undecided.

## References

Prior art DMEx deliberately builds on rather than reinvents:

- [MDN: Using server-sent events — Event stream format](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events#event_stream_format) —
  the wire format `DMEx.Message` (see [Message Envelope](#message-envelope))
  must be a valid producer/consumer of.
- [oban-bg/oban](https://github.com/oban-bg/oban) — reference for robust,
  SQL-only job dispatch: `LISTEN`/`NOTIFY` for low-latency wakeup backed by
  belt-and-suspenders polling, and a Postgres/SQLite-agnostic engine
  abstraction.
- [commanded/eventstore](https://github.com/commanded/eventstore) —
  reference for an append-only, Postgres-backed event log with per-event-type
  Elixir modules and subscription/dispatch semantics.

## Overview

DMEx is a decentralized message queue: a simpler, embeddable alternative to
RabbitMQ with **no single point of failure**. There is no broker process, no
cluster to operate, and no shared infrastructure between applications. Instead,
every application that publishes messages owns and persists its own
append-only message log, and streams it directly to consumers over plain HTTP
using Server-Sent Events (SSE).

This buys three things:

- **Technological simplicity** — the only moving parts are a SQL table
  (SQLite or Postgres) and an HTTP endpoint. No broker to deploy, cluster, or
  keep highly available.
- **No single point of failure** — there is no shared message bus whose
  outage takes down every producer/consumer pair. Each stream's availability
  depends only on its own application and its own database.
- **Persistent, arbitrarily replayable streams** — messages are never
  removed from the log by the act of consumption. Any consumer can start (or
  restart) from any point in history, including from the very beginning.

## Core Concepts

### Message log

Each application owns exactly **one** append-only message log: a single,
totally-ordered stream of every message that application has ever published.
It is backed by a plain SQL table (SQLite or Postgres) with, at minimum:

- `id` — a monotonically increasing offset (the primary means of ordering
  and resumption).
- `event_type` — the type string of the event module the row was published
  from (see [Event Specifications](#event-specifications)).
- `topic` — an optional filter tag, declared on the publishing event module
  (see below).
- `data` — the JSON-encoded event payload.
- `metadata`/`headers` — free-form, message-level metadata. DMEx always
  populates one key itself: `origin`, the full module name of the producer
  module the message was published through (see
  [Publishing API](#publishing-api)).
- `inserted_at` — timestamp, for observability only (not used for ordering).

The table is append-only: rows are never updated or deleted by normal
operation. This is what makes arbitrary replay possible.

`topic` does **not** create a second stream. There is still exactly one
totally-ordered log, and `id` is drawn from a single per-app sequence
regardless of topic — a consumer subscribed to one topic simply sees a
sparse subset of that same id space, so `last_seen` remains directly
comparable no matter which topic(s) a consumer has subscribed to. Topic is
purely a selective-delivery filter applied at subscribe time (see
[Consuming API](#consuming-api) and [Process Architecture](#process-architecture)),
not a partitioning of the log itself. `topic` is never passed at the publish
call site — it's declared once on the event module (see
[Event Specifications](#event-specifications)) and resolved from there on
every `publish/2` call for that event type.

### Producer

The application that owns a message log is the **producer** for that log. It
is the only writer, and it exposes its log to the outside world over HTTP via
an SSE endpoint. There is no separate "broker" process — the producer *is*
the application, and the queue is just a table it happens to keep.

Concretely, a producer application declares its own producer module (e.g.
`use DMEx.Producer` in `MyApp.Producer`), the same pattern as event modules
(see [Event Specifications](#event-specifications)). That module is what
gets passed as the first argument to `DMEx.Publisher.publish/2`, and its
full name is what ends up in every published message's `origin` metadata
(see [Publishing API](#publishing-api)) — so "which producer published this"
is always structurally derived from real code, never a string a caller could
get wrong.

### Consumer

A **consumer** connects directly to a producer's SSE endpoint and reads
messages as they're appended. Critically, **the producer holds no
consumer state** — no consumer groups, no registered offsets, nothing. Each
consumer keeps its own read position (`last_seen` id) in its own storage;
this bookkeeping is handled by DMEx itself (see
[Consuming API](#consuming-api)), not left to hand-rolled application code.
On (re)start, a consumer sends the `last_seen` id it last durably recorded,
and the producer streams everything after that id, live, as it arrives.

This is the key decentralization move: producers and consumers talk directly,
peer-to-peer, with no third party mediating delivery or tracking state on
either side's behalf. There is deliberately no discovery service in that
path either: a producer's URL is plain **static configuration of the
consumer application** (e.g. app config/env), supplied wherever
`DMEx.Consumer.subscribe/2` is called (see
[Consuming API](#consuming-api)) — not looked up dynamically through any
registry. That would just reintroduce a shared, centrally-run piece of
infrastructure everyone depends on, exactly what DMEx is trying to avoid.

## Message Envelope

`DMEx.Message` is the **one generic container module** every message flows
through, regardless of which application or event type produced it. It is a
direct, minimal mapping onto the
[SSE event stream format](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events#event_stream_format):

| `DMEx.Message` field | SSE wire field | Meaning |
| --- | --- | --- |
| `id` | `id:` | The message's offset in the app's log; doubles as the value a consumer echoes back as `last_seen` on reconnect. |
| `event` | `event:` | A **transport-level** framing type, not the application's event type. Defaults to SSE's own default (`message`) for a normal single message; [adaptive batching](#adaptive-batching--backpressure-batch_window-max_batch_size) reserves `batch` here. This is the only field DMEx itself ever branches on when parsing a chunk. |
| `data` | `data:` | A JSON-encoded envelope, `{"event_type": "...", "payload": {...}, "metadata": {...}}` — **the application's event type lives here, nested under `data`, not in `event:`** (see [Event Specifications](#event-specifications)). `event_type` is the publishing event module's full name; `payload` is that module's own JSON-encoded struct fields. |
| `retry` | `retry:` | Optional reconnection hint, per spec; not required for DMEx's own resume logic since that's driven by `last_seen`, not `retry`. |

Deliberately, the application's event type is **not** carried in SSE's
`event:` field. `event:` is reserved for DMEx's own transport-level framing
concerns (is this a single message or a batch?) and is meant to stay a
small, fixed vocabulary DMEx controls; the open-ended, ever-growing set of
application event types belongs entirely inside `data`, where it can't
collide with or constrain how the transport layer itself evolves.

`metadata` inside `data` carries whatever the publisher passed to
`DMEx.Publisher.publish/2`'s own `metadata:` option, plus keys DMEx injects
itself: `origin` (the full producer module name, fixed at publish time — see
[Publishing API](#publishing-api)), and `max_id` (the log's current latest
id, given the subscription's `topic` — recomputed fresh on every send, not
fixed at publish time — see
[Adaptive batching & backpressure](#adaptive-batching--backpressure-batch_window-max_batch_size)).

`DMEx.Message` owns exactly two responsibilities: encoding itself to a
spec-compliant `text/event-stream` chunk, and parsing a chunk back into a
struct — `data` decoding one level (into `event_type` + `payload`) is as far
as it goes. It knows nothing about any particular event's *shape* — turning
`payload` into a concrete struct is the event module's job. This split keeps
the wire format concern (this module) fully decoupled from the domain
payload concern (event modules, next section).

## Event Specifications

Rather than publishing bare maps, each event a producer emits is defined as
**its own Elixir module** — one struct per event type, owned by the
publishing application — the same shape as
[Commanded](https://github.com/commanded/eventstore) event modules:

```elixir
defmodule MyApp.Events.OrderPlaced do
  use DMEx.Event, topic: "orders"

  @derive JSON.Encoder
  defstruct [:order_id, :customer_id, :total]
end
```

- There is no `type:` option. `event_type` is **derived automatically from
  the event module's own full, fully-qualified name** (e.g.
  `"MyApp.Events.OrderPlaced"`, via `inspect(__MODULE__)`) at compile time,
  via `use DMEx.Event`. The full name — not just the last segment — is what
  guarantees `event_type` uniqueness across every producer app in the
  network: two unrelated apps can each have their own `Events.OrderPlaced`
  submodule without their wire types colliding. Module naming is the single
  source of truth for wire type; it can't drift from what the module is
  actually called.
- `topic:` is the one thing `use DMEx.Event` still takes explicitly, giving
  the module `type/0` and `topic/0` callbacks that `DMEx.Publisher.publish/2`
  reads when appending to the log and building the `event_type` key of the
  `DMEx.Message` [data envelope](#message-envelope) — never
  `DMEx.Message.event` itself, which stays transport-level (see above).
  `topic` is a property of the *event type*, not something a caller chooses
  per publish — every `OrderPlaced` is on the `"orders"` topic, always.
- The struct fields are the event's full, versioned shape. Evolving an
  event's shape is scoped to editing that one module (see
  [Open Questions](#open-questions) on schema versioning).
- On the read side, a consumer must be able to turn `event_type` back into
  the same struct. DMEx does this via an explicit module list/registry
  supplied by the consumer (e.g. `event_modules: [MyApp.Events.OrderPlaced, ...]`)
  rather than dynamically atomizing whatever string the producer sent —
  turning arbitrary attacker-controlled strings into atoms is an
  unbounded-atom-table risk, so the mapping is always consumer-declared, not
  inferred.

## Publishing API

Sketch, in `DMEx` namespace:

```elixir
DMEx.Publisher.publish(MyApp.Producer, %MyApp.Events.OrderPlaced{...}, metadata: %{})
```

From the API's perspective, **publishing a message is just writing a row to
the database** — nothing more. `DMEx.Publisher.publish/2` serializes the
event struct with Elixir's native `JSON` module (`JSON.encode!/1`, via
`@derive JSON.Encoder` on the event struct), reads `event_type` and `topic`
off the struct's own module (`OrderPlaced.type/0` / `OrderPlaced.topic/0` —
never passed as call arguments), merges `origin: inspect(MyApp.Producer)`
into `metadata` (again, never passed by the caller — it's always the full
module name of the producer module the publish call went through), and does
a single local Ecto `insert` (`event_type`, `topic`, `data`, `metadata`) via
the configured storage adapter — `event_type` is its own indexed DB column
here (useful for querying/filtering the log directly), even though on the
wire it later travels nested inside the SSE `data:` envelope rather than as
`event:` (see [Message Envelope](#message-envelope)). There is no
client/server RPC, no broker call, and no network hop on the write path —
the insert itself, plus the
[live dispatch](#live-dispatch-sql-based-triggers) mechanism that notices it
afterwards, is the entire publish story. This is also why publishing has
single-writer, local-transaction semantics for free: it's a normal database
write against the app's own database, no different from any other write
that application makes. It returns the assigned `id` (offset) of the row.

## Consuming API

Sketch, in `DMEx` namespace:

```elixir
defmodule MyApp.OrderHandler do
  use DMEx.Consumer.Handler

  @impl true
  def handle_message(%MyApp.Events.OrderPlaced{} = event, %DMEx.Message{} = message) do
    # side effects go here; raise (or let something raise) to signal failure
    :ok
  end
end
```

```elixir
DMEx.Consumer.subscribe(producer_url,
  topic: "orders",
  event_modules: [MyApp.Events.OrderPlaced],
  handler: MyApp.OrderHandler,
  on_failure: :crash,      # :crash | :skip | :dead_message
  batch_window: 200,       # ms; widen this if the consumer can't keep up
  max_batch_size: 500
)
```

- Starts a subscription — see [Process Architecture](#process-architecture)
  for the full mechanics of the process that owns it — which opens an SSE
  connection to a remote producer's endpoint, resuming from whatever
  `last_seen` DMEx itself has durably persisted for this subscription (there
  is no `last_seen` argument: DMEx owns that bookkeeping, see below),
  passing `topic` so the producer only streams matching messages (omitting
  `topic` subscribes to everything on that app's log), and passing
  `batch_window`/`max_batch_size` to control delivery pacing/backpressure
  (see [Adaptive batching & backpressure](#adaptive-batching--backpressure-batch_window-max_batch_size)).
- De-batches a `batch`-type chunk into individual `DMEx.Message`s if
  needed, then for each: `JSON.decode!/1`s `data` into
  `%{"event_type" => ..., "payload" => ...}`, looks up `event_type` (**not**
  the SSE `event:` field — see [Message Envelope](#message-envelope))
  against the supplied `event_modules`, decodes `payload` into that module's
  struct, then invokes `handler.handle_message/2` with the decoded event and
  the raw `DMEx.Message`. An `event_type` with no matching module is
  surfaced as an error/unknown-event rather than silently guessed at.
- **`handle_message/2` succeeding (not raising) is what advances
  `last_seen`.** DMEx runs the callback and the `last_seen` update in one
  local transaction against the consumer's own storage — the same
  `DMEx.Storage` adapter behaviour used producer-side, just pointed at the
  consumer's own bookkeeping table. If the callback raises, the transaction
  rolls back and `last_seen` does not move; DMEx application code never
  manages this bookkeeping directly.
- `on_failure` controls what happens once local retries of a failing message
  are exhausted: `:crash` (default) — crash and let the SSE connection
  re-stream the same message on restart; `:skip` — give up on that message,
  advance `last_seen` anyway, and move on; `:dead_message` — write the
  failed message to the consumer's `dead_messages` table, then advance
  `last_seen` and move on. See
  [Process Architecture](#process-architecture) for the full mechanics of
  all three.

## Transport: SSE over HTTP

- A consumer opens
  `GET /streams/:app?last_seen=<id>&topic=<topic>&batch_window=<ms>&max_batch_size=<n>`
  (exact route TBD) via Phoenix. `topic` is optional (omitting it means
  "everything"); `batch_window`/`max_batch_size` are optional with
  DMEx-supplied defaults (see
  [Adaptive batching & backpressure](#adaptive-batching--backpressure-batch_window-max_batch_size)).
- The producer validates `last_seen`, then streams every message with
  `id > last_seen` (and matching `topic`, if given), in ascending order,
  followed by new matching messages live as they're published — framed as
  single-message or batched SSE events per
  [Adaptive batching & backpressure](#adaptive-batching--backpressure-batch_window-max_batch_size)
  below.
- The connection is long-lived. If it drops, the consumer reconnects with
  whatever `last_seen` it last durably recorded — reconnect/backoff strategy
  is entirely a consumer-side concern.
- Because the protocol is just HTTP + SSE, no custom binary protocol or
  client library is required — any SSE-capable HTTP client can consume a
  DMEx stream.

### Adaptive batching & backpressure (`batch_window`, `max_batch_size`)

There is no separate catch-up mode and no separate HTTP endpoint for it.
Catching up and live tailing are **the same SSE connection** — they only
differ in how much is pending to send at any given moment, and that's
handled by ordinary batching, not a mode switch. Two parameters, supplied by
the consumer on connect, drive it:

- `batch_window` — the minimum amount of time between flushes to this
  connection (e.g. `batch_window=200`, in ms). `DMEx.Producer.Stream`
  accumulates matching rows — from its own initial range-scan and from
  `DMEx.Dispatcher` fan-out (see
  [Live dispatch](#live-dispatch-sql-based-triggers)) alike, into the same
  accumulator — and flushes at most once per `batch_window`.
- `max_batch_size` — the most rows a single flush will ever contain. If the
  accumulator hits this size before `batch_window` elapses, it flushes
  immediately rather than waiting out the rest of the window.

At each flush: zero rows accumulated → nothing sent; exactly one → a normal
single-message SSE event (`event: message`, per
[Message Envelope](#message-envelope)); two or more → one `event: batch`
event whose `data:` is a JSON array of individually-encoded messages.
`DMEx.Consumer.Stream` de-batches a `batch` event back into individual
`DMEx.Message`s before doing anything else with them.

This is what replaces the earlier catch-up-before-streaming design. A
freshly-connected consumer whose `last_seen` is far behind the current tip
has, by definition, a large number of rows already pending for its very
first flush — so the first few `batch_window` ticks naturally produce one or
more `max_batch_size`-capped batch events back-to-back until the accumulator
drains. That *is* bulk catch-up, expressed as nothing more than the normal
batching path running hot, not a distinct mode, endpoint, or process. Once
the accumulator empties, flushes taper down to zero or one row each — the
connection has "caught up" without ever switching protocols. No `HEAD`
preflight or separate backlog endpoint is needed: the very first flush's
`max_id` metadata (see below) already tells the consumer how big the gap
was.

Regardless of burst size, **batching is purely a wire-framing convenience,
never a change to processing semantics**: every de-batched message still
goes through the exact same one-at-a-time
[handler/transaction/`last_seen`/`on_failure` pipeline](#process-architecture)
— its own transaction, its own `handler.handle_message/2` call, its own
`last_seen` advance, its own `on_failure` handling if it fails. A batch is
never processed or committed as a unit; a mid-batch failure only ever rolls
back that one message, and reconnecting after a crash still resumes from a
precise `last_seen`, never "redo the whole batch."

Because `batch_window`/`max_batch_size` are consumer-supplied, they also
double as the consumer's own **backpressure control**. A consumer that's
struggling to keep up — its handler falling behind, its own process
backing up, or simply noticing a growing gap via `max_id` (below) — can
reconnect with a larger `batch_window`: fewer, larger, less frequent
flushes instead of a steady drip it can't keep pace with. A small
`batch_window` does the opposite, favoring near-real-time single-message
delivery once a consumer has shown it can keep up. Pacing is something each
consumer negotiates for itself on (re)connect, not something DMEx
auto-detects and manages centrally.

Every message's `data.metadata` (see [Message Envelope](#message-envelope))
carries `max_id`: the log's current latest id, given the subscription's
`topic`, as of the moment that message was sent — recomputed fresh on every
flush via the same cheap latest-id lookup used elsewhere (see
[Storage Adapters](#storage-adapters)), not cached at connect time. Diffing
`message.id` against `max_id` is exactly the "am I falling behind" signal a
consumer needs in order to decide whether its next reconnect should widen
`batch_window`.

## Process Architecture

Both sides of a subscription are modeled as one OTP process **per SSE
connection**, not as shared/pooled infrastructure — each connection owns its
half of the wire independently, and a crash on one connection can't affect
any other.

### Consumer side

- `DMEx.Consumer.Supervisor` is a `DynamicSupervisor` that owns one
  `DMEx.Consumer.Stream` `GenServer` per active subscription
  (`DMEx.Consumer.subscribe/2` starts a new child under it).
- Each `DMEx.Consumer.Stream` process owns exactly one outbound SSE HTTP
  connection to a producer: it holds the connection, parses `DMEx.Message`
  chunks off it, decodes them via the subscription's `event_modules`, and
  processes them one at a time, in order, against the subscription's
  `handler` module.
- **Processing a single message** is: begin a local transaction (consumer's
  own storage) → call `handler.handle_message/2` → on success, write the new
  `last_seen` in that same transaction and commit → on the handler raising,
  the transaction rolls back (so `last_seen` is untouched) and
  `DMEx.Consumer.Stream` retries that *same* message locally — no new fetch
  needed, it's still in hand — a bounded number of times (with backoff,
  mirroring how [Oban](https://github.com/oban-bg/oban) retries a failing
  job before giving up).
- **Once local retries are exhausted, `on_failure` decides what happens
  next:**
  - **`:crash` (default)** — the process lets the exception propagate rather
    than skipping the message or swallowing the error.
    `DMEx.Consumer.Supervisor` restarts it per normal one-for-one
    supervision. On restart it re-reads its persisted `last_seen` — still
    pointing at the message before the one that crashed it, since that
    transaction never committed — and reopens the SSE connection with that
    `last_seen`. The producer's own catch-up range-scan (`id > last_seen`)
    then naturally re-streams the same message that caused the crash. A
    permanently-failing handler crash-loops on the same message forever
    rather than silently dropping it.
  - **`:skip`** — instead of crashing, the transaction commits an advance of
    `last_seen` past the failing message *without* the handler having
    succeeded, and processing continues with the next message. The failure
    is not otherwise recorded by DMEx (application-level logging/telemetry
    is on the handler); the message is simply gone from the consumer's
    perspective.
  - **`:dead_message`** — same as `:skip` (advance `last_seen`, keep going),
    but first the failing message — its `event_type`, `topic`, original log
    `id`, `data`, `metadata` (including `origin`), the failure reason, and
    the attempt count — is written to a `dead_messages` table via the
    consumer's own storage adapter, in the same transaction as the
    `last_seen` advance. Unlike `last_seen`, `dead_messages` is **one table
    shared across all of a consumer's subscriptions**, not one per
    subscription — a single place to see every message any subscription in
    the application ever gave up on.
  - In all three modes, the local-retry step beforehand is identical; only
    the terminal action after retries are exhausted differs. See
    [Open Questions](#open-questions) for what's still unresolved about
    `dead_messages` (retention, replay tooling).

### Producer side

- `DMEx.Producer.Supervisor` is the mirror image: a `DynamicSupervisor` that
  owns one `DMEx.Producer.Stream` `GenServer` per **accepted** inbound
  connection. The Phoenix endpoint starts one when a
  `GET /streams/:app` request comes in, handing it the negotiated
  `last_seen`/`topic`/`batch_window`/`max_batch_size`.
- Each `DMEx.Producer.Stream` process owns exactly one consumer's half of the
  connection: the chunked HTTP response it writes SSE events to, and an
  accumulator of pending rows for that connection. It seeds the accumulator
  with its own initial range-scan, then registers with that app's
  `DMEx.Dispatcher` (see [Live dispatch](#live-dispatch-sql-based-triggers))
  as interested in new rows, filtered by its `topic`, which keep landing in
  the same accumulator as they're fanned out.
- On a `batch_window` timer (reset after every flush, and fired early if
  `max_batch_size` is reached), `DMEx.Producer.Stream` flushes whatever is in
  its accumulator: nothing if empty, a single-message `DMEx.Message` if
  exactly one row, or one `event: batch` `DMEx.Message` otherwise — see
  [Adaptive batching & backpressure](#adaptive-batching--backpressure-batch_window-max_batch_size).
  Every flush stamps `data.metadata.max_id` with the current latest id for
  that connection's `topic`, read fresh at flush time, never cached from
  subscribe time. A slow or crashed consumer connection only affects its own
  `DMEx.Producer.Stream` and its own accumulator; it never blocks dispatch or
  flushing for any other connection.

## Storage Adapters

A common `DMEx.Storage` behaviour is implemented by both a SQLite adapter and
a Postgres adapter (via Ecto), so applications can pick whichever fits their
deployment without changing the Publishing/Consuming APIs. The behaviour
needs to support, at minimum:

- Appending a message and returning its assigned id.
- Range-scanning messages with `id > last_seen`, in id order, efficiently
  (i.e. an index on `id`) — feeds both a fresh connection's initial
  catch-up scan and `DMEx.Producer.Stream`'s ongoing accumulator (see
  [Adaptive batching & backpressure](#adaptive-batching--backpressure-batch_window-max_batch_size)).
- Reading the latest `id` (and, given a `topic`, the latest `id` for that
  topic) cheaply, for stamping `max_id` on every flush (same section).

The schema is intentionally minimal so it maps cleanly onto both engines.

The same `DMEx.Storage` behaviour (and the same two adapters) is reused on
the consumer side for its own local bookkeeping — the `last_seen` table per
subscription and the shared `dead_messages` table (see
[Process Architecture](#process-architecture)) — rather than inventing a
second storage abstraction for that half of DMEx.

### Live dispatch (SQL-based triggers)

Once a consumer has caught up to the end of the log, new messages need to
reach it as they're published, not just on the next poll. DMEx borrows the
belt-and-suspenders dispatch pattern from
[Oban](https://github.com/oban-bg/oban) and
[EventStore](https://github.com/commanded/eventstore) rather than inventing
a new one:

- **Postgres**: `DMEx.Publisher.publish/2` issues `NOTIFY dmex_<app>, <id>`
  (via `Postgrex.Notifications`, same mechanism Oban's `Oban.Notifier` and
  EventStore's subscriptions use) right after the insert. A per-app
  `DMEx.Dispatcher` GenServer `LISTEN`s and, on notification, reads rows with
  `id > last_dispatched` and fans them out to the registered
  `DMEx.Producer.Stream` processes (see
  [Process Architecture](#process-architecture)) whose `topic` matches each
  row. The `NOTIFY` payload only carries a wakeup signal (the new id), not
  the row data itself — Postgres caps `NOTIFY` payloads at 8000 bytes, and
  both Oban and EventStore treat it purely as a signal, re-reading the
  authoritative row from the table.
- **Both engines**: the same `DMEx.Dispatcher` also polls on a fixed
  interval (e.g. every second) regardless of notifications received, exactly
  as Oban's stager polls even with `LISTEN`/`NOTIFY` active. This is what
  makes dispatch *robust*: a missed or coalesced `NOTIFY` (e.g. across a
  connection blip) only costs up to one poll interval of latency, never a
  stuck consumer.
- **SQLite**: no `LISTEN`/`NOTIFY` equivalent exists, so `DMEx.Dispatcher`
  runs polling-only — the same code path Postgres uses as its fallback,
  matching how Oban's SQLite (Lite) engine operates versus its Postgres
  (Basic) engine.

## Delivery Semantics

- **At-least-once delivery.** A consumer may receive the same message more
  than once (e.g. if it crashes after processing but before durably
  recording its new `last_seen`). Consumers must be idempotent with respect
  to message id.
- **Ordering** is guaranteed only within a single app's stream, never across
  streams.
- **Retention is unbounded by default.** Nothing is deleted as a side effect
  of consumption; a log grows until an operator explicitly prunes/archives
  it (policy TBD — see Open Questions).

## Decentralization / No-SPOF Story

To be precise about what "no single point of failure" means here: DMEx
introduces no shared broker, cluster, or coordination service whose failure
would affect multiple applications at once. Removing DMEx-the-broker from
the picture removes an entire class of shared-infrastructure outage.

This is **not** a claim of automatic high availability for any individual
application's stream — if a producer's own database goes down, that
producer's stream is unavailable until it recovers, exactly as it would be
for any other data that application owns. Achieving HA for a given stream is
the same problem as achieving HA for that application's database (e.g.
Postgres replication/failover), and is out of scope for DMEx itself.

## Open Questions

Things intentionally left open for the next round of discussion:

- **Auth / access control** — who's allowed to read a given producer's SSE
  endpoint, and how is that enforced?
- **Backpressure & slow consumers** — `batch_window`/`max_batch_size` plus
  `max_id` give a consumer the means to *notice* it's falling behind and
  *self-correct* by reconnecting with different values, but that requires
  the consumer to actually be watching and to actually reconnect. Still
  open: is falling back purely consumer-initiated, or should the producer
  ever proactively drop/throttle a connection it can see is chronically
  behind (e.g. its `DMEx.Producer.Stream` accumulator growing unbounded
  between flushes) regardless of whether that consumer is paying attention?
- **`batch_window`/`max_batch_size` defaults & bounds** — what are DMEx's
  own defaults when a consumer doesn't specify them, and are there
  producer-side sanity limits (a floor on `batch_window`, a ceiling on
  `max_batch_size`) to stop a misconfigured or malicious consumer from
  requesting pathological values?
- **Retention / compaction policy** — logs grow forever by default; what
  operator-facing controls exist for archiving or pruning old messages, and
  how does that interact with "arbitrarily replayable"?
- **Multi-consumer fan-out** — many consumers reading the same producer
  stream independently is already supported (each tracks its own
  `last_seen`, and each gets its own `DMEx.Producer.Stream` process), but
  are there limits on concurrent SSE connections/processes per producer app
  worth designing for?
- **Topic matching semantics** — is `topic` an exact-match tag only, or does
  it need RabbitMQ-style hierarchical/wildcard patterns (e.g. `orders.*`)?
  Also: should an event module's `topic:` be allowed to default from its
  `type:` when omitted, or always be required explicitly?
- **Slow-consumer isolation depth** — a `DMEx.Producer.Stream` process
  crashing only affects its own connection, but if its accumulator keeps
  growing between flushes because the underlying HTTP write to a slow
  client can't keep up (independent of `batch_window`/`max_batch_size`
  tuning), does `DMEx.Dispatcher` ever need to shed that connection
  (drop/disconnect) rather than let it grow unbounded?
- **Local retry backoff strategy** — how many in-process retries before
  `on_failure` kicks in, and on what schedule (fixed, exponential,
  jittered)? Same question regardless of which `on_failure` mode is
  configured.
- **`dead_messages` lifecycle** — is there any built-in tooling to inspect,
  replay, or purge rows in `dead_messages`, or is it purely an
  operator-queried table? Does it ever get pruned, or does it grow unbounded
  like the message log itself?
- **`:skip` visibility** — `:skip` silently drops a message with no DMEx-side
  record of the failure (unlike `:dead_message`). Is that intentional as a
  "you must supply your own telemetry" mode, or should DMEx always emit a
  telemetry event/log line on skip regardless of `on_failure`?
- **Batch framing shape** — is a single `event: batch` chunk whose `data:`
  is a JSON array of encoded messages the right shape, or should each
  batched message instead get its own `data:` line, using the SSE spec's
  own multi-line-per-field convention rather than an inner JSON array? Also:
  is there a byte-size cap on a flush (independent of `max_batch_size`'s row
  count) worth enforcing, given SSE/HTTP chunk size practicalities?
- **Event schema versioning** — each event type now has a dedicated Elixir
  module, but there's no versioning scheme yet for evolving a struct's
  fields (e.g. `order_placed_v2`) without breaking consumers pinned to the
  old shape.
- **Sharing event modules across independent applications** — the whole
  [Event Specifications](#event-specifications) design assumes the consumer
  has the *same* Elixir module (`MyApp.Events.OrderPlaced`) the producer
  used, so it can `use DMEx.Event`-derive the same `event_type` and decode
  `data` into the same struct. That's a given inside one umbrella app, but
  producer and consumer are, in general, independent applications — possibly
  different teams, repos, deploys, even languages. How does a consumer get
  its hands on the producer's event modules? A few options worth weighing
  against each other:
  - **Shared library/package** — the producer app publishes its event
    modules as a small, dependency-light package (private Hex package or git
    dep) that both sides add as a dependency. Keeps the "one Elixir module is
    the schema" model intact end to end; closest to how
    [Commanded](https://github.com/commanded/eventstore)-based systems
    typically share event structs. Costs: a real versioned-release/publish
    workflow, and it's Elixir-only — no path for a non-Elixir consumer.
  - **Self-describing producer / schema endpoint** — alongside the SSE
    endpoint, the producer exposes something like
    `GET /streams/:app/schema` listing its known `event_type`s and field
    shapes (e.g. as JSON Schema). Consumers could codegen structs from it,
    or just validate/introspect at runtime. Decouples deploys and opens the
    door to non-Elixir consumers, at the cost of a second interface DMEx has
    to keep honest and versioned alongside the actual event modules.
  - **Loose/dynamic decode fallback** — let a subscription skip
    `event_modules` entirely and receive the raw decoded JSON `map` plus
    `event_type` string instead of a typed struct. No shared module needed
    at all, but the consumer loses compile-time struct guarantees and just
    has to know the shape out-of-band (docs, tests) — closer to how most
    ad-hoc webhook consumers already operate today.
  - **External schema/type registry** — a separate, independently-versioned
    store of type definitions (could be as low-tech as a dedicated git repo
    of module source both sides vendor, or a real schema registry service)
    that isn't coupled to either app's release cadence. Most infrastructure
    to stand up, but avoids tying producer and consumer deploy schedules
    together the way a shared library dependency does.
  
  These aren't mutually exclusive — e.g. a schema endpoint could exist
  purely for discovery/tooling while a shared package remains the actual
  runtime dependency for Elixir consumers.
- **Event-module registry** — consumers currently supply their
  `event_modules` list by hand per `subscribe/2` call; is there a lighter-weight
  registration mechanism (e.g. an application-level config/registry) worth
  adding once there are many event types?
- **Poll interval tuning** — what's the default `DMEx.Dispatcher` poll
  interval, and is it configurable per app/stream, matching the
  latency/DB-load tradeoff Oban exposes via its stager?
- **Cross-node clustering** — if a producer application itself runs as a
  multi-node OTP cluster, how is "the" append-only log, and the
  `LISTEN`/`NOTIFY`-based `DMEx.Dispatcher`, kept single-writer consistent
  across nodes?
