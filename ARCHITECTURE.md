# DMEx — Architecture

> Status: working draft. The core mechanics below are agreed on; see
> [Open Questions](#open-questions) for what is still undecided, and
> [Event-Type Selection](#event-type-selection-post-v1) for the one mechanic
> deliberately left out of v1.

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

**Scope for v1.** A producer streams its whole log, and every subscription
receives every message on it. There is no server-side selective delivery in
v1: no filters, no per-subscription subsets, and — here or later — no topics.
A consumer that cares about only some event types declares the modules it
knows and ignores everything else locally (see
[Consuming API](#consuming-api)). Server-side selection is a real requirement
and it has a design — the consumer's own `event_modules` list, sent as a query
parameter and applied as a predicate on the producer's read — but it is
deliberately not in v1; see
[Event-Type Selection](#event-type-selection-post-v1).

### References

Prior art DMEx deliberately builds on rather than reinvents:

- [MDN: Using server-sent events — Event stream format](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events#event_stream_format)
  — the wire format DMEx must be a valid producer and consumer of; see
  [Message Envelope](#message-envelope).
- [oban-bg/oban](https://github.com/oban-bg/oban) — reference for robust,
  SQL-only job dispatch: `LISTEN`/`NOTIFY` for low-latency wakeup backed by
  belt-and-suspenders polling, and a Postgres/SQLite-agnostic engine
  abstraction.
- [commanded/commanded](https://github.com/commanded/commanded) — reference
  for one Elixir module (and struct) per event type, owned by the publishing
  application.
- [commanded/eventstore](https://github.com/commanded/eventstore) — reference
  for an append-only, Postgres-backed event log with subscription/dispatch
  semantics.

## Core Concepts

### Message log

Each application owns exactly **one** append-only message log: a single,
totally-ordered stream of every message that application has ever published.
It is backed by one plain SQL table (SQLite or Postgres):

```sql
CREATE TABLE dmex_messages (
  id          bigserial   PRIMARY KEY,        -- SQLite: INTEGER PRIMARY KEY AUTOINCREMENT
  event_type  text        NOT NULL,
  data        jsonb       NOT NULL,           -- SQLite: text
  metadata    jsonb       NOT NULL DEFAULT '{}',  -- SQLite: text
  inserted_at timestamptz NOT NULL DEFAULT now()
);
```

- `id` — a monotonically increasing offset, and the primary means of ordering
  and resumption. **`bigint`, not `int`**: a log nothing deletes from
  outgrows 2³¹. On SQLite, `AUTOINCREMENT` rather than a bare
  `INTEGER PRIMARY KEY`, so ids stay monotonic even after a retention policy
  (see [Open Questions](#open-questions)) has deleted the tail — a plain
  rowid can be reused, which would break every consumer cursor at once.
- `event_type` — the type string of the event module the row was published
  from (see [Event Specifications](#event-specifications)). It is the only
  classification a row carries, and it describes what the message *is*, never
  who might want to read it (see below).
- `data` — the JSON-encoded event struct. `jsonb` on Postgres, so operators
  can query inside payloads directly; the accepted cost is that `jsonb`
  normalizes key order and drops duplicate keys, so stored bytes are not
  byte-identical to what the publisher encoded (see
  [Open Questions](#open-questions)). SQLite has no `jsonb` column type —
  `text`, with its JSON functions available over it.
- `metadata` — free-form message metadata, entirely publisher-owned: DMEx
  writes nothing of its own into it (see [Publishing API](#publishing-api)).
  Defaulted to `{}` rather than nullable, so no read path has to distinguish
  "no metadata" from `NULL`.
- `inserted_at` — timestamp, for observability only (never used for ordering;
  see [Append ordering](#append-ordering-id-gaps) for why wall-clock time
  cannot be trusted for that).

Two notes on the table itself. It is **prefixed** (`dmex_messages`, following
Oban's `oban_jobs`) because it lives in the host application's own database
alongside that application's tables, where an unprefixed `messages` would be
a likely collision. And **the primary key is the only index**: every read DMEx
issues is an `id > cursor` range scan over the whole log (see
[Producer side](#producer-side)), which the primary key serves directly, so
v1 ships no secondary index at all — see [Open Questions](#open-questions) for
whether `event_type` deserves one anyway.

The table is append-only: rows are never updated or deleted by normal
operation. This is what makes arbitrary replay possible.

**There is no `topic` column, and its absence is load-bearing.** An earlier
draft tagged each row with a topic at publish time and filtered subscriptions
on it. The problem is that this table is immutable, so any classification
stamped into a row is immutable too: the set of messages a topic denotes would
be fixed, message by message, at the moment each one was published. Renaming
or re-scoping a topic could never apply to history; a message published before
a topic existed could never join it; and a row could belong to exactly one
topic, forever. That is a *filing* decision — made by and for whoever reads
the log — welded into the *record of what happened*, and the two have
different lifetimes.

So the log records only what a message is (`event_type`), never who might want
it. Selective delivery becomes a separate mechanic resolved at read time,
where the interested party names the event types it wants and the log is not
asked to have anticipated the grouping. That mechanic is sketched but
deliberately **not in v1** — see
[Event-Type Selection](#event-type-selection-post-v1), which also explains why
it introduces no *replacement* for `topic` either, named or otherwise. In v1
there is exactly one stream per producer, every subscription is that whole
stream, and `id` is drawn from a single per-app sequence, so every consumer's
`last_seen` is directly comparable with every other's.

### Producer

The application that owns a message log is the **producer** for that log. It
is the only writer, and it exposes its log to the outside world over HTTP via
an SSE endpoint. There is no separate "broker" process — the producer *is*
the application, and the queue is just a table it happens to keep.

Concretely, a producer application declares its own producer module (e.g.
`use DMEx.Producer` in `MyApp.Producer`), the same pattern as event modules
(see [Event Specifications](#event-specifications)). That module is what gets
passed as the first argument to `DMEx.Publisher.publish/3`, and it is the
handle for everything append-time that isn't the event itself: which storage
adapter and repo the insert goes through, and the app key that scopes the
[append lock](#append-ordering-id-gaps) and the `dmex_<app>` notification
channel (see [Live dispatch](#live-dispatch-wakeups)).

Note what it deliberately does *not* do: stamp the publisher's identity into
the message. **Nothing records "which producer published this," because
nothing needs to.** `event_type` is the event module's fully-qualified name
(see [Event Specifications](#event-specifications)), so
`MyApp.Events.OrderPlaced` already says which application it came from, on
every message, structurally — and a consumer independently knows, because it
dialed one statically-configured producer URL to get the stream at all. A
separate `origin` field would restate on the wire what the type string and
the connection each already establish.

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
| `id` | `id:` | The highest message id contained in this frame (see below). DMEx's own client ignores it, resuming from its durable `last_seen` instead, but it is what a generic SSE client echoes back as `Last-Event-ID` — the same header DMEx's client uses (see [Transport](#transport-sse-over-http)), so both resume through one mechanism. |
| `event` | `event:` | A **transport-level** framing type, not the application's event type. Always `batch` for data frames — there is no single-message frame type (see [Batching & framing](#batching--framing-batch_window-max_batch_size)). This is the only field DMEx itself ever branches on when parsing a chunk. |
| `data` | `data:` | A JSON **object**, `{"metadata": {...}, "messages": [...]}`: frame-level metadata plus `messages`, a list of **serialized log rows** exactly as stored — `id`, `event_type`, `data`, `metadata`, `inserted_at` (see [Message log](#message-log)). **The application's event type lives here, nested under `data`, not in `event:`** (see [Event Specifications](#event-specifications)). |
| `retry` | `retry:` | The producer's effective heartbeat interval, in ms — a spec-compliant reconnect delay for a generic client, and the pacing signal a DMEx consumer measures silence against (see [Heartbeats](#heartbeats)). Not used for *resumption*, which is driven by `last_seen`. |

Deliberately, the application's event type is **not** carried in SSE's
`event:` field. `event:` is reserved for DMEx's own transport-level framing
concerns and is meant to stay a small, fixed vocabulary DMEx controls; the
open-ended, ever-growing set of application event types belongs entirely
inside `data`, where it can't collide with or constrain how the transport
layer itself evolves.

**Every data frame is a batch**, including a frame carrying exactly one
message — it is simply a one-element `messages` array. There is no separate
single-message framing. This costs a couple of bytes on the wire and buys a
single encode path, a single decode path, and no branch anywhere in DMEx
that could let batched and unbatched messages drift apart in behaviour.

Because a frame carries many messages but SSE allows only one `id:` per
frame, **the frame's `id:` is the highest message id in the frame**.
Per-message ids live inside `data.messages`, each row carrying its own `id`. A
consumer advances `last_seen` per message from that list, never from the
frame's `id:`; the
frame-level `id:` exists so that a generic, non-DMEx SSE client resuming via
`Last-Event-ID` lands on a correct (never skipped, never
re-read-from-the-middle) position.

`data` has exactly two keys, and only one of them is DMEx's invention:

- **`messages`** — the frame's rows from the log, serialized as they are
  stored (`id`, `event_type`, `data`, `metadata`, `inserted_at` — see
  [Message log](#message-log)). No wrapper, no reshaping, no fields lifted out
  or renamed: whatever the log holds is what the frame carries. That keeps
  encoding to "serialize what the read returned" and means the wire format has
  no shape of its own to drift from the schema.
- **`metadata`** — frame-level facts, computed once at send time. Currently
  `max_id`: the log's current latest id as of the moment the frame was sent
  (see [Batching & framing](#batching--framing-batch_window-max_batch_size)).
  This
  is the one thing that is genuinely *about the frame* rather than about any
  row in it, which is why it needs a home of its own — stamping it into every
  row would repeat it up to `max_batch_size` times and invite a reader to
  treat it as per-message truth.

A row's own `metadata` field therefore stays where it is, inside the row. It
is the consumer that picks it out when calling a handler (see
[Consuming API](#consuming-api)); the producer does no rearranging on the way
out. Nothing DMEx adds is ever merged into that map either, so there are no
reserved key names a publisher has to avoid.

The rest of each row is **DMEx's** half: `id` drives resumption and
[duplicate suppression](#consuming-api), `event_type` selects the module to
decode into — and, since a v1 subscription is the producer's whole log,
decides whether the row is delivered to the handler at all (see
[Consuming API](#consuming-api)). Neither reaches application code — a handler
receives its decoded struct and the row's `metadata`, nothing else, so no
consumer ever has to know the log row exists.

`DMEx.Message` models a **frame**, not a single message: one struct per SSE
chunk, holding the frame's metadata and the list of messages it carries. It
owns exactly two responsibilities: encoding itself to a spec-compliant
`text/event-stream` chunk, and parsing a chunk back into a struct — decoding
`data` one level (into frame metadata plus the list of rows) is as far as it
goes. It knows nothing about any particular event's *shape*; turning a row's
`data` into a concrete struct is the event module's job, and the frame struct
itself never reaches application code — a handler sees its own event and that
row's metadata, never the frame (see
[Consuming API](#consuming-api)). This split keeps the wire format
concern (this module) fully decoupled from the domain payload concern (event
modules, next section).

## Event Specifications

Rather than publishing bare maps, each event a producer emits is defined as
**its own Elixir module** — one struct per event type, owned by the
publishing application — the same shape as
[Commanded](https://github.com/commanded/commanded) event modules:

```elixir
defmodule MyApp.Events.OrderPlaced do
  use DMEx.Event

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
- **`use DMEx.Event` takes no options at all.** Its whole job is to give the
  module a `type/0` callback that `publish/3` reads when appending — where
  `event_type` is the column that then travels inside each row of the
  `DMEx.Message` [data envelope](#message-envelope), never
  `DMEx.Message.event` itself, which stays transport-level (see above). In
  particular there is no `topic:` option, and no delivery-grouping option of
  any other name: an event module declares what the event *is*, and nothing
  about who should receive it. Who receives it is the *consumer's* statement to
  make, and it already makes it — see
  [Event-Type Selection](#event-type-selection-post-v1).
- The struct fields are the event's full, versioned shape. Evolving an
  event's shape is scoped to editing that one module (see
  [Open Questions](#open-questions) on schema versioning).
- On the read side, a consumer must be able to turn `event_type` back into
  the same struct. DMEx does this via an explicit module list/registry
  supplied by the consumer (e.g. `event_modules: [MyApp.Events.OrderPlaced, ...]`)
  rather than dynamically atomizing whatever string the producer sent —
  turning arbitrary attacker-controlled strings into atoms is an
  unbounded-atom-table risk, so the mapping is always consumer-declared, not
  inferred. In v1 that list does double duty: because a subscription is the
  producer's entire log, `event_modules` is also the consumer's *filter* (see
  [Consuming API](#consuming-api)) — and it is the same list that would later
  become the wire-level selector sent to the producer, which is why no
  separate grouping concept is needed anywhere (see
  [Event-Type Selection](#event-type-selection-post-v1)).

## Publishing API

Sketch, in `DMEx` namespace — `publish(producer, event, opts \\ [])`, so both
`publish/2` and `publish/3` exist; this document says `publish/3` throughout:

```elixir
DMEx.Publisher.publish(MyApp.Producer, %MyApp.Events.OrderPlaced{...}, metadata: %{})
```

From the API's perspective, **publishing a message is just writing a row to
the database** — nothing more. `publish/3`:

- serializes the event struct with Elixir's native `JSON` module
  (`JSON.encode!/1`, via `@derive JSON.Encoder` on the event struct);
- reads `event_type` off the struct's own module (`OrderPlaced.type/0` — never
  passed as a call argument);
- passes `metadata` through untouched — the producer module is not recorded
  in it, since `event_type` is fully qualified and already identifies the
  publishing application (see [Producer](#producer));
- does a single local Ecto `insert` (`event_type`, `data`, `metadata`) via
  the configured storage adapter, and returns the assigned `id` (offset) of
  the row.

Note that `event_type` is its own column here (see
[Message log](#message-log)), even though on the wire it travels nested inside
the SSE `data:` envelope rather than as `event:` (see
[Message Envelope](#message-envelope)).

There is no client/server RPC, no broker call, and no network hop on the
write path — the insert itself, plus the [live dispatch](#live-dispatch-wakeups)
mechanism that notices it afterwards, is the entire publish story. It's a
normal database write against the app's own database, no different from any
other write that application makes.

### Append ordering (id gaps)

One caveat the "just an insert" framing hides, and it is not optional to
resolve: **publishing is single-*application*, not single-*writer*.** Two
concurrent requests in the same app calling `publish/3` are two concurrent
transactions.

That matters because Postgres allocates sequence values at INSERT time but
makes rows visible at COMMIT time. If T1 takes `id=100` and T2 takes
`id=101` but T2 commits first, a reader scanning `id > 99 ORDER BY id`
sees 101 and not 100, advances its cursor past it, and **permanently skips
message 100** once T1 finally commits — with `last_seen` advancing right
over the gap, so nothing downstream ever detects the loss.

Every read path in DMEx is a cursor over `id` (see
[Producer side](#producer-side)), so this is load-bearing rather than
theoretical. It is also silent, which makes it the most dangerous failure
mode in the design.

DMEx therefore **serializes appends per app**: `publish/3` takes a
*transaction-scoped* Postgres advisory lock keyed on the app
(`pg_advisory_xact_lock`), so ids commit in the order they were allocated and
the cursor is sound by construction. The lock must be held until COMMIT, not
merely for the duration of the insert — a lock released at the end of the
INSERT statement lets a later-allocated id commit first, which is the exact
failure it was meant to prevent.

This costs publish concurrency — appends to one log are effectively serial —
in exchange for a reader model with no gap-detection logic anywhere in it.
On SQLite the point is moot: it is single-writer already.

Two consequences worth stating: a `publish/3` call made *inside* an enclosing
application transaction extends the lock to that whole transaction's
lifetime, so a long-running caller transaction serializes every other publish
in the app for its duration; and see [Open Questions](#open-questions) for
the alternatives considered (snapshot-watermark reads, read-behind lag) and
why they lost.

## Consuming API

Sketch, in `DMEx` namespace:

```elixir
defmodule MyApp.OrderHandler do
  use DMEx.Consumer.Handler

  @impl true
  def handle_message(%MyApp.Events.OrderPlaced{} = event, metadata) do
    # side effects go here; raise (or let something raise) to signal failure
    :ok
  end
end
```

```elixir
DMEx.Consumer.subscribe(producer_url,
  event_modules: [MyApp.Events.OrderPlaced],
  handler: MyApp.OrderHandler,
  start: :beginning,       # :beginning (default) | :latest | <id>; first run only
  on_failure: :crash,      # :crash | :skip | :dead_message
  batch_window: 200,       # ms; preferred minimum time between frames
  max_batch_size: 500,     # rows; preferred maximum messages per frame
  queue_limit: 2_000,      # rows; bound on the internal queue
  adaptive: true           # re-negotiate the two above from measured drain rate
)
```

- Starts a subscription — see [Process Architecture](#process-architecture)
  for the full mechanics of the process that owns it — which opens an SSE
  connection to a remote producer's endpoint, resuming from the `last_seen`
  DMEx itself has durably persisted (there is no `last_seen` argument: DMEx
  owns that bookkeeping and sends it as `Last-Event-ID`, see
  [Transport](#transport-sse-over-http)), and sending
  `batch_window`/`max_batch_size` as request headers to control delivery
  pacing (see
  [Batching & framing](#batching--framing-batch_window-max_batch_size)).
  **There is no filter option of any kind**: a v1 subscription is the
  producer's whole log, and `event_modules` is what narrows it, locally (see
  [Event-Type Selection](#event-type-selection-post-v1) for how that same list
  later becomes a server-side selector, without gaining a new option here).
- **`batch_window` and `max_batch_size` are a preference, not a setting.**
  They are what the consumer asks for on the *first* connect and what it
  converges back toward when it is keeping up; the producer may clamp them to
  its own bounds, and DMEx's own client will ask for different values on a
  later reconnect if it measures that it cannot drain what it requested (see
  [Backpressure & adaptive batching](#backpressure--adaptive-batching)).
  `adaptive: false` pins them, for a consumer that would rather have
  predictable framing than an automatically-fitted one.
- **`start:` only applies on a first run.** It is the answer to "where does
  this handler begin when it has no cursor yet": `:beginning` (the default,
  the whole log), `:latest` (from the tip, skipping history), or an explicit
  id. It maps straight onto the `Last-Event-ID` values above — `:latest`
  travels as the string `latest` — and once a cursor exists it is ignored
  entirely, since the durable `last_seen` is always the authority. Note the
  one wrinkle in `:latest`: a consumer that starts, receives nothing, and
  restarts sends `latest` again and so resolves against a *newer* tip, which
  is the honest meaning of "from now on" rather than a lost-message bug —
  nothing was ever committed to be lost.
- **`last_seen` is keyed by handler module.** The `handler` is the
  subscription's identity — `MyApp.OrderHandler` has exactly one cursor,
  wherever it reads from. This keeps the bookkeeping table trivial (handler
  module → id) and makes the cursor mean something a developer can reason
  about: "how far this piece of code has got," not "how far some producer URL
  has got." Two consequences follow, and both are worth knowing before they
  bite:
  - **One live subscription per handler module.** Subscribing the same handler
    to two producers would have both sharing and clobbering a single cursor.
    DMEx therefore registers each stream under its handler module and rejects
    a second `subscribe/2` for a handler that already has one running — a
    handler per stream is the model, and enforcing it is cheaper than
    debugging a cursor that jumps around.
  - **Renaming a handler module resets its cursor**, and the next start
    therefore replays the producer's log from the beginning. That is a routine
    refactor with a decidedly non-routine consequence, so a rename wants a
    deliberate migration of the row (see
    [Open Questions](#open-questions) on the consumer-side schema).
- **Reads the SSE socket in `active: :once` (or passive) mode, re-armed only
  while its internal queue has headroom.** This is a hard requirement, not a
  tuning choice: it is what makes the TCP receive window close when the
  handler falls behind, which is the structural backpressure mechanism of the
  system (see [Producer side](#producer-side)). A client that reads eagerly
  into its own mailbox does not remove the backpressure problem, it relocates
  it — the producer sees a fast-draining socket and streams at full rate into
  a consumer quietly accumulating unbounded chunks in memory. The queue
  (`queue_limit`, see
  [Backpressure & adaptive batching](#backpressure--adaptive-batching)) does
  not weaken that: it is bounded, so the re-arm still stops, just after
  `queue_limit` rows of slack rather than one frame's worth. What the slack
  buys is a *measurement* — how fast the handler actually drains — that a
  consumer holding exactly one frame has no way to take.
- De-batches the frame's already-decoded `data.messages` list
  (`DMEx.Message` does that one level of JSON decoding, see
  [Message Envelope](#message-envelope)) and, for each row: looks up
  `event_type` (**not** the SSE `event:` field) against the supplied
  `event_modules`, decodes the row's `data` into that module's struct, then
  invokes `handler.handle_message/2` with that struct and the row's
  `metadata`. **Picking the handler's arguments out of the row is the
  consumer's job**, and the only place it happens — the producer ships rows
  verbatim (see [Message Envelope](#message-envelope)), so this is the single
  point where the log's shape is translated into the application's.
- **An `event_type` with no matching module is skipped, not an error.** This
  is the direct consequence of v1 having no server-side selection: a
  subscription receives the producer's entire log, so rows the consumer has no
  module for are the normal case, not a fault. Such a row advances `last_seen`
  exactly as a handled one does — it must, or a consumer would wedge on the
  first event type it doesn't know — and invokes nothing. `event_modules` is
  therefore the subscription's filter, applied on the consumer after the bytes
  have already crossed the wire; the deferred selection mechanic moves that
  same list to the producer so they never cross it (see
  [Event-Type Selection](#event-type-selection-post-v1)). Skips are worth a
  telemetry event — a consumer silently discarding most of a stream and a
  consumer silently discarding the one type it wanted look identical
  otherwise.
- **Suppresses duplicates itself, by id.** Before running a handler, the
  stream drops any row whose `id` is `<= last_seen`. This needs no
  new state and no dedupe table: delivery within a subscription is strictly
  ordered and `last_seen` is durable, so "already processed" is exactly "id
  not greater than the cursor" — an integer comparison inside the transaction
  that is about to advance that same cursor. Note what this does and does not
  buy, because the distinction is the whole of
  [Delivery Semantics](#delivery-semantics): it makes *transport-level*
  re-delivery harmless, but it cannot cover a crash between a handler's side
  effects landing and its transaction committing, since it is that very commit
  that records the message as processed.
- **The handler sees its event and the publisher's metadata — nothing else.**
  `handle_message/2` takes the decoded struct and the row's `metadata` map.
  Everything else in the row stays with DMEx: `id` (resumption and
  duplicate suppression), `event_type` (which module to decode into),
  `max_id`, the frame's `id:`, the `DMEx.Message` struct. None of it is a
  property of the *event*, and none of it is something a consumer should have
  to reason about — so a handler is a plain function of its domain data plus
  whatever context the publisher chose to attach, testable by constructing a
  struct and a plain map.
- **`handle_message/2` succeeding (not raising) is what advances
  `last_seen`.** DMEx runs the callback and the `last_seen` update in one
  local transaction against the consumer's own storage — the same
  `DMEx.Storage` adapter behaviour used producer-side, just pointed at the
  consumer's own bookkeeping table. If the callback raises, the transaction
  rolls back and `last_seen` does not move; application code never manages
  this bookkeeping directly.
- `on_failure` controls what happens once local retries of a failing message
  are exhausted: `:crash` (default) — crash and let the SSE connection
  re-stream the same message on restart; `:skip` — give up on that message,
  advance `last_seen` anyway, and move on; `:dead_message` — write the
  failed message to the consumer's `dead_messages` table, then advance
  `last_seen` and move on. See
  [Process Architecture](#process-architecture) for the full mechanics of
  all three.

## Transport: SSE over HTTP

- A consumer opens `GET /stream` via Phoenix, sending its resume
  position in the **`Last-Event-ID` request header** and its delivery pacing
  in the **`DMEx-Batch-Window` and `DMEx-Max-Batch-Size` request headers**. The
  route is singular and carries no application segment and no query string at
  all: a producer application owns exactly one log (see
  [Message log](#message-log)) and v1 streams all of it, so there is nothing
  for a path or query parameter to select between, and the app's own hostname
  already identifies the app. Both pacing headers are optional and fall back
  to DMEx-supplied defaults (see
  [Batching & framing](#batching--framing-batch_window-max_batch_size)).
- **The URL says what the subscription *is*; headers say how it is *paced*.**
  That is the line the two mechanisms fall on, and it is why `batch_window`
  and `max_batch_size` are headers rather than query parameters. In v1 the
  left side of that line is empty — one producer, one log, one stream, so the
  URL has nothing to carry — but the line is the one a future event-type
  selector would fall on, since the set of types selected changes *which*
  messages exist for the subscription at all and therefore belongs in the URL
  (see [Event-Type Selection](#event-type-selection-post-v1)). The pacing
  values change nothing about *which* rows arrive or in what order, only how
  they are cut into
  frames; two connections differing only in `batch_window` deliver the same
  messages in the same order, grouped differently. Keeping them out of the URL
  means one stable URL per
  subscription across every reconnect, which matters more now than it did
  before: an adaptive consumer re-negotiates its pacing repeatedly over its
  lifetime (see
  [Backpressure & adaptive batching](#backpressure--adaptive-batching)), and if
  those values lived in the query string, every re-negotiation would mint a new
  URL and shatter that subscription's grouping in proxy logs, access logs, and
  metrics. It also puts pacing on the same channel as `Last-Event-ID`, which is
  already a header for the same reason.
- **Header naming and validation.** `DMEx-Batch-Window` (integer ms) and
  `DMEx-Max-Batch-Size` (integer rows), no `X-` prefix — that convention was
  deprecated by [RFC 6648](https://www.rfc-editor.org/rfc/rfc6648), and a
  vendor-named header is the current idiom. HTTP header names are
  case-insensitive, so Phoenix sees them downcased; nothing should ever compare
  them any other way. A header present but not a base-10 integer is a `400`,
  the same standard `Last-Event-ID` is held to: a client sending garbage has a bug
  worth hearing about. A well-formed value outside the producer's bounds is
  **clamped, not rejected** — the producer's floor on `batch_window` and
  ceiling on `max_batch_size` exist to protect the producer (see
  [Open Questions](#open-questions)), and a consumer asking for more than it
  may have should get a working stream at the permitted rate, not a `400` it
  has no way to interpret.
- **The producer echoes what it actually applied**, as `DMEx-Batch-Window` and
  `DMEx-Max-Batch-Size` **response** headers on the `200`. Without this, a
  clamped consumer cannot tell the difference between "the producer is honoring
  my request and I still can't keep up" and "the producer capped me and my
  request never took effect" — and those call for opposite reactions from a
  consumer adapting its own parameters. Echoing effective values closes that
  loop with no extra round trip, and gives the adaptive controller its actual
  operating point rather than its hoped-for one. Note this cannot be inferred
  from `retry:`, which carries `heartbeat_ms + batch_window` as a single sum
  (see [Heartbeats](#heartbeats)).
- **A browser `EventSource` cannot set these**, and that is an accepted
  regression from having them in the query string. `EventSource` sets no
  request headers of its own beyond `Last-Event-ID`, so a bare browser client
  gets the producer's defaults and nothing else. This is the deliberate trade:
  pacing is a tuning knob whose defaults must be sane anyway, resumption is a
  correctness mechanism — and resumption is the one the spec gave a header to.
  A generic client that does control its headers (any ordinary HTTP client
  library) has full access to both.
- **Resumption uses the spec's own channel, not a query parameter.**
  `Last-Event-ID` is what the SSE specification defines for exactly this, and
  using it means a bare browser `EventSource` — which sets the header itself on
  every reconnect and cannot set any other — resumes correctly with no
  client-side logic at all. DMEx's own client sends the same header, differing
  only in where the value comes from: its durable `last_seen` rather than the
  last frame's `id:`. One resumption mechanism, one code path on the producer.
  It also keeps the URL stable across reconnects, which groups cleanly in
  proxy and metrics tooling; the tradeoff is that the resume position no longer
  shows up in access logs, so it belongs in the producer's own telemetry
  instead.
- **`Last-Event-ID` is a string, and DMEx uses that.** The SSE spec puts no
  numeric constraint on the header, so the producer accepts a small vocabulary
  rather than an integer alone:
  - **an integer id** — resume after it, the ordinary case;
  - **`latest`** — start from the tip: the producer resolves it against the
    log's current `max(id)` and streams nothing until the next publish. This
    is how a consumer that only cares about what happens from now on skips
    history, without a preflight request or a second parameter
    (see [Consuming API](#consuming-api), where it is the `:latest` atom);
  - **absent** — from id 0, i.e. the whole log. A first connection with no
    stated position gets everything, which is also what a fresh `EventSource`
    does;
  - **anything else** — `400`. A vocabulary of two forms plus absent is small
    enough to validate exactly, and a client sending something outside it has
    a bug worth hearing about rather than being handed an arbitrary slice of
    the log.

- The producer then streams every message with `id > last_seen`, in ascending
  order, followed by new messages live as they're published, all framed
  identically as batch frames.
- The producer never turns a connect attempt away for load reasons. There is
  no `503`/`Retry-After` admission control, because there is no aggregate
  budget to enforce it against — see
  [What this deliberately does not do](#what-this-deliberately-does-not-do).
- The connection is long-lived. If it drops, the consumer reconnects, sending
  whatever `last_seen` it last durably recorded as `Last-Event-ID`. Reconnect
  backoff is owned
  by DMEx (`DMEx.Consumer.Stream`), not left to application code, and must
  be jittered — an event that drops many connections at once (a producer
  restart, a network blip) would otherwise bring them all back
  simultaneously, and with no admission control there is nothing to absorb
  that.
- Because the protocol is just HTTP + SSE, no custom binary protocol or
  client library is required — any SSE-capable HTTP client can consume a
  DMEx stream. Uniform batch framing helps here: a generic client sees one
  frame shape rather than having to branch on `event:`.

### Batching & framing (`batch_window`, `max_batch_size`)

There is no separate catch-up mode, no separate HTTP endpoint for it, and no
distinction between a "catching up" and a "live tailing" connection.
Catching up and live tailing are **the same SSE connection running the same
loop** (see [Producer side](#producer-side)); they differ only in whether
the loop finds rows waiting when it reads. Two parameters, supplied by the
consumer as request headers on connect (see
[Transport](#transport-sse-over-http)), tune it:

- `max_batch_size` — the `LIMIT` on each read, and therefore the most
  messages a single frame will ever contain.
- `batch_window` — the minimum time between frames. After sending, the loop
  waits out the remainder of the window before reading again, so a busy
  stream at the tip coalesces into periodic frames rather than a frame per
  publish.

These compose without conflict: `batch_window` is a hard floor on frame
*rate*, `max_batch_size` a hard cap on frame *size*. Because the loop is
sequential — read, send, wait, repeat — neither can preempt the other, and
together they cap the producer's per-connection work at
`max_batch_size` rows per `batch_window`.

A freshly-connected consumer whose `last_seen` is far behind the tip simply
finds a full `max_batch_size` on every read and sends full frames
back-to-back until it drains. That *is* bulk catch-up: the ordinary loop
running hot, not a distinct mode, endpoint, or process. No `HEAD` preflight
or separate backlog endpoint is needed — the first frame's `max_id`
metadata already tells the consumer how big the gap is.

Regardless of burst size, **batching is purely a wire-framing convenience,
never a change to processing semantics**: every de-batched message still
goes through the exact same one-at-a-time
[handler/transaction/`last_seen`/`on_failure` pipeline](#process-architecture)
— its own transaction, its own `handler.handle_message/2` call, its own
`last_seen` advance, its own `on_failure` handling if it fails. A batch is
never processed or committed as a unit; a mid-batch failure only ever rolls
back that one message, and reconnecting after a crash still resumes from a
precise `last_seen`, never "redo the whole batch." Since every frame is a
batch frame, there is no second path this could ever drift from.

Every frame carries `max_id` in its frame-level `data.metadata` (see
[Message Envelope](#message-envelope)): the log's current latest id as of the
moment the frame was sent — one value per frame, since that is what a single
read produces. Diffing a message's `id` against `max_id` is the "am I falling
behind" signal — useful for consumer telemetry and alerting. Note that it is
**not** the mechanism that protects the producer: that is the loop's own
structure (see
[Producer side](#producer-side)), which holds regardless of whether any
consumer is paying attention to `max_id`.

Both parameters are **per-connection, and fixed for that connection's
lifetime**. There is no in-band way to re-pace a live stream, and none is
wanted: the loop reads them out of `state` on every iteration (see
[Producer side](#producer-side)), so a mid-stream change would mean the
producer accepting and applying consumer input on an already-open connection —
a second inbound channel on a protocol that currently has none, on a request
process whose whole job is to write. A consumer that wants different pacing
reconnects with different headers, which costs it one `id > last_seen` read it
was going to issue eventually anyway and costs the producer nothing it doesn't
already handle (see
[Backpressure & adaptive batching](#backpressure--adaptive-batching)).

### Heartbeats

When a connection is caught up and the log is quiet, the producer sends
nothing for as long as no one publishes — potentially hours on a
low-traffic log. To keep the connection alive and observable, the producer
emits an SSE **comment frame** (`: hb`) on an idle interval. Comment lines
are ignored by every spec-compliant SSE client, so this never pollutes the
message stream or requires consumer-side handling.

Heartbeats do three jobs, and the third is the one that matters most:

- they stop idle-connection timeouts in intermediary proxies and load
  balancers from silently killing a healthy but quiet stream;
- they give the consumer a liveness signal, so it can distinguish "nothing
  new to send" from "connection is dead but the socket never closed," and
  reconnect on its own after missing several intervals;
- they give TCP keepalive something to work against on an otherwise silent
  connection, and bound how long a vanished peer can go unnoticed (see
  [Stalled connections](#stalled-connections)).

**The interval is advertised, not assumed.** A consumer can only count missed
heartbeats if it knows how often to expect one, and that interval is
producer-side configuration the consumer has no other way to learn. The
producer therefore sends SSE's `retry:` field, set to its **effective**
heartbeat interval — `heartbeat_ms + batch_window`, since the loop's
`throttle` runs after the heartbeat branch (see
[Producer side](#producer-side)), making that sum the real upper bound on
silence. It is emitted at stream open and re-sent if the value ever changes.

`retry:` is the spec-sanctioned channel for exactly this kind of pacing hint,
and it does double duty: a generic SSE client reads it as its reconnect
delay, while a DMEx consumer reads it as "expect a frame or a comment within
this window" and reconnects after `k` missed intervals. One caveat that
follows from coupling the two: DMEx's own client keeps its own jittered
backoff (see [Transport](#transport-sse-over-http)) rather than reconnecting
at exactly `retry:` the way a generic client does — a single advertised value
applied literally by every consumer is a synchronized reconnect storm, which
is the one thing the producer has no admission control to absorb.

Note the scope, because it is narrower than it looks: heartbeats are a
liveness aid for the *consumer* and for intermediaries. They are **not** a
producer-side stall detector. A stall that begins mid-data-frame leaves the
loop blocked inside the write, never reaching the heartbeat branch at all —
which is both the most likely stall and one no heartbeat-based check could
ever observe. Detecting that is the socket layer's job, not this one's.

## Process Architecture

Both sides of a subscription are modeled as one OTP process **per SSE
connection**, not as shared/pooled infrastructure — each connection owns its
half of the wire independently, and a crash on one connection can't affect
any other.

The two sides are not symmetric, though, and the asymmetry is deliberate.
Consumer-side connections are owned by a DMEx `DynamicSupervisor`, because
DMEx starts them. Producer-side connections are owned by the web server,
because the request *is* the connection — see
[Producer side](#producer-side).

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
    value as `Last-Event-ID`. The producer's own catch-up range-scan
    (`id > last_seen`)
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
    but first the failing message — its `event_type`, original log `id`,
    `data`, `metadata`, the producer URL it arrived from, the failure reason,
    and the attempt count — is written to a `dead_messages` table via the
    consumer's own storage adapter, in the same transaction as the
    `last_seen` advance. Unlike `last_seen`, `dead_messages` is **one table
    shared across all of a consumer's subscriptions**, not one per
    subscription — a single place to see every message any subscription in
    the application ever gave up on. That sharing is why the row records the
    producer URL: since messages no longer carry an `origin` (see
    [Producer](#producer)), the subscription that received it is what
    identifies where a dead message came from, and a shared table needs that
    on the row. A log `id` alone is ambiguous across producers.
  - In all three modes, the local-retry step beforehand is identical; only
    the terminal action after retries are exhausted differs. See
    [Open Questions](#open-questions) for what's still unresolved about
    `dead_messages` (retention, replay tooling).

#### Backpressure & adaptive batching

The producer's loop makes overload unrepresentable *for the producer* (see
[Producer side](#producer-side)). It does nothing for the consumer, and the
mechanism that protects the producer is actively hostile to a consumer that
cannot keep up: the consumer stops reading, its receive window closes, the
producer's `chunk/2` blocks, and after `send_timeout` the producer **closes the
connection** (see [Stalled connections](#stalled-connections)). The consumer
reconnects, is handed frames at the same rate it just failed to drain, and
stalls again. Nothing is lost — `last_seen` is durable and a drop is not a
delivery event — but the subscription settles into a reconnect loop, and each
reconnect costs the producer a fresh catch-up read against its own database.
The slower the consumer, the more often it makes the producer work.

The gap is that TCP backpressure is one bit. It can say *stop*; it cannot say
*send me less at a time*. `batch_window` and `max_batch_size` are exactly that
second message, and they are already per-connection consumer-supplied values —
so the consumer needs a way to know what to ask for, and an occasion to ask.

**The queue is the measurement.** `DMEx.Consumer.Stream` holds one bounded
FIFO of rows — de-batched on arrival, since `DMEx.Message` has already parsed
the frame — sitting between the socket and the
handler/transaction/`last_seen` pipeline. It gates the socket re-arm: the next
`active: :once` is requested only while the queue is below `queue_limit`, so
the receive window still closes and structural backpressure still holds (see
[Consuming API](#consuming-api)). The queue is not there to absorb overload —
`queue_limit` rows of slack absorb nothing at a sustained mismatch. It is there
so the process can observe two things a consumer holding exactly one frame
cannot:

- **depth** — how close the socket is to being held off, i.e. whether there is
  pressure right now;
- **drain rate** — an EWMA of messages committed per second, which is this
  handler's actual capacity, measured rather than configured.

The queue itself is not durable and does not need to be. Everything in it sits
above `last_seen` with no handler having run for it, so throwing it away is
indistinguishable from never having received it: the producer still holds those
rows, and the next connect re-reads them from the same cursor every reconnect in
this design already resumes from.

**The control law is AIMD**, over the connection rather than the packet. On
sustained pressure — depth above a high-water mark for longer than one
`batch_window`, so a single burst doesn't trip it — the consumer sizes its next
request to what it has been measured to drain:

```
max_batch_size' = clamp(drain_rate × batch_window, floor, max_batch_size)
```

That is the whole target: ask for no more per window than the handler empties
per window. If that computation hits the floor and pressure persists, the
consumer widens `batch_window` instead (multiplicatively), which lowers frame
*rate* once frame *size* has nowhere left to go. Recovery is the additive half:
after several consecutive windows spent near-empty, the consumer steps back
toward its configured `batch_window`/`max_batch_size` a notch at a time. Back
off fast, recover slowly — the asymmetry is deliberate, and it is the same
reason every other AIMD controller has it.

**Re-parameterizing means reconnecting, and the order matters:**

1. **Close the connection first.** This is the immediate relief and it is
   instant: it stops the inflow and hands the producer back its request
   process, rather than leaving a stalled socket for `send_timeout` to reap.
2. **Drain the queue locally.** Every queued row goes through the ordinary
   pipeline — its own transaction, its own `last_seen` advance — so nothing
   already transferred is re-fetched, and the drain itself refines the
   drain-rate estimate under no inbound load at all.
3. **Reconnect from the post-drain `last_seen`** with the new headers, after a
   jittered delay. The cursor is exactly current, so the new connection starts
   with an empty queue and no overlap to suppress.
4. **Read the echoed `DMEx-Batch-Window` / `DMEx-Max-Batch-Size` response
   headers** (see [Transport](#transport-sse-over-http)) and adopt those as the
   operating point. If the producer clamped the request, the controller's next
   decision must be made against what it was actually granted — otherwise it
   adapts against a number that never took effect and concludes its own
   arithmetic is broken. Re-read `retry:` at the same time: effective heartbeat
   is `heartbeat_ms + batch_window` (see [Heartbeats](#heartbeats)), so a
   consumer that just changed `batch_window` has invalidated its own
   liveness deadline.

Flapping is the obvious failure mode of any control loop whose actuator is a
reconnect, and three things damp it: separate high- and low-water marks
(hysteresis, so depth hovering at one threshold can't oscillate), a minimum
interval between re-parameterizations that must comfortably exceed one
close-drain-reconnect cycle, and the jittered delay in step 3 — for the same
reason all reconnect backoff here is jittered (see
[Transport](#transport-sse-over-http)), since a producer restart re-pacing
every consumer at once is a synchronized storm with no admission control to
absorb it.

Three limits, stated plainly:

- **Adaptation cannot make a handler faster.** If drain rate is below publish
  rate, the consumer falls behind permanently, and no framing parameter
  changes that. What it converts is the *symptom*: instead of a reconnect loop
  driven by `send_timeout`, the subscription runs stably at its own speed and
  falls visibly behind, which is what the `max_id` diff already reports (see
  [Batching & framing](#batching--framing-batch_window-max_batch_size)).
  Visible lag is a capacity problem an operator can see; a reconnect loop is a
  capacity problem disguised as a network one.
- **Learned values are in-memory and reset on restart.** A restarted stream
  asks for its configured preference again and re-learns over a few windows.
  Persisting them alongside `last_seen` was rejected: the right pacing is a
  property of the current host, load, and handler, not of the subscription, and
  a value pinned during one bad afternoon would quietly throttle that
  subscription forever. Under `on_failure: :crash` the queue dies with the
  process too, which is correct for the same reason — nothing in it was
  committed.
- **The producer never initiates any of this.** It holds no consumer state (see
  [Consumer](#consumer)) and has no channel to push a rate hint down; the only
  producer-side inputs are its clamp bounds and the echoed effective values.
  Adaptation is entirely a consumer-side loop closed through reconnects, which
  is what keeps it compatible with the "producer holds nothing" property rather
  than eroding it.

Finally, note who else this is for. A third-party SSE client reading eagerly
has no TCP backpressure to fall back on — its socket always drains fast and the
producer always streams at full rate (see
[Open Questions](#open-questions)). A bounded queue plus a reconnect with
smaller headers is a mechanism such a client can implement *without* control
over its socket's active mode, in any language, because both parameters are
now plain request headers. That is the flexibility argument for putting them
there: structural backpressure requires a cooperative client library, while
cooperative backpressure requires only an HTTP client that can set two headers.

### Producer side

**The Phoenix request process *is* the stream.** There is no
`DMEx.Producer.Supervisor` and no separate per-connection `GenServer`. The
`GET /stream` request process registers itself for wakeups and then runs the
send loop inline, for the life of the connection.

This is deliberate. In both Bandit and Cowboy the **connection process owns
the socket**; writing chunks from some other process is awkward at best, and
— critically — the socket-level timeouts DMEx relies on to reclaim stalled
connections (see [Stalled connections](#stalled-connections)) act on the
connection, so the process blocked in the write needs to be the one that
dies with it. Making the request process the stream means `chunk/2` works
naturally, the socket closes when the process dies, and the web server's own
supervision covers it.

The loop, per connection — note that `batch_window` and `max_batch_size` are
per-connection values read from the request's `DMEx-Batch-Window` /
`DMEx-Max-Batch-Size` headers (clamped to the producer's bounds, then echoed
back on the response — see [Transport](#transport-sse-over-http)) and carried
in `state`, not compile-time constants:

```elixir
def loop(conn, state) do
  idle? =
    receive do
      :wakeup -> false
    after
      state.heartbeat_ms -> true          # nothing published for a whole interval
    end

  drain_mailbox()                          # coalesce every pending wakeup into one pass
  clear_pending(state)                     # ETS flag, cleared *before* the read

  state =
    case {read_and_send(conn, state), idle?} do   # LIMIT max_batch_size
      {{:sent, state}, _} -> state         # the frame's own bytes are the liveness signal
      {{:empty, state}, true} -> heartbeat(conn, state)   # ": hb" comment frame
      {{:empty, state}, false} -> state    # spurious wakeup; nothing to say
    end

  throttle(state.batch_window)
  loop(conn, state)
end
```

Four properties follow from this shape, and they are the whole reason for
it:

- **Memory per connection is one batch.** There is no accumulator. The
  producer never holds rows on behalf of a consumer; it holds at most the
  frame it is currently writing.
- **Reads are gated on sends completing.** This is the key inversion. Under
  fan-out, read rate is driven by the *publisher's* rate, decoupled from the
  consumer's drain rate — and that decoupling *is* the unbounded buffer.
  Here, the next read cannot happen until the previous write returns, so
  read rate is gated by the consumer's actual drain rate. Backpressure is
  structural rather than monitored: overload is not detected and handled, it
  is unrepresentable.
- **A stalled consumer stalls its own loop and nothing else.** If the
  consumer stops reading, its receive window closes, `chunk/2` blocks, and
  that connection's loop simply stops making progress. It reads no more
  rows, allocates no more memory, and issues no more queries. Only wakeups
  accumulate in its mailbox, and those are bare atoms — bounded further by
  [wakeup coalescing](#wakeup-coalescing).
- **The cursor is the only source of truth.** A wakeup is a hint that work
  may exist, never a carrier of work. Missing one costs latency (bounded by
  the poll interval, see [Live dispatch](#live-dispatch-wakeups)) and never
  correctness, so there is no gap-vs-overlap reconciliation anywhere in the
  design.

The cursor is a **single id: the last one actually sent.** There is no
separate "covered-through" cursor, and no upper bound on the read either.
Every connection issues exactly the same query, with no predicate but the
cursor:

```sql
SELECT id, event_type, data, metadata,
       (SELECT max(id) FROM dmex_messages) AS max_id
  FROM dmex_messages
 WHERE id > $1
 ORDER BY id
 LIMIT $2
```

Two properties of this query are worth being explicit about:

- **A single cursor is unambiguously sufficient**, because with no filtering
  the rows above the cursor are *dense*: the next id is the next row, so
  "resume after the last id sent" and "resume after the last id covered" are
  the same number and there is nothing a second cursor could distinguish. The
  question only becomes live with server-side selection, where a subscription
  sees a sparse subset of one id sequence and re-reading from the last *sent*
  id means scanning id space known to hold nothing for it — a cost an index on
  the selecting column answers, and a decision deferred with the rest of
  [Event-Type Selection](#event-type-selection-post-v1).
- **`max_id` comes back on the same round trip**, as a correlated scalar
  subquery evaluated once per read. On the primary key that is a single
  descent to the rightmost entry. There is no separate latest-id
  query, so a design paying `C / batch_window` extra queries per second
  against the same database taking the writes is avoided. When the read
  returns zero rows the frame is skipped entirely, so the absent `max_id`
  doesn't matter.

Not carrying an upper bound also keeps the read honest with respect to the
wakeup being contentless: the loop reads whatever is there *now*, not
whatever some notification claimed was there when it fired. Reads return at
most `max_batch_size` rows, so a caught-up loop and a loop one million
messages behind issue exactly the same query with exactly the same bound.

The tradeoff, stated plainly: every connection issues its own queries, so
`C` connections at the tip with publish rate `P` cost up to `C × P` reads
per second — reduced by [wakeup coalescing](#wakeup-coalescing) and by
`batch_window`, which caps any one connection at one read per window however
fast the log is being written. In v1 that is the whole of it: with no
server-side selection, every publish is potentially relevant to every
connection, so there is nothing to route wakeups by and `C × P` is the honest
ceiling. Cutting it to per-event-type interest is one of the concrete payoffs
of [Event-Type Selection](#event-type-selection-post-v1), and it is worth
knowing that the lever exists before the ceiling is hit. In practice the
binding constraint is **database connection-pool contention**, not raw query
load.
That is a real ceiling — but it degrades gradually and can be raised with
hardware, traded against a failure mode where one unresponsive client
exhausts the host application's memory. See
[Open Questions](#open-questions).

#### Stalled connections

The loop bounds memory, but it cannot by itself reclaim a connection that
has stopped making progress: a process blocked in `chunk/2` cannot time
itself out, because it is not running. That is left entirely to the socket
layer. **DMEx runs no watcher process and no global connection budget** —
see [the scope note](#what-this-deliberately-does-not-do) below for why, and
for what would change that.

Two transport options, both set on the web server's listener:

- **`{send_timeout, N}` with `{send_timeout_close, true}`** — the primary
  mechanism. `gen_tcp:send` returns `:ok` once data reaches the driver/kernel
  buffer, so the timeout only engages when the output queue is genuinely
  full, which is exactly the stalled-consumer condition: the peer stops
  reading, its receive window closes, the buffer fills, the write blocks,
  and after `N` ms it fails with `{:error, :timeout}` and closes the socket.
  No external observer, no process-state inference, and it fires wherever
  the stall begins — including in the middle of a data frame, which is the
  most likely place and the case a heartbeat-based check structurally
  cannot see (see [Heartbeats](#heartbeats)).
- **`{keepalive, true}`, with `TCP_KEEPIDLE`/`TCP_KEEPINTVL` tuned** — for
  the case `send_timeout` cannot reach. On a low-traffic log a peer can
  vanish without sending FIN or RST (power loss, NAT rebind), leaving an
  idle connection whose only traffic is heartbeats. Those are a few bytes,
  the buffer never fills, every `send` returns `:ok` — so `send_timeout`
  never engages and the producer holds a connection process for a peer that
  no longer exists. TCP keepalive is what detects this, and **the OS default
  is useless here** (two hours on Linux); it must be set explicitly, on the
  order of the heartbeat interval.

A frame **byte cap** — independent of `max_batch_size`, which bounds rows
rather than bytes — belongs alongside these. It keeps any single outstanding
write bounded, so `send_timeout` can be set tightly without risking a false
positive on a legitimately large frame to a slow-but-alive consumer. See
[Open Questions](#open-questions) for sizing.

Both mechanisms terminate the connection at the socket, so the request
process dies with it and nothing else is required.

**Dropping a connection is not a delivery event.** Nothing is lost and
nothing is skipped: the producer holds no consumer state, the consumer's
`last_seen` is durable, and resumption is the same `id > last_seen` read a
cold connect does. It costs a reconnect and a re-read, nothing more. This is
what makes it safe to be aggressive — the log *is* the buffer, and the
producer's in-memory frame is only ever a transient copy of rows that are
already durable.

**There is no in-band notification when a connection is dropped**, and there
cannot be: any such frame would have to be written to the very socket that
is stalled, so it is undeliverable in exactly the case that matters. The
close is the signal. A consumer distinguishes a drop from a clean end of
stream the same way it handles any other transport failure — reconnect from
its durable `last_seen`, with jittered backoff (see
[Transport](#transport-sse-over-http)).

#### What this deliberately does not do

The judgment call, recorded so it can be revisited rather than
rediscovered: **DMEx does not implement global shedding or admission
control.** There is no sweeper process tracking per-connection progress, no
aggregate memory or connection budget, and no `503`/`Retry-After` path that
turns consumers away when a producer is loaded.

The reasoning is that the per-connection mechanisms above already cover the
failure that actually threatens the host application. The loop makes
unbounded memory growth unrepresentable (see
[Producer side](#producer-side)); `send_timeout` and keepalive reclaim
connections that stop making progress. What global machinery would add is
protection against *aggregate* pressure — many individually-healthy
connections adding up to more than the app can afford — and against a class
of stall the socket layer cannot see: a loop wedged for a non-socket reason
(a hung query, a pool checkout that never returns), or a consumer that is
pathologically slow while still technically progressing, which no
per-call timeout can ever fire on.

Those are real, and none of them are addressed *by the producer*. The last of
them is at least approached from the other end — a DMEx consumer that keeps
falling behind re-parameterizes down to a rate it can actually sustain (see
[Backpressure & adaptive batching](#backpressure--adaptive-batching)) instead
of living permanently at the edge of `send_timeout`. But that is voluntary and
consumer-side: it is a well-behaved client being well-behaved, not the producer
defending itself, and it does exactly nothing about a client that declines to
implement it. The bet is that they
matter only past a connection count where a producer app has bigger capacity
questions anyway, and that the machinery is cheaper to add later — one
sweeper process and a counter — than to carry unused. Revisit when
connection counts per producer reach the point where aggregate budget is a
live constraint; that is the same threshold at which admission control
starts being worth its complexity, and the two should be designed together.

### Live dispatch (wakeups)

Once a consumer has caught up to the end of the log, new messages need to
reach it as they're published, not just on the next poll. DMEx borrows the
belt-and-suspenders dispatch pattern from
[Oban](https://github.com/oban-bg/oban) and
[EventStore](https://github.com/commanded/eventstore) rather than inventing
a new one. Both engines run the same per-app `DMEx.Dispatcher` GenServer;
they differ only in whether it also has `LISTEN`/`NOTIFY` available.

- **Postgres**: `publish/3` issues `NOTIFY dmex_<app>` alongside the
  insert, in the same transaction (via `Postgrex.Notifications`, the same
  mechanism `Oban.Notifier` and EventStore's subscriptions use). The
  notification carries an **empty payload** — there is no routing key in v1,
  because there is nothing to route by: every connection reads the whole log
  and so every publish is relevant to all of them. Postgres
  delivers notifications at COMMIT, which is exactly right: a row is never
  announced before it is visible, and — given the per-app append
  serialization from [Append ordering](#append-ordering-id-gaps) —
  notification order follows commit order. `DMEx.Dispatcher` `LISTEN`s and,
  on notification, broadcasts a bare `:wakeup` to every registered stream
  process (see [Producer side](#producer-side)) via a `Registry`.
- **Polling, both engines**: the same `DMEx.Dispatcher` also polls on a fixed
  interval (e.g. every second) regardless of notifications received, exactly
  as Oban's stager polls even with `LISTEN`/`NOTIFY` active. This is what
  makes dispatch *robust*: a missed or coalesced `NOTIFY` (e.g. across a
  connection blip) only costs up to one poll interval of latency, never a
  stuck consumer.
- **SQLite**: no `LISTEN`/`NOTIFY` equivalent exists, so `DMEx.Dispatcher`
  runs polling-only — the same code path Postgres uses as its fallback,
  matching how Oban's SQLite (Lite) engine operates versus its Postgres
  (Basic) engine.

Note what the `Dispatcher` does **not** do: it does not read rows, and it
does not fan out data. Each stream reads for itself, bounded by its own
`max_batch_size`. A notification carries **no payload at all — never a range,
an id, or a row** — because the loop reads from its own cursor to whatever is
there now, so anything in the payload would be information nothing consumes.
The cursor in each stream is the authority; the notification is only ever a
hint that work may exist.

This keeps `DMEx.Dispatcher` to roughly "hold the `LISTEN` connection,
broadcast an atom, run the poll timer" — a few dozen lines, not a component
with its own failure modes. It also means the one shared per-app process
never holds message data on any connection's behalf.

#### Wakeup fan-out

Every publish wakes every connection. With no server-side selection there is
no routing key to scope a wakeup by, so `Dispatcher` broadcasts to all
registered streams and each decides for itself — by reading — whether there is
anything to send. This is the fan-out behind the `C × P` read ceiling in
[Producer side](#producer-side), and routing wakeups by `event_type` is the
first thing [Event-Type Selection](#event-type-selection-post-v1) would buy
back.

One detail this still has to get right, and it matters more without routing:

- **The poll must not wake connections for nothing.** A poll that
  unconditionally wakes every connection re-creates a `C`-per-interval
  thundering herd — at a thousand connections and a one-second interval, a
  thousand reads per second even on a completely idle log. Instead the
  `Dispatcher` polls `SELECT max(id) FROM dmex_messages`, compares it against
  the highest id it has already announced, and broadcasts only if it moved.
  One cheap query per interval — a single index descent, no scan — and zero
  wakeups when nothing was published. So the idle case costs one query per
  interval per *app*, not per connection; it is only a genuinely busy log that
  pays the fan-out.

#### Wakeup coalescing

Wakeups are the last place in the producer where memory could grow without
bound. A stalled connection is blocked in `chunk/2` and therefore cannot
service its mailbox, while publishes keep arriving — so `:wakeup` messages
accumulate at the publish rate for as long as the stall lasts.

`send_timeout` (see [Stalled connections](#stalled-connections)) already
bounds this at `publish_rate × send_timeout` per connection, which is
survivable but not free: a partition that stalls many connections at once
multiplies it by the number of stalled connections, right when the producer
is least able to absorb it. It is also the one bound here that depends on a
tunable being set correctly rather than on structure.

Since a wakeup carries no information, it can be collapsed to **at most one
pending per connection**. Each stream keeps a row in a shared ETS table
carrying a single `pending` flag:

- the `Dispatcher` sets the flag and only actually `send/2`s `:wakeup` if it
  flipped from `false` — so a connection with a wakeup already queued gets
  no further messages, however long it stalls;
- the loop clears the flag **before** reading, not after (see the
  `clear_pending/1` call in the [loop](#producer-side)). A publish landing
  between the clear and the read re-sets the flag and enqueues a wakeup, so
  the loop runs again and performs a possibly-empty read. Spurious wakeups
  are harmless; missed ones would not be, and this ordering makes them
  impossible.

Mailbox depth per connection is then O(1) rather than O(publish rate ×
stall duration), regardless of how long a stall lasts or how many
connections stall together — and, usefully, without depending on
`send_timeout` being tuned correctly.

The `Dispatcher`'s own mailbox gets the same treatment for the same reason:
it drains every pending notification before broadcasting, so a burst of
publishes costs one broadcast pass rather than one per publish.

## Storage Adapters

A common `DMEx.Storage` behaviour is implemented by both a SQLite adapter and
a Postgres adapter (via Ecto), so applications can pick whichever fits their
deployment without changing the Publishing/Consuming APIs. The behaviour
needs to support, at minimum:

- Appending a message and returning its assigned id, under the per-app
  append serialization described in
  [Append ordering](#append-ordering-id-gaps).
- Range-scanning messages with `id > cursor`, in id order, with a `LIMIT` —
  the single read every connection loop issues, whether it is catching up or
  tailing (see [Producer side](#producer-side)). It takes no other predicate,
  so the primary key serves it directly and no secondary index is needed.
- Returning the log's latest `id` **as part of that same read**, as a
  correlated scalar subquery, for stamping `max_id`. This is not a separate
  capability and must not be a separate round trip: it is one descent to the
  rightmost primary-key entry, folded into a query the loop was issuing
  anyway.
- Reading `max(id)` for the `Dispatcher`'s poll (see
  [Wakeup fan-out](#wakeup-fan-out)) — Postgres's fallback path and SQLite's
  only path.

The schema is intentionally minimal so it maps cleanly onto both engines, and
the read surface is deliberately narrower still: three shapes, all of them
keyed on `id` alone. Selective reads — a range scan restricted to a set of
event types — are the capability this behaviour would have to grow for
[Event-Type Selection](#event-type-selection-post-v1), and the one that would
bring an index requirement with it.

The same `DMEx.Storage` behaviour (and the same two adapters) is reused on
the consumer side for its own local bookkeeping — a `last_seen` table keyed by
handler module and the shared `dead_messages` table (see
[Process Architecture](#process-architecture)) — rather than inventing a
second storage abstraction for that half of DMEx.

## Delivery Semantics

- **At-least-once delivery, with duplicate suppression by id.** DMEx drops
  any message whose id it has already committed (see
  [Consuming API](#consuming-api)), so no handler is invoked twice for the
  same id by transport-level re-delivery — a reconnect overlap, a replayed
  frame, a generic client resuming via `Last-Event-ID`. Application code needs
  no dedupe logic and never sees the id.

  **This is not exactly-once, and the gap is worth being precise about.** The
  `handle_message/2` call and the `last_seen` advance share one local
  transaction (see [Consumer side](#consumer-side)), so DMEx's bookkeeping
  cannot drift from the callback. But that commit is also the *only* record
  that a message was handled — so if the process or host dies after a
  handler's side effects have landed and before the transaction commits, the
  message is redelivered with `id > last_seen`, which is indistinguishable
  from never having been processed. Suppression cannot fire, and the effects
  happen again.

  What follows for handler authors: **side effects that go through the
  consumer's own repo are exactly-once for free**, because they roll back with
  the same transaction that would have recorded them. Effects that leave that
  transaction — an HTTP call, a message to another service, a write to a
  different database — must be idempotent on something in the event or the
  publisher's metadata, since the log id is deliberately not exposed. Putting
  external effects in a handler is the case to think twice about; a local
  write plus a separate outbox is the pattern that stays correct.
- **Ordering** is guaranteed only within a single app's stream, never across
  streams.
- **Retention is unbounded by default.** Nothing is deleted as a side effect
  of consumption; a log grows until an operator explicitly prunes/archives
  it (policy TBD — see [Open Questions](#open-questions)).

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

## Event-Type Selection (post-v1)

Server-side selective delivery — a consumer receiving part of a producer's log
rather than all of it — is a real requirement, and this is the shape it is
expected to take. **None of it is in v1.** v1 streams whole logs and filters
consumer-side (see [Consuming API](#consuming-api)); everything in this
section is a design direction, recorded so the v1 decisions above stay
compatible with it and so the reasoning isn't rediscovered later. It is not a
description of shipped behaviour.

**There are no topics** — not as a column (see [Message log](#message-log)),
and not as a separate named mechanic either. A topic is a name standing for a
set of event types, and a name is a second vocabulary somebody has to define,
publish, version, discover, and keep in sync with the types it names. Every
one of those costs is paid to describe something the consumer has already
said.

Because it has. A subscription already declares `event_modules` — the list of
event modules it can decode (see [Event Specifications](#event-specifications))
— and that list is not optional: without it a consumer cannot turn an
`event_type` back into a struct at all. **That list is the selection.** DMEx
maps it through each module's `type/0` and sends the resulting `event_type`
strings as a query parameter on `GET /stream`; the producer adds one predicate
to its read and streams only matching rows. Nothing new is declared anywhere,
by anyone.

### What that buys

- **One list, two jobs, no drift.** A separately-defined grouping can always
  disagree with reality — naming a type the consumer cannot decode, or
  omitting one it can, either of which is a silent bug. A selection *derived
  from* `event_modules` is exactly the set of types the handler is equipped
  for, by construction, and cannot be otherwise.
- **Arbitrary subsets, no coordination.** Any consumer selects any set of
  types; sets overlap freely between consumers and nobody has to agree on
  them. No producer-side curation, and no consumer blocked waiting for a
  producer to define the grouping it happens to need.
- **Evolvable, in the place it is read.** Changing what a subscription
  receives is editing a list in the consumer's own code — the same edit that
  makes it *able* to handle the new type in the first place. An ordinary code
  change, an ordinary deploy, an ordinary reconnect; no remote definition
  changing under a running consumer.
- **The producer stays stateless about consumers.** The selection travels on
  the request, per connection, and nothing about it is stored. The
  ["producer holds nothing"](#consumer) property survives intact — a
  producer-side registry of named groupings would have been the first thing to
  chip at it.

One consequence to know before it bites: **adding a module to `event_modules`
selects that type from the cursor forward, not from the beginning.** The
cursor is a single id and only ever moves forward, so historical rows of the
newly-selected type — everything below `last_seen`, published while the
consumer wasn't asking for it — are not revisited. A consumer that needs that
history has to reset its cursor deliberately. Whether DMEx should offer
anything to make that less manual is open.

### What it would not change

- **Not a partitioning of the log.** There is still exactly one
  totally-ordered log per producer and one id sequence; a selective
  subscription sees a sparse subset of that same id space, so every consumer's
  `last_seen` stays directly comparable with every other's regardless of what
  it selected.
- **Not visible at the publish call site.** `publish/3` keeps its signature
  and writes the same columns, and event modules keep taking no options. A
  publisher does not know, and does not need to know, who is selecting its
  event types.
- **Not a change to delivery semantics.** Selection changes which rows a
  connection is sent, and nothing about ordering, duplicate suppression, or
  the handler/transaction/`last_seen` pipeline.

### Where it would attach

The seams are already in place, and naming them is most of what deferring the
work is meant to protect:

- **Subscribe** — no new option. `event_modules` gains a second role, and
  `DMEx.Consumer.Stream` derives the wire values from it.
- **Transport** — the selected types go in the URL, by the rule already stated
  in [Transport](#transport-sse-over-http): they change which messages exist
  for the subscription, so they are identity, not pacing. Absent means
  everything, so a bare browser `EventSource` keeps working exactly as it does
  in v1. Fixed for a connection's lifetime, for the same reason pacing is: a
  consumer wanting a different set reconnects.
- **Read path** — the loop's query grows one predicate
  (`WHERE event_type = ANY($1) AND id > $2`), which brings back both the
  composite-index requirement (`(event_type, id)`) and the sparse-cursor
  question retired in [Producer side](#producer-side).
- **Wakeups** — the routing key is the published row's own `event_type`, and
  connections register under the types they selected. That is *exact*: a
  publish wakes precisely the connections that asked for that type, with no
  intermediate grouping to be approximate about it. This is the
  read-amplification lever described in [Wakeup fan-out](#wakeup-fan-out).
- **Producer-side validation** — there is none, deliberately. A consumer may
  legitimately name a type the producer has never published (a consumer
  deployed ahead of the producer that will emit it), so the parameter is an
  opaque set of strings compared against a text column: no checking against a
  known-type list, and emphatically no atomizing of consumer-supplied strings,
  which is the same unbounded-atom-table risk the consumer side already
  refuses to take (see [Event Specifications](#event-specifications)). A name
  that matches nothing simply matches nothing.
- **Consumer** — the skip path in [Consuming API](#consuming-api) becomes a
  defence rather than the normal case. A selective stream should only ever
  carry decodable types, so an unknown one now indicates a real mismatch and
  is worth surfacing rather than quietly dropping.

### Open, if and when it lands

- **URL length is the hard one.** `event_type`s are fully-qualified module
  names (`MyApp.Events.OrderPlaced` is 24 characters), so a consumer with
  fifty of them builds a multi-kilobyte query string — against proxies and
  servers that commonly cap request URLs well below what an HTTP client will
  happily send. Note that the obvious escapes are mostly bad: a body on a
  `GET` is not reliably proxied, a `POST`-then-`GET` handshake reintroduces
  exactly the producer-side per-consumer state this design is careful not to
  have, and hashing the set into a short token needs a registry to resolve it
  again. Possible answers worth weighing: accept the cap and document it, fall
  back to whole-log delivery plus consumer-side filtering above some size
  (correct, just less efficient — and it is precisely v1's behaviour, which is
  a comfortable fallback to have), or compress the set into something of fixed
  size, which is the next entry.
- **A fixed-size probabilistic filter is the most promising escape.** Rather
  than enumerating the types, the consumer sends a **Bloom filter** built from
  its `event_type` strings — a few hundred base64url bytes regardless of how
  long the list is — and the producer tests each candidate row's type against
  it. (For a set that never changes once built, which this one does not, a
  static-set structure like a binary fuse or XOR filter is smaller still at
  the same error rate.)

  What makes it fit here is that the error is one-sided, and one-sided in the
  survivable direction: **no false negatives, so every selected type is
  guaranteed to be delivered**, and the entire cost of a false positive is
  that an unwanted row crosses the wire and is dropped. That drop needs no new
  machinery — it is the skip path v1 already specifies for an `event_type`
  with no matching module (see [Consuming API](#consuming-api)), so the
  degraded case of this scheme is exactly the *normal* case of v1, not a new
  failure mode. It also collapses the whole design space onto one mechanism:
  an explicit list is a filter with a 0% false-positive rate and unbounded
  size, sending no selector at all is a filter with a 100% rate and zero size,
  and a consumer picks its point on that line by sizing the filter — spending
  URL bytes to buy bandwidth.

  Four things to pin down:
  - **Whether the producer expands the filter or tests row by row.** A Bloom
    filter cannot be pushed into a SQL index; membership is a computation, not
    something an index can seek on. The producer can sidestep that by
    expanding the filter *once at connect time* against its own compiled event
    modules, turning it straight back into the `event_type = ANY($1)` list the
    read path wants. Testing per row instead leaves the read as v1's
    unfiltered scan with filtering in the application: no index seek, but no
    regression against v1 either — and no expansion to keep current, which the
    next point is about.
  - **Expansion must be over the producer's event modules, not over its log
    contents.** The two sources look interchangeable and are not. Expanding
    over the types the log has actually seen
    (`SELECT DISTINCT event_type ...`) goes stale silently: a producer's code
    can know a type it has not yet emitted, and the first emission of it
    involves no restart and no reconnect, so an open connection keeps an
    expansion missing a type its consumer genuinely selected. Expanding over
    the producer's own compiled event modules has no such gap — that set
    changes only when the producer deploys, and a deploy takes down the web
    server and with it every SSE connection, since the request process *is*
    the stream (see [Producer side](#producer-side)). Every affected consumer
    reconnects and re-expands against the new set, so the repair is forced by
    the same property that makes producer restarts uneventful, with no
    re-expansion machinery to build or get wrong.

    Worth recording the trap, since it is the intuitive reading: that a type
    the producer has only just begun publishing must have been unknown to the
    consumer too, and so could only ever be a false positive. It does not
    hold. The two vocabularies are independent — a consumer deployed ahead of
    the producer legitimately names types the producer has not yet emitted,
    which is exactly why validation was dropped above — so such a type can be
    a true positive, and dropping it would be a real false negative. What
    saves it is the deploy boundary, not the definition.

    The one residue is a rolling multi-node deploy, where a connection pinned
    to a node running old code outlives a node running new code that has begun
    publishing the new type. That belongs to the existing cross-node
    clustering question (see [Open Questions](#open-questions)), not to this
    one.
  - **Wakeup routing.** With expansion, routing works exactly as it does for
    an explicit list. Without it, the `Dispatcher` must test every
    connection's filter against each published type — a few hashes per
    connection per publish, cheap in absolute terms but `O(C)` again, which is
    the amplification the routing existed to remove.
  - **Readability, and therefore whether it should be the only encoding.** An
    explicit list says what a subscription is, in a proxy log, at a glance; a
    base64 blob says nothing and cannot be reversed. That argues for keeping
    both forms and reaching for the filter only above the size where the list
    stops fitting.

  One thing it does settle: filter size, hash count, and seed must agree
  exactly on both sides, so they should be **fixed by DMEx rather than
  negotiated**. A mismatch would reintroduce false *negatives* — the one error
  this scheme is chosen for not having — and would do it silently.
- **Wire form and URL stability** — repeated `event_type=` parameters or one
  delimited list, and whether the set is canonically sorted so a given
  subscription produces a byte-identical URL on every reconnect. That matters
  for the same reason pacing was kept out of the URL: one stable URL per
  subscription groups cleanly in proxy logs, access logs, and metrics. A
  filter encoding gets this property for free, being deterministic in the set
  and the fixed parameters.
- **Whether selection should be narrowable below `event_modules`** — a
  consumer that can *decode* twenty types but only *wants* five would
  otherwise have to receive fifteen it discards, or split into two handlers
  and therefore two cursors. Does `subscribe/2` want an explicit override, or
  is "your module list is your subscription" the whole rule? The rule is
  simpler; the override is the escape hatch for a handler whose decode
  vocabulary is genuinely wider than its interest.
- **Second cursor, again** — with a sparse subset the last-*sent* id no longer
  implies the rows in between were examined and rejected. Whether that needs a
  second cursor or is answered entirely by the `(event_type, id)` index (an
  index seek costing the same whether the next matching row is one id away or
  ten million) has to be settled with the index, not before it.
- **Non-Elixir consumers** — the selector is a list of Elixir module names, so
  a consumer in another language must know those strings out-of-band. That is
  the same coupling as
  [sharing event modules across independent applications](#open-questions),
  now showing up in the URL as well as in the decode step, and it strengthens
  the case for the schema-endpoint option discussed there.

## Open Questions

Things intentionally left open for the next round of discussion.

- **Auth / access control** — who's allowed to read a given producer's SSE
  endpoint, and how is that enforced?
- **Consumer-side schema** — the producer's table is pinned down (see
  [Message log](#message-log)) and `last_seen`'s key is settled (the handler
  module, see [Consuming API](#consuming-api)), but the DDL for both consumer
  tables is not written yet. Two specifics: since a handler rename resets a
  cursor and replays a log from scratch, is there a supported way to migrate
  the row — an operator-facing rename helper, or just "write the migration
  yourself"? And `dead_messages` has its columns listed in
  [Consumer side](#consumer-side) but not its keys or indexes, including
  whether `(producer_url, id)` should be unique so a replay tool can't insert
  the same dead message twice.
- **Is an `event_type` index worth its write cost?** No v1 read path uses
  one — every producer read is a bare `id > cursor` range scan (see
  [Producer side](#producer-side)) — so in v1 an index on `event_type` exists
  purely for operators querying the log by hand. It is not free: appends are
  already serialized per app (see
  [Append ordering](#append-ordering-id-gaps)), so index maintenance sits
  directly on the one write path that cannot be parallelized away. Open: ship
  without it and let operators add one, or include it because ad-hoc log
  inspection is a first-class use case? Note this stops being a judgement call
  the moment [Event-Type Selection](#event-type-selection-post-v1) lands — a
  selective read filters on `event_type`, so `(event_type, id)` becomes a
  required index rather than an operator convenience.
- **Payload byte fidelity** — `data` and `metadata` are `jsonb` (see
  [Message log](#message-log)), chosen for in-database querying of payloads,
  which is a first-class use case for a log people inspect by hand. The
  accepted cost is that `jsonb` normalizes: key order is not preserved,
  duplicate keys are dropped, insignificant whitespace is lost. Because the
  wire envelope is re-serialized from storage (see
  [Message Envelope](#message-envelope)), the bytes a consumer receives are
  therefore not guaranteed to be the bytes the publisher encoded. Open: does
  this need documenting as a hard caveat — no signing or hashing of raw
  payload bytes end-to-end — or does some future use case justify a
  side-by-side exact-bytes column?
- **`batch_window`/`max_batch_size` defaults & bounds** — what are DMEx's
  own defaults when a consumer doesn't specify the
  `DMEx-Batch-Window`/`DMEx-Max-Batch-Size` headers, and what are the
  producer-side sanity limits (a floor on `batch_window`, a ceiling on
  `max_batch_size`) that a request is clamped to? The floor matters especially,
  since `batch_window` divides directly into per-connection query rate. This is
  more load-bearing now that the values are negotiated rather than merely
  configured: the defaults are what every browser `EventSource` gets
  permanently, and the bounds are the range an adaptive consumer's control loop
  operates inside (see
  [Backpressure & adaptive batching](#backpressure--adaptive-batching)). A
  floor set too high leaves a slow consumer nothing to ask for and puts it back
  on the `send_timeout` path the adaptation exists to avoid.
- **Adaptive control-law constants** — the shape is settled (AIMD over
  reconnects, see
  [Backpressure & adaptive batching](#backpressure--adaptive-batching)); the
  numbers are not. What are the high- and low-water marks as fractions of
  `queue_limit`, the EWMA window for drain rate, the multiplicative
  back-off and additive recovery steps, the number of quiet windows before
  recovery starts, and the minimum interval between re-parameterizations? And
  what is a sensible default `queue_limit` — large enough that drain rate is
  measurable over a batch or two, small enough that the memory is
  uninteresting. Related: should the queue also be bounded in *bytes*, for the
  same reason the producer wants a frame byte cap (see below)? A `queue_limit`
  in rows says nothing about memory when payloads vary by three orders of
  magnitude.
- **Adaptation-induced reconnect churn** — every re-parameterization is a
  reconnect, and a reconnect costs the producer a fresh `id > last_seen`
  read. Hysteresis and a minimum re-parameterization interval bound this per
  consumer, but there is no aggregate bound, and there is deliberately no
  admission control to absorb one (see
  [What this deliberately does not do](#what-this-deliberately-does-not-do)).
  Open: is a correlated event — a producer deploy that briefly slows every
  consumer, so all of them re-parameterize together — actually damped enough by
  jitter alone, and should the adaptation be rate-limited over a longer window
  (e.g. at most N re-parameterizations per hour before it gives up and just
  runs behind)?
- **Read amplification ceiling** — each connection issuing its own reads
  (see [Producer side](#producer-side)) trades bounded memory for query
  volume. In v1 every connection sees every publish (see
  [Wakeup fan-out](#wakeup-fan-out)) — routing wakeups by interest is deferred
  with [Event-Type Selection](#event-type-selection-post-v1) — so the ceiling
  is the
  full `C × P`, and the practical limit is DB connection-pool contention.
  Open: what is the realistic connection ceiling per producer app, does the
  pool need partitioning so stream reads can't starve publish writes, and is a
  shared cache of recent rows at the tip ever worth the shared mutable state
  it reintroduces? This subsumes the older "multi-consumer fan-out"
  question: many consumers reading one stream independently already works
  (each tracks its own `last_seen` and gets its own connection process), so
  the only remaining limit is read load, not memory.
- **Frame byte cap** — capping frame size matters for stall detection, not
  just wire format (see [Stalled connections](#stalled-connections)): it
  bounds any single outstanding write, so `send_timeout` can be tight
  without false-positiving on a large frame to a slow-but-alive consumer.
  Open: what is the cap, how does it interact with `max_batch_size` when a
  batch of large payloads hits it first (truncate the frame and continue on
  the next iteration, presumably), and how does it relate to the chosen
  `send_timeout`?
- **Timeout defaults** — values for the heartbeat interval, `send_timeout`,
  and the keepalive idle/interval settings, and how the three should relate
  to each other and to typical proxy idle timeouts. Too tight a
  `send_timeout` drops healthy slow consumers and, with no admission control
  to absorb the reconnects, that is a storm with nothing damping it. Two
  knock-on decisions now that `retry:` carries the effective heartbeat
  interval (see [Heartbeats](#heartbeats)): what `k` is — how many missed
  intervals before a DMEx consumer gives up on a silent connection — and how
  much jitter the client applies around an advertised value every consumer
  receives identically.
- **Revisiting global shedding** — deliberately deferred (see
  [What this deliberately does not do](#what-this-deliberately-does-not-do)).
  What signal should trigger reopening it: a connection-count threshold, an
  observed incident of a non-socket wedge, or memory pressure that the
  per-connection bounds demonstrably failed to contain? Worth deciding the
  tripwire now rather than rediscovering the need in production.
- **Retention / compaction policy** — logs grow forever by default; what
  operator-facing controls exist for archiving or pruning old messages, and
  how does that interact with "arbitrarily replayable"?
- **Server-side selective delivery** — deliberately out of scope for v1, with
  its intended shape (the consumer's `event_modules` list on the wire as an
  `event_type` selector) and its own open questions written up in
  [Event-Type Selection](#event-type-selection-post-v1). The
  question that belongs *here* is when to build it: shipping v1 without it
  means every consumer pays for every message its producer publishes, in
  bandwidth and in a decode-and-discard pass, and every producer pays `C × P`
  reads (see [Producer side](#producer-side)). Open: is the trigger a measured
  one — bandwidth on a fat log, read load at a connection count — or is it
  simply the first non-trivial adopter with a log whose consumers each want a
  slice?
- **Append serialization cost** — per-app advisory-locked appends (see
  [Append ordering](#append-ordering-id-gaps)) make the reader cursor sound
  but serialize all publishes for an app, and a publish inside a long
  enclosing transaction extends that serialization for the transaction's
  lifetime. Open: what publish throughput does that actually cap out at, how
  should callers be warned off publishing inside long transactions, and is it
  ever worth revisiting the rejected alternatives — reading only up to a
  snapshot watermark (`pg_snapshot_xmin(pg_current_snapshot())`), which keeps
  concurrent writes but adds a non-obvious concept to every read path, or
  serving only rows older than *k* ms, which is simple, probabilistic, and
  not actually correct?
- **Consumer-side socket mode enforcement** — the *structural* backpressure
  property depends on the consumer reading `active: :once` and re-arming only
  with queue headroom, so that TCP pressure is real (see
  [Consuming API](#consuming-api)). DMEx's own client does this, but the doc
  also advertises that any SSE-capable client can consume a stream. Adaptive
  batching (see
  [Backpressure & adaptive batching](#backpressure--adaptive-batching)) gives
  an eager third-party client a *cooperative* substitute that needs only two
  request headers, which narrows the question but does not close it: a client
  that implements neither still reads at full rate into unbounded memory of its
  own, and the producer cannot tell the difference — the socket drains fast
  either way. Open: is "your client must bound something, either its socket or
  its queue" a documented compatibility caveat, or does the producer want a
  detectable signal (a consumer that never slows down and never
  re-parameterizes is indistinguishable from a genuinely fast one, which may
  simply be fine)?
- **`:crash` vs supervisor restart intensity** — `:crash` is specified to
  crash-loop on a poison message forever, but `DynamicSupervisor` defaults
  to `max_restarts: 3, max_seconds: 5`. A tight crash-loop blows that budget
  in under a second and takes down `DMEx.Consumer.Supervisor` — and with it
  **every other subscription in the consumer app**, including healthy ones
  reading unrelated producers. Open: what restart intensity actually expresses
  "crash-loop forever on this one subscription without killing its
  siblings"? Related: each restart reopens the connection and triggers a
  fresh producer-side read, so an un-backed-off crash-loop hammers a
  *different application's* database — reconnect backoff (see
  [Transport](#transport-sse-over-http)) bounds this, but the interaction
  needs stating explicitly.
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
- **Batch framing shape** — the always-batch decision and the
  `{"metadata": ..., "messages": [...]}` object are settled (see
  [Message Envelope](#message-envelope)); how that object is laid across the
  wire is not. A single `data:` line holding the whole JSON object is the
  simple option; SSE's multi-line-per-field convention would let the frame
  metadata and each message sit on their own `data:` line, which is far more
  readable in a `curl` session and in proxy logs, at the cost of the
  consumer's parser having to rejoin lines before decoding.
- **Event schema versioning** — each event type has a dedicated Elixir
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
  its hands on the producer's event modules?

  One constraint to carry into this decision: dropping `origin` (see
  [Producer](#producer)) rests on `event_type` being app-prefixed, so that
  `MyApp.Events.OrderPlaced` identifies its publisher. An org-scoped shared
  package (`MyOrg.Events.OrderPlaced`, published once and depended on by
  several producer apps) breaks that — the same `event_type` could then be
  published by more than one application, and "which app published this"
  would no longer be derivable from the wire. Any option below that leads to
  shared, non-app-prefixed module names reopens the `origin` question.

  A few options worth weighing against each other:
  - **Shared library/package** — the producer app publishes its event
    modules as a small, dependency-light package (private Hex package or git
    dep) that both sides add as a dependency. Keeps the "one Elixir module is
    the schema" model intact end to end; closest to how
    [Commanded](https://github.com/commanded/commanded)-based systems
    typically share event structs. Costs: a real versioned-release/publish
    workflow, and it's Elixir-only — no path for a non-Elixir consumer.
  - **Self-describing producer / schema endpoint** — alongside the SSE
    endpoint, the producer exposes something like
    `GET /stream/schema` listing its known `event_type`s and field
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
  `event_modules` list by hand per `subscribe/2` call; is there a
  lighter-weight registration mechanism (e.g. an application-level
  config/registry) worth adding once there are many event types?
- **Poll interval tuning** — what's the default `DMEx.Dispatcher` poll
  interval, and is it configurable per app/stream, matching the
  latency/DB-load tradeoff Oban exposes via its stager?
- **Cross-node clustering** — if a producer application itself runs as a
  multi-node OTP cluster, how is "the" append-only log, and the
  `LISTEN`/`NOTIFY`-based `DMEx.Dispatcher`, kept single-writer consistent
  across nodes?
