# DMEx — Architecture

> Status: working draft. The core mechanics below are agreed on; see
> [Open Questions](#open-questions) for what is still undecided, and
> [Message-Type Selection](#message-type-selection-post-v1) for the one mechanic
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
A consumer that cares about only some message types declares the modules it
knows and ignores everything else locally (see
[Consuming API](#consuming-api)). Server-side selection is a real requirement
and it has a design — the consumer's own `message_modules` list, sent as a query
parameter and applied as a predicate on the producer's read — but it is
deliberately not in v1; see
[Message-Type Selection](#message-type-selection-post-v1).

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
  for one Elixir module (and struct) per message type, owned by the publishing
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
  message_type  text        NOT NULL,
  data        jsonb       NOT NULL,           -- SQLite: text CHECK (json_valid(data))
  metadata    jsonb       NOT NULL DEFAULT '{}',  -- SQLite: text CHECK (json_valid(metadata))
  inserted_at timestamptz NOT NULL DEFAULT now()
);
```

- `id` — a monotonically increasing offset, and the primary means of ordering
  and resumption. **`bigint`, not `int`**: a log nothing deletes from
  outgrows 2³¹. On SQLite, `AUTOINCREMENT` rather than a bare
  `INTEGER PRIMARY KEY`, so ids stay monotonic even after a retention policy
  (see [Open Questions](#open-questions)) has deleted the tail — a plain
  rowid can be reused, which would break every consumer cursor at once.
- `message_type` — the type string of the message module the row was published
  from (see [Message Specifications](#message-specifications)). It is the only
  classification a row carries, and it describes what the message *is*, never
  who might want to read it (see below).
- `data` — the JSON-encoded message struct. `jsonb` on Postgres, so operators
  can query inside payloads directly; the accepted cost is that `jsonb`
  normalizes key order and drops duplicate keys, so stored bytes are not
  byte-identical to what the publisher encoded (see
  [Open Questions](#open-questions)). SQLite has no `jsonb` column type —
  `text`, with its JSON functions available over it — and a
  `CHECK (json_valid(...))` constraint, since the producer splices this text
  straight into the wire frame without parsing it (see
  [Frame encoding](#frame-encoding)) and a malformed row would corrupt the
  frame around it rather than failing on its own.
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
whether `message_type` deserves one anyway.

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

So the log records only what a message is (`message_type`), never who might want
it. Selective delivery becomes a separate mechanic resolved at read time,
where the interested party names the message types it wants and the log is not
asked to have anticipated the grouping. That mechanic is sketched but
deliberately **not in v1** — see
[Message-Type Selection](#message-type-selection-post-v1), which also explains why
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
`use DMEx.Producer` in `MyApp.Producer`), the same pattern as message modules
(see [Message Specifications](#message-specifications)). That module is what gets
passed as the first argument to `DMEx.Publisher.publish/3`, and it is the
handle for everything append-time that isn't the message itself: which storage
adapter and repo the insert goes through, and the app key that scopes the
[append lock](#append-ordering-id-gaps) and the `dmex_<app>` notification
channel (see [Live dispatch](#live-dispatch-wakeups)).

Note what it deliberately does *not* do: stamp the publisher's identity into
the message. **Nothing records "which producer published this," because
nothing needs to.** `message_type` is the message module's fully-qualified name
(see [Message Specifications](#message-specifications)), so
`MyApp.Messages.OrderPlaced` already says which application it came from, on
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

`DMEx.Frame` is the **one generic container module** every message flows
through, regardless of which application or message type produced it. It is a
direct, minimal mapping onto the
[SSE event stream format](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events#event_stream_format):

| `DMEx.Frame` field | SSE wire field | Meaning |
| --- | --- | --- |
| `id` | `id:` | The highest message id contained in this frame (see below). DMEx's own client ignores it, resuming from its durable `last_seen` instead, but it is what a generic SSE client echoes back as `Last-Event-ID` — the same header DMEx's client uses (see [Transport](#transport-sse-over-http)), so both resume through one mechanism. |
| `event` | `event:` | A **transport-level** framing type, not the application's message type. Always `batch` for data frames — there is no single-message frame type (see [Batching & framing](#batching--framing-batch_window-max_batch_size)). This is the only field DMEx itself ever branches on when parsing a chunk. |
| `data` | `data:` | A JSON **object**, `{"metadata": {...}, "messages": [...]}`: frame-level metadata plus `messages`, a list of **serialized log rows** exactly as stored — `id`, `message_type`, `data`, `metadata`, `inserted_at` (see [Message log](#message-log)). **The application's message type lives here, nested under `data`, not in `event:`** (see [Message Specifications](#message-specifications)). |
| `retry` | `retry:` | The producer's effective heartbeat interval, in ms — a spec-compliant reconnect delay for a generic client, and the pacing signal a DMEx consumer measures silence against (see [Heartbeats](#heartbeats)). Not used for *resumption*, which is driven by `last_seen`. |

SSE's `event:` field carries DMEx's own framing type; the application's
`message_type` lives inside `data`. Keeping them separate lets `event:` stay a
small, fixed vocabulary DMEx controls, while the open-ended, ever-growing set
of application message types sits where it can neither collide with nor
constrain how the transport layer evolves.

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
  stored (`id`, `message_type`, `data`, `metadata`, `inserted_at` — see
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
[duplicate suppression](#consuming-api), `message_type` selects the module to
decode into — and, since a v1 subscription is the producer's whole log,
decides whether the row is delivered to the handler at all (see
[Consuming API](#consuming-api)). Neither reaches application code — a handler
receives its decoded struct and the row's `metadata`, nothing else, so no
consumer ever has to know the log row exists.

`DMEx.Frame` models a **frame**, not a single message: one struct per SSE
chunk, holding the frame's metadata and the list of messages it carries. It
owns exactly two responsibilities: encoding itself to a spec-compliant
`text/event-stream` chunk, and parsing a chunk back into a struct — decoding
`data` one level (into frame metadata plus the list of rows) is as far as it
goes. It knows nothing about any particular message's *shape*; turning a row's
`data` into a concrete struct is the message module's job, and the frame struct
itself never reaches application code — a handler sees its own message and that
row's metadata, never the frame (see
[Consuming API](#consuming-api)). This split keeps the wire format
concern (this module) fully decoupled from the domain payload concern (message
modules, next section).

## Message Specifications

Rather than publishing bare maps, each message a producer emits is defined as
**its own Elixir module** — one struct per message type, owned by the
publishing application — the same shape as
[Commanded](https://github.com/commanded/commanded) message modules:

```elixir
defmodule MyApp.Messages.OrderPlaced do
  use DMEx.Message

  @derive JSON.Encoder
  defstruct [:order_id, :customer_id, :total]
end
```

- There is no `type:` option. `message_type` is **derived automatically from
  the message module's own full, fully-qualified name** (e.g.
  `"MyApp.Messages.OrderPlaced"`, via `inspect(__MODULE__)`) at compile time,
  via `use DMEx.Message`. The full name — not just the last segment — is what
  guarantees `message_type` uniqueness across every producer app in the
  network: two unrelated apps can each have their own `Messages.OrderPlaced`
  submodule without their wire types colliding. Module naming is the single
  source of truth for wire type; it can't drift from what the module is
  actually called.
- **`use DMEx.Message` takes no options at all.** It gives the module two
  callbacks and nothing else: `type/0`, which `publish/3` reads when appending
  — where
  `message_type` is the column that then travels inside each row of the
  `DMEx.Frame` [data envelope](#message-envelope), never
  `DMEx.Frame.event` itself, which stays transport-level (see above). In
  particular there is no `topic:` option, and no delivery-grouping option of
  any other name: a message module declares what the message *is*, and nothing
  about who should receive it. Who receives it is the *consumer's* statement to
  make, and it already makes it — see
  [Message-Type Selection](#message-type-selection-post-v1).

  The second callback is **`cast/1`**, the read-side inverse: it turns a row's
  already-JSON-decoded `data` map into the struct. It is generated with a
  default that maps string keys onto the module's own declared fields and
  **resolves those keys against existing atoms only** — the same
  unbounded-atom-table refusal the module list below rests on — and it is
  **overridable**, because the default can only be right for what JSON models
  directly. A struct field holding a `DateTime`, a `Decimal`, or a nested
  struct round-trips to a string or a plain map and needs a `cast/1` of its
  own; scalars need nothing. This mirrors `@derive JSON.Encoder` on the
  writing side: a default that covers the common shape, replaceable where the
  type demands it.
- The struct fields are the message's full, versioned shape. Evolving a
  message's shape is scoped to editing that one module (see
  [Open Questions](#open-questions) on schema versioning).
- On the read side, a consumer must be able to turn `message_type` back into
  the same struct. DMEx does this via an explicit module list/registry
  supplied by the consumer (e.g. `message_modules: [MyApp.Messages.OrderPlaced, ...]`)
  rather than dynamically atomizing whatever string the producer sent —
  turning arbitrary attacker-controlled strings into atoms is an
  unbounded-atom-table risk, so the mapping is always consumer-declared, not
  inferred. In v1 that list does double duty: because a subscription is the
  producer's entire log, `message_modules` is also the consumer's *filter* (see
  [Consuming API](#consuming-api)) — and it is the same list that would later
  become the wire-level selector sent to the producer, which is why no
  separate grouping concept is needed anywhere (see
  [Message-Type Selection](#message-type-selection-post-v1)).

## Publishing API

Sketch, in `DMEx` namespace — `publish(producer, message, opts \\ [])`, so both
`publish/2` and `publish/3` exist; this document says `publish/3` throughout:

```elixir
DMEx.Publisher.publish(MyApp.Producer, %MyApp.Messages.OrderPlaced{...}, metadata: %{})
```

From the API's perspective, **publishing a message is just writing a row to
the database** — nothing more. `publish/3`:

- serializes the message struct with Elixir's native `JSON` module
  (`JSON.encode!/1`, via `@derive JSON.Encoder` on the message struct);
- reads `message_type` off the struct's own module (`OrderPlaced.type/0` — never
  passed as a call argument);
- passes `metadata` through untouched — the producer module is not recorded
  in it, since `message_type` is fully qualified and already identifies the
  publishing application (see [Producer](#producer));
- does a single local Ecto `insert` (`message_type`, `data`, `metadata`) via
  the configured storage adapter, and returns the assigned `id` (offset) of
  the row.

Note that `message_type` is its own column here (see
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
  def handle_message(%MyApp.Messages.OrderPlaced{} = message, metadata) do
    # side effects go here; raise (or let something raise) to signal failure
    :ok
  end
end
```

```elixir
DMEx.Consumer.subscribe(producer_url,
  message_modules: [MyApp.Messages.OrderPlaced],
  handler: MyApp.OrderHandler,
  start: :beginning,       # :beginning (default) | :latest | <id>; first run only
  on_failure: :crash,      # :crash | :skip | :dead_message
  batch_window: 200,       # ms; preferred minimum time between frames
  max_batch_size: 500,     # rows; preferred maximum messages per frame
  queue_limit: 1_000,      # rows; bound on the internal queue (2 × max_batch_size)
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
  producer's whole log, and `message_modules` is what narrows it, locally (see
  [Message-Type Selection](#message-type-selection-post-v1) for how that same list
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
  (`DMEx.Frame` does that one level of JSON decoding, see
  [Message Envelope](#message-envelope)) and, for each row: looks up
  `message_type` (**not** the SSE `event:` field) against the supplied
  `message_modules`, decodes the row's `data` into that module's struct, then
  invokes `handler.handle_message/2` with that struct and the row's
  `metadata`. **Picking the handler's arguments out of the row is the
  consumer's job**, and the only place it happens — the producer ships rows
  verbatim (see [Message Envelope](#message-envelope)), so this is the single
  point where the log's shape is translated into the application's.
- **An `message_type` with no matching module is skipped, not an error.** This
  is the direct consequence of v1 having no server-side selection: a
  subscription receives the producer's entire log, so rows the consumer has no
  module for are the normal case, not a fault. Such a row advances `last_seen`
  exactly as a handled one does — it must, or a consumer would wedge on the
  first message type it doesn't know — and invokes nothing. `message_modules` is
  therefore the subscription's filter, applied on the consumer after the bytes
  have already crossed the wire; the deferred selection mechanic moves that
  same list to the producer so they never cross it (see
  [Message-Type Selection](#message-type-selection-post-v1)). Skips are worth a
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
- **The handler sees its message and the publisher's metadata — nothing else.**
  `handle_message/2` takes the decoded struct and the row's `metadata` map.
  Everything else in the row stays with DMEx: `id` (resumption and
  duplicate suppression), `message_type` (which module to decode into),
  `max_id`, the frame's `id:`, the `DMEx.Frame` struct. None of it is a
  property of the *message*, and none of it is something a consumer should have
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

- A consumer opens `GET /stream` via Plug — mounted with
  `forward "/stream", to: DMEx.Producer.Plug, init_opts: [producer: MyApp.Producer]`
  from a Phoenix router, a `Plug.Router`, or a bare Bandit/Cowboy pipeline —
  sending its resume
  position in the **`Last-Event-ID` request header** and its delivery pacing
  in the **`DMEx-Batch-Window` and `DMEx-Max-Batch-Size` request headers**. The
  route is singular and carries no application segment, and in v1 no query
  string either: a producer application owns exactly one log (see
  [Message log](#message-log)) and v1 streams all of it, so there is nothing
  for a path or query parameter to select between, and the app's own hostname
  already identifies the app. (The post-v1 `filter` parameter is the one thing
  that will ever hang off this URL — see
  [Message-Type Selection](#message-type-selection-post-v1).) Both pacing headers
  are optional and fall back to DMEx-supplied defaults (see
  [Batching & framing](#batching--framing-batch_window-max_batch_size)).
- **The URL says what the subscription *is*; headers say how it is *paced*.**
  That is the line the two mechanisms fall on, and it is why `batch_window`
  and `max_batch_size` are headers rather than query parameters. In v1 the URL
  side of that line is empty — one producer, one log, one stream, so there is
  nothing to select between — but it is where the post-v1 message-type selector
  lands, since which types a subscription carries changes *which* messages
  exist for it at all (see
  [Message-Type Selection](#message-type-selection-post-v1)). The pacing values
  change nothing about *which* rows arrive or in what
  order, only how they are cut into frames; two connections differing only in
  `batch_window` deliver the same messages in the same order, grouped
  differently. Keeping them out of the URL means one stable URL per
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
  library) has full access to both. Note the limitation stops there: the
  post-v1 message-type selector is a query parameter precisely so that a browser
  client can still say what it wants to receive (see
  [Message-Type Selection](#message-type-selection-post-v1)).
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

### Frame encoding

A data frame, in full — one `id:`, one `event:`, one `data:`, terminated by a
blank line:

```
id: 1042
event: batch
data: {"metadata":{"max_id":1180},"messages":[{"id":1041,"message_type":"MyApp.Messages.OrderPlaced","data":{"order_id":7,"total":1999},"metadata":{},"inserted_at":"2026-07-27T10:31:04.221Z"},{"id":1042,"message_type":"MyApp.Messages.OrderShipped","data":{"order_id":7},"metadata":{"trace_id":"abc"},"inserted_at":"2026-07-27T10:31:04.884Z"}]}

```

`retry:` travels in a frame of its own at stream open (see
[Heartbeats](#heartbeats)), which is well-defined and dispatches nothing: the
SSE parser applies a `retry:` field the moment it parses the line, while an
event is only dispatched if the data buffer is non-empty when the blank line
arrives. Heartbeats are comment frames (`: hb`) on the same principle.

**One `data:` line, not one per message.** SSE joins consecutive `data:` lines
with LF into a single payload, so the choice is really about what that joined
payload is. With one line it is exactly a JSON document: a client hands
`event.data` to its JSON parser and is finished, in any language, with no
DMEx-specific rule to learn. Putting the frame metadata and each row on its own
`data:` line — the readable-in-`curl` option — would make the joined payload
NDJSON instead, which is a second framing layer *inside* the frame that DMEx
would have to specify and every client would have to implement before it could
parse anything at all. That is a poor trade against a format whose selling point
is that any SSE-capable HTTP client can consume it (see
[Transport](#transport-sse-over-http)): `curl` readability is recoverable with a
`jq` in the pipe, and a bespoke sub-format is not recoverable at all.

The honest cost is that a consumer must buffer a whole frame before it can parse
any of it, where per-line framing would let it parse row by row as bytes arrive.
That is bounded by the frame byte cap (see
[Stalled connections](#stalled-connections)), which the consumer enforces on its
own side as well — see [unbatching](#unbatching-on-the-consumer) below.

**No base64, and none is needed.** The only reason to encode a payload here
would be that `data:` is line-delimited, so a raw LF or CR would end the field
and split the frame. JSON cannot produce one: the grammar requires U+0000–U+001F
to be escaped inside strings, so a compact `JSON.encode!` result is newline-free
by construction *of the format*, not by a choice the encoder makes and could
un-make. (U+2028/U+2029 are not SSE line terminators either — the spec's
terminators are CR, LF and CRLF only — so they need no special handling despite
being a perennial hazard in JavaScript contexts.) Base64 would cost a third more
bytes on every message, make the stream opaque to `curl`, `jq` and proxy logs,
and buy nothing.

Two invariants keep that true, and they are the whole of the encoding contract:
the frame is **never pretty-printed**, and nothing is spliced into it that did
not come out of a JSON encoder or a JSON-typed column.

**Rows are spliced, never re-encoded.** The frame is built as iodata — a fixed
prefix, the rows joined by commas, a fixed suffix — and each row's `data` and
`metadata` go in as **text**, exactly as the read returned them (`data::text` on
Postgres; already `text` on SQLite, see [Message log](#message-log)). The
alternative is to let the driver decode `jsonb` into an Elixir map and re-encode
it, which pays a decode plus an encode per row — and, because every connection
issues its own read (see [Producer side](#producer-side)), pays it once per
connection for the same rows. Splicing makes the producer's per-row work a
memcpy, which is what "serialize what the read returned" (see
[Message Envelope](#message-envelope)) has to mean if it is to mean anything.

This rests on the stored text being valid JSON. Postgres's column type
guarantees it; SQLite's `text` does not, so the DDL carries
`CHECK (json_valid(...))` on both columns (see [Message log](#message-log)) —
worth having regardless, and load-bearing here, since a malformed row would
otherwise corrupt the frame around it rather than failing on its own. It is also
another face of the [payload byte fidelity](#open-questions) question: what gets
spliced is `jsonb`'s normalized rendering, not the publisher's original bytes.

#### Compression

Negotiated the ordinary HTTP way — the consumer sends `Accept-Encoding: gzip`,
the producer answers with `Content-Encoding: gzip` and compresses the chunk
stream. A browser `EventSource` gets it for free, which is notable given how
little else it can negotiate (see [Transport](#transport-sse-over-http)).

It pays unusually well here, and **batching is why**. A frame holds up to
`max_batch_size` rows with identical key names, a small set of repeating
`message_type` strings, and near-identical timestamps — the redundancy is
*within* the frame, so it is available to the compressor immediately rather than
depending on cross-frame dictionary state. Framing and compression compound
rather than merely coexisting.

Three things have to be got right, and the third is the one that decides the
default:

- **It must flush per frame.** Left to itself, deflate buffers until it has a
  block worth emitting, which would hold a frame on the wire until the *next*
  frame pushed it out — unbounded added latency on a quiet stream, and a
  heartbeat that arrives only when it is no longer needed, which is precisely
  the failure heartbeats exist to prevent. Every `chunk/2` must end in a sync
  flush. If the web server applies compression automatically, that behaviour has
  to be verified rather than assumed; if it does not flush per chunk, DMEx
  compresses and sets the header itself.
- **Flushing costs ratio, and that is the trade being made.** A sync flush ends
  the block and emits a few bytes of overhead, and small frames compress worse
  than large ones. A consumer that has adapted down to tiny batches is therefore
  also getting worse compression — the two degrade together, which is the right
  direction but worth knowing.
- **Per-connection compressor state is the catch, and it lands squarely on the
  property the producer loop was built to have.** A zlib deflate stream at
  defaults (`windowBits` 15, `memLevel` 8) is on the order of 256 KB of state
  held for the connection's entire life. At a thousand connections that is
  ~256 MB — dwarfing "memory per connection is one batch" (see
  [Producer side](#producer-side)) and quietly reintroducing the unbounded-growth
  class of failure that section exists to make unrepresentable. Smaller
  parameters (`windowBits` 12, `memLevel` 4, ~32 KB) trade ratio for footprint
  and are the sane operating point.

So compression is **producer-side opt-in with explicitly-sized zlib parameters**,
not on by default: it is the one feature in the design whose cost is paid in the
same currency the producer's memory bound is denominated in. The consumer side
carries an inflate stream too, but one per subscription rather than one per
connection served, so it is not the same question.

Worth recording rather than resolving: compressing a response that mixes
attacker-influenced content with content the attacker should not learn leaks
through length (the BREACH/CRIME family). The setting here is unlike the classic
one — no per-request reflection, no secret to extract token-by-token — and it
requires an adversary who can both publish into a producer's log and observe the
sizes of someone else's stream. That is a question about who may publish and who
may read, which belongs with [auth](#open-questions).

#### Response headers and intermediaries

Beyond the pacing echo (`DMEx-Batch-Window` / `DMEx-Max-Batch-Size`, see
[Transport](#transport-sse-over-http)), the `200` carries the ordinary SSE set —
`Content-Type: text/event-stream` (always UTF-8; the charset parameter is
ignored by the spec), `Cache-Control: no-cache`, and `Connection: keep-alive` on
HTTP/1.1 — plus one that is not standard and is not optional in practice:

- **`X-Accel-Buffering: no`.** nginx proxies with `proxy_buffering on` by
  default, which holds response bytes until its buffer fills or the response
  ends. On an SSE stream that converts `batch_window` pacing into "whenever the
  buffer happens to fill" and swallows heartbeats entirely, so a healthy stream
  looks dead to both the consumer's liveness check and the proxy's own idle
  timeout. The header is nginx-specific and ignored elsewhere, and it is
  strictly a fallback for operators who have not set `proxy_buffering off`
  themselves — but it is the difference between DMEx working behind a default
  nginx and not.

Two operator-side settings belong in the deployment notes for the same reason:
`proxy_read_timeout` must comfortably exceed the effective heartbeat interval
(`heartbeat_ms + batch_window`, see [Heartbeats](#heartbeats)), and any
intermediary doing its own response buffering has to be turned off rather than
tuned.

#### Unbatching on the consumer

**Chunk boundaries are not frame boundaries.** Chunked transfer encoding and TCP
both deliver arbitrary byte runs, so a frame can span several reads and a read
can contain several frames plus part of another. The consumer accumulates into a
buffer and dispatches on the blank-line delimiter, discarding comment lines
(`: hb`) after counting them as liveness (see [Heartbeats](#heartbeats)).

**That accumulation buffer must be bounded, and the queue bound does not cover
it.** A producer that never emits a blank line — broken, hostile, or simply
mid-frame when it wedged — grows the buffer without limit, and it does so on the
socket path *above* the queue, where `queue_limit` has no reach (see
[The queue](#the-queue)). The cap is the consumer's own maximum frame size:
derived from the producer's echoed `DMEx-Max-Batch-Size` with generous headroom,
or a configured absolute. Exceeding it drops the connection and reconnects from
`last_seen`, which costs nothing beyond a re-read — a drop is not a delivery
event (see [Stalled connections](#stalled-connections)).

On a complete frame the consumer parses `data` **once**, one level deep, into
frame metadata plus the list of rows (see
[Message Envelope](#message-envelope)), pushes those rows onto the queue in
order, and discards the frame. Only `max_id` outlives it, as the lag telemetry.
Turning a row's `data` into a struct happens later, in the pipeline, for the
reasons given in [The queue](#the-queue) — which is also why the parse here
stops one level short rather than decoding the whole frame in one pass.

Note the asymmetry with the producer, and that it is real rather than an
inconsistency: the producer splices row JSON without parsing it, while the
consumer must parse it, because it is the side that has to turn rows into
structs. Only the *depth* of the parse is negotiable, and that is what stopping
one level short buys.

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
  connection to a producer: it holds the connection, parses `DMEx.Frame`
  chunks off it, decodes them via the subscription's `message_modules`, and
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
    but first the failing message is written to a `dead_messages` table via
    the consumer's own storage adapter, in the same transaction as the
    `last_seen` advance. That atomicity is the point: a dead message and the
    cursor that skipped past it commit together or not at all, so there is no
    state where a message was skipped without being recorded, or recorded
    without being skipped.

    **`dead_messages` is structurally a message log.** Its columns are
    `dmex_messages`' columns — `id`, `message_type`, `data`, `metadata`,
    `inserted_at` — unchanged in name, type and meaning: `id` is still the
    producer's own log id, and `inserted_at` is still the original publish
    time. A dead row therefore *is* a log row, and replaying one is handing it
    back to a handler with no reshaping, no field lifting, and no second decode
    path to keep in step with the first.

    Equal as *values*, not as bytes: the payload has been decoded out of a
    frame and re-encoded on the way in, so its key order and whitespace come
    from the consumer's encoder rather than from the producer's `jsonb`
    rendering. That is the same
    [payload byte fidelity](#open-questions) caveat the log itself carries, one
    step further along.

    Two columns sit alongside those, and neither is part of the message: the
    **handler**, because unlike `last_seen` this is **one table shared across
    all of a consumer's subscriptions** — a single place to see everything
    anything ever gave up on — and the **producer URL**, because messages carry
    no `origin` (see [Producer](#producer)) and provenance would otherwise be
    lost. Identity is `(handler, id)`: a handler has exactly one live
    subscription, so its own ids cannot collide.

    **Nothing about the failure is stored.** No exception, no message, no
    attempt count. A failure is an *event*, not a property of the message, so
    it goes to the log and to `[:dmex, :consumer, :failed]`. Persisting it
    would make each row a mixture of a message and an incident, and stop it
    being replayable as the former — which is the entire reason the table has
    the shape it does.
  - In all three modes, the local-retry step beforehand is identical; only
    the terminal action after retries are exhausted differs.

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

##### The queue

**The queue is the measurement.** `DMEx.Consumer.Stream` holds one bounded
FIFO of rows — de-batched on arrival, since `DMEx.Frame` has already parsed
the frame — sitting between the socket and the
handler/transaction/`last_seen` pipeline. It gates the socket re-arm: the next
`active: :once` is requested only while the queue still has room (the exact
gate is below), so the receive window still closes and structural backpressure
still holds (see
[Consuming API](#consuming-api)). The queue is not there to absorb overload —
`queue_limit` rows of slack absorb nothing at a sustained mismatch. It is there
so the process can observe two things a consumer holding exactly one frame
cannot:

- **depth** — how close the socket is to being held off, i.e. whether there is
  pressure right now;
- **drain rate** — this handler's actual capacity, measured rather than
  configured. How that is derived, and in particular how it survives an idle
  connection, is the next section.

Three rules fix what goes into the queue, and all three exist so that every
number the controller reads is in one unit:

- **It holds rows, not frames and not structs.** A row enters exactly as
  `DMEx.Frame` yielded it (`id`, `message_type`, `data`, `metadata`,
  `inserted_at`, see [Message Envelope](#message-envelope)); turning its `data`
  into a message struct happens in the pipeline, on the way *out*. Decoding on
  the way in would put per-row JSON work on the socket path — above the
  `queue_limit` bound, so unbounded by it, and invisible to the drain
  measurement even though it is real cost the consumer pays for every row.
- **Every row is admitted, including ones that will be skipped.** The
  `message_modules` lookup happens in the pipeline too, not at admission — a skip
  is a pipeline step rather than a filter, since it still advances `last_seen`
  (see [Consuming API](#consuming-api)). This matters more than it looks: in v1
  a subscription receives the producer's whole log, so most rows may be skips,
  and filtering them at the door would leave depth and drain rate counting
  *handled* rows while `max_batch_size` counts *wire* rows. Every use of one to
  compute the other would then need a mix ratio that changes whenever the
  producer's traffic shape does. Admitting everything keeps depth, drain rate
  and `max_batch_size` in the same unit — rows on the wire — with no conversion
  anywhere in the controller.
- **It is FIFO, and one pipeline drains it.** Ordering within a subscription is
  a delivery guarantee and duplicate suppression is an `id > last_seen`
  comparison (see [Delivery Semantics](#delivery-semantics)); both assume the
  queue hands rows on in the order they arrived, one at a time. The queue adds
  slack, never concurrency.

**The bound is stated relative to the arrival quantum, and the default is
`2 × max_batch_size`.** Rows arrive one frame at a time, up to
`max_batch_size` of them at once, so a `queue_limit` that is not comfortably
larger than `max_batch_size` makes the socket re-arm gate on ordinary operation
rather than on pressure — one normal frame would fill it. Two frames' worth is
the smallest bound that makes the distinction meaningful: a frame can land while
the previous one is still draining without that alone being a signal, and depth
above one frame's worth means the consumer genuinely did not finish the last
frame before the next arrived. Larger multiples buy nothing the measurement
needs — the queue is not there to absorb overload — and cost memory in direct
proportion. The invariant holds by itself once set, because adaptation only ever
moves `max_batch_size` *below* the configured value (see
[the control law](#the-control-law)), never above it, so the ratio can only
widen from 2×.

**The re-arm gate leaves room for a whole frame**, and that is what makes
`queue_limit` a true bound rather than an approximate one. Rows are admitted a
frame at a time — the frame has already been parsed when the first of its rows
is enqueued (see [Frame encoding](#frame-encoding)) — so a gate of "re-arm while
depth is below `queue_limit`" would let a full frame land on a nearly-full queue
and overshoot by up to `max_batch_size`. The gate is therefore
`depth + max_batch_size <= queue_limit`: re-arm only while a whole frame would
still fit. At the 2× default that is "re-arm while depth is at most one frame's
worth," and peak depth is exactly `queue_limit`.

Two things fall out of that, and they are convenient rather than coincidental.
The pressure threshold and the re-arm threshold are **the same number**
(`max_batch_size`, i.e. half of `queue_limit`), so "depth above the high-water
mark" and "the socket is being held off" are one condition rather than two that
have to be kept consistent. And the trigger for adaptation is then just that
condition sustained past one `batch_window` (see
[the control law](#the-control-law)) — the producer has had a full window to
deliver a frame the consumer had no room for.

The queue itself is not durable and does not need to be. Everything in it sits
above `last_seen` with no handler having run for it, so throwing it away is
indistinguishable from never having received it: the producer still holds those
rows, and the next connect re-reads them from the same cursor every reconnect in
this design already resumes from.

##### Deriving throughput when the stream idles

The obvious measurement is messages committed per wall-clock second, and it is
wrong — worth being precise about, because it is the one every throughput
counter already gives you. **Committed-per-second is the arrival rate whenever
the consumer is not saturated.** A handler capable of 5,000 rows/s that is sent
50 reports 50; an idle connection reports zero. Neither number is a capacity,
and feeding either into a sizing rule would shrink an idle consumer's batches
toward the floor precisely when there was nothing wrong.

**So the primitive is service time, not throughput.** For each row leaving the
queue, the stream records the monotonic duration of its pipeline pass: decode →
transaction begin → `handle_message/2` → `last_seen` write → commit. Capacity is
`1 / mean_service_time`, and that has three properties throughput does not:

- **It is defined at depth 1.** Service time is a property of a single row, so a
  stream that never holds more than one row still measures it. No backlog is
  required to learn how fast the handler is — which is the entire point, since a
  backlog is the condition the measurement exists to avoid.
- **Idle contributes no samples rather than bad ones.** The EWMA is indexed by
  *sample*, not by clock. An hour of silence leaves the estimate exactly where
  the last row left it, rather than decaying it toward zero and making every
  quiet consumer re-learn its own capacity from scratch each time traffic
  resumes.
- **It is the right statistic for sizing a batch.** Draining `n` rows costs the
  *sum* of `n` service times, and by linearity the expected sum is `n × mean`
  whatever the distribution's shape. The mean is therefore exactly what
  `drain_rate × batch_window` wants. Tail latency governs a single message's
  deadline, not a batch's total, so tracking a high quantile instead would
  systematically undersize batches for any handler with bimodal cost — one cheap
  message type and one expensive one, which is the common case rather than the
  exotic one.

**What a sample contains, and what it must not:**

- **Queue wait is excluded.** Time a row spends *in* the queue is a function of
  the backlog ahead of it, not of the handler. Including it would make service
  time grow with depth, which is to say it would fold the pressure signal into
  the capacity signal — and the controller needs them independent, since it uses
  one to decide *whether* to act and the other to decide *how much*.
- **Local retry backoff is excluded, along with the failed attempts.** A raising
  handler is retried in-process with backoff (see
  [Consumer side](#consumer-side)); those sleeps are wall-clock the stream
  spends not draining, but they are a failure mode, not a capacity. One poison
  message under exponential backoff would otherwise drag the estimate down by
  orders of magnitude and pin the subscription's batches at the floor long after
  `on_failure` had resolved it. The sample is the successful pass. A persistent
  failure still shows up — it fills the queue, which is the depth signal — and
  all three `on_failure` modes terminate it without the controller needing an
  opinion.
- **Skips are sampled at their real cost.** A row with no matching module costs
  a lookup, a `last_seen` write and a commit: cheap beside a handler, but not
  free, and genuinely paid per wire row. Since the queue admits them and
  `max_batch_size` counts them, the estimate must price them (see
  [Open Questions](#open-questions) on what per-row commits cost a
  mostly-skipped stream).

**Utilization is the second measurement, and it is what makes idleness
legible.** Alongside service time the stream accumulates busy time — the same
intervals it samples — over a sliding wall-clock window;
`utilization = busy_time / window` is the fraction of the time the pipeline had
anything to do. The three signals answer different questions, and the idle
column is why none of them substitutes for another:

| signal | what it says | when the stream is idle |
| --- | --- | --- |
| service time | how fast this handler is | last known value, unchanged |
| utilization | how much of that capacity is already spoken for | ≈ 0 |
| depth | whether there is pressure right now | 0 |

**Pressure triggers; capacity sizes; utilization is for humans.** Adaptation
fires only on depth sustained above the high-water mark for longer than one
`batch_window` — a condition an idle or comfortably-keeping-up stream cannot
reach, because rows leave faster than they arrive. Service time triggers
nothing; it is read only *after* the trigger, to answer "then how much should I
ask for?" Utilization triggers nothing either, and is not in the control loop at
all — it is the telemetry that separates "keeping up with room to spare" (low
utilization, low lag) from "keeping up with none" (utilization near 1, lag still
low, one traffic increment from falling behind). The `max_id` lag diff (see
[Batching & framing](#batching--framing-batch_window-max_batch_size)) cannot
tell those apart, because both of them are caught up.

**Warm-up and staleness**, the two guards that follow from an estimate built
from possibly very few samples:

- Below a minimum sample count the controller does not act at all: it holds the
  configured `batch_window`/`max_batch_size` and lets the queue fill or not.
  Re-parameterizing off two or three samples is how a control loop ends up
  chasing its own warm-up.
- After a long silence the estimate is **stale rather than wrong**, and is
  treated that way: it is kept as the starting point, since a handler's speed
  rarely changes for free and it is the only evidence there is, but it must be
  re-warmed to the minimum sample count before it can drive a
  re-parameterization. That costs nothing in practice — the samples come from
  the very rows creating the pressure.

All of this is per-subscription, in-memory, monotonic-clock, and dies with the
process (see the limits at the end of this section).

##### The control law

**The control law is AIMD**, over the connection rather than the packet. On
sustained pressure — depth above a high-water mark for longer than one
`batch_window`, so a single burst doesn't trip it — the consumer sizes its next
request to what it has been measured to drain:

```
max_batch_size' = clamp(drain_rate × batch_window, floor, max_batch_size)
```

where `drain_rate` is `1 / mean_service_time` from the previous section, not an
observed throughput. That is the whole target: ask for no more per window than
the handler empties per window. If that computation hits the floor and pressure
persists, the consumer widens `batch_window` instead (multiplicatively), which
lowers frame *rate* once frame *size* has nowhere left to go. Recovery is the
additive half: after several consecutive windows spent near-empty, the consumer
steps back toward its configured `batch_window`/`max_batch_size` a notch at a
time. Back off fast, recover slowly — the asymmetry is deliberate, and it is the
same reason every other AIMD controller has it.

**Recovery unwinds in the reverse order of back-off**: `batch_window` narrows
back toward its configured value before `max_batch_size` grows again. The two
are not symmetric in what they cost while the stream is quiet. An oversized
`max_batch_size` costs nothing at all when there is little to send — it is a
`LIMIT` on the producer's read, not a demand, so a quiet log simply returns
fewer rows — whereas a widened `batch_window` is a floor on frame *rate*, and
therefore sits directly in the end-to-end latency of every message on an
otherwise idle stream. A consumer that widened its window under load and then
went quiet is paying that latency for nothing, and it is the one parameter worth
reclaiming promptly. The reconnect this costs is the cheapest kind: an idle
stream's re-read finds nothing.

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
SELECT id, message_type, data, metadata, inserted_at,
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
  [Message-Type Selection](#message-type-selection-post-v1).
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
ceiling. Cutting it to per-message-type interest is one of the concrete payoffs
of [Message-Type Selection](#message-type-selection-post-v1), and it is worth
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
[Producer side](#producer-side), and routing wakeups by `message_type` is the
first thing [Message-Type Selection](#message-type-selection-post-v1) would buy
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
message types — are the capability this behaviour would have to grow for
[Message-Type Selection](#message-type-selection-post-v1), and the one that would
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
  different database — must be idempotent on something in the message or the
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

## Message-Type Selection (post-v1)

Server-side selective delivery — a consumer receiving part of a producer's log
rather than all of it — is a real requirement, and this is the shape it is
expected to take. **None of it is in v1.** v1 streams whole logs and filters
consumer-side (see [Consuming API](#consuming-api)); everything in this
section is a design direction, recorded so the v1 decisions above stay
compatible with it and so the reasoning isn't rediscovered later. It is not a
description of shipped behaviour.

**There are no topics** — not as a column (see [Message log](#message-log)),
and not as a separate named mechanic either. A topic is a name standing for a
set of message types, and a name is a second vocabulary somebody has to define,
publish, version, discover, and keep in sync with the types it names. Every
one of those costs is paid to describe something the consumer has already
said.

Because it has. A subscription already declares `message_modules` — the list of
message modules it can decode (see [Message Specifications](#message-specifications))
— and that list is not optional: without it a consumer cannot turn an
`message_type` back into a struct at all. **That list is the selection.** DMEx
maps it through each module's `type/0`, encodes the resulting `message_type`
strings into a compact filter, and sends it on the `GET /stream` request; the
producer resolves that filter against its own message types and reads only
matching rows. Nothing new is declared anywhere, by anyone.

### What that buys

- **One list, two jobs, no drift.** A separately-defined grouping can always
  disagree with reality — naming a type the consumer cannot decode, or
  omitting one it can, either of which is a silent bug. A selection *derived
  from* `message_modules` is exactly the set of types the handler is equipped
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

One consequence to know before it bites: **adding a module to `message_modules`
selects that type from the cursor forward, not from the beginning.** The
cursor is a single id and only ever moves forward, so historical rows of the
newly-selected type — everything below `last_seen`, published while the
consumer wasn't asking for it — are not revisited. A consumer that needs that
history has to reset its cursor deliberately. Whether DMEx should offer
anything to make that less manual is open.

### The wire form: an XOR filter in the query string

Two decisions, settled: **the selection travels as an XOR filter**, and it
travels in the **URL, as `?filter=<blob>`**.

**Why a filter rather than the names.** `message_type`s are fully-qualified
module names — `MyApp.Messages.OrderPlaced` is 24 characters — so enumerating
them costs roughly 25–40 bytes per type and a consumer with a few hundred
modules overruns what proxies and servers will carry. An
[XOR filter](https://arxiv.org/abs/1912.08258) (Graf & Lemire) encodes the
same set at **≈1.23 bytes per type at 8-bit fingerprints, ≈2.46 at 16-bit**,
before base64url. That is a ten- to thirty-fold reduction depending on
fingerprint width — not a constant size, since the filter still grows with the
set, but with a small enough constant that the size question stops being
interesting: a thousand message types fit in a couple of kilobytes at xor16.

XOR filters are the right family here rather than Bloom or cuckoo because the
set is **static**: built once from `message_modules` and never added to for a
connection's lifetime. That is exactly the constraint XOR filters trade
mutability for, and they are ~15% smaller than a Bloom filter at equal error
and faster to query (three lookups, no probing loop). Binary fuse filters are
the closely-related successor and smaller still; XOR is chosen for having the
wider set of implementations across languages, which matters for the
non-Elixir consumers discussed below.

The error direction is what makes any of this acceptable: **no false
negatives**, so every type the consumer selected is delivered, and a false
positive only costs an unwanted row crossing the wire to be dropped by the
skip path v1 already specifies (see [Consuming API](#consuming-api)).

**Sizing is not about bandwidth percentages, and this is the subtle part.**
Membership is tested on the *type string*, not per row, so a false positive
does not sprinkle 0.39% of extra rows across the stream — it admits **one
entire message type, for the whole life of the connection**. At an 8-bit
fingerprint (2⁻⁸ ≈ 0.39%), a producer with 100 types and a consumer selecting
10 expects 90 × 0.0039 ≈ 0.35 spurious types; if the unlucky one is the
highest-volume type in the system, that connection receives all of it. Since
the cost of going to 16-bit fingerprints is 1.23 extra bytes per selected type
— nothing, at these set sizes — and it cuts that probability by 256×,
**xor16 is the sensible default** and the 8-bit variant an option for
consumers that know their producer's traffic shape.

**Why the query string, and why the rule in
[Transport](#transport-sse-over-http) survives unamended.** That rule puts
what a subscription *is* in the URL and how it is *paced* in headers. A type
selector is identity, so it lands in the URL — and two things make that the
right answer rather than merely the consistent one:

- **A browser `EventSource` can select.** It sets no request headers beyond
  `Last-Event-ID`, but the URL is entirely its own. Carrying the filter in a
  header would have left every browser client receiving its producer's whole
  log and discarding most of it; carrying it in the query string gives a bare
  `EventSource` real server-side selection with no client library at all. This
  is the deciding argument.
- **Selection changes at deploy cadence, not connection cadence.** The
  URL-stability half of the rule has force in proportion to how often a value
  changes, and that is what separates the two cases: an adaptive consumer
  re-negotiates `batch_window`/`max_batch_size` repeatedly *within* one
  subscription's lifetime (see
  [Backpressure & adaptive batching](#backpressure--adaptive-batching)), so
  those in a URL would mint new URLs continuously, while `message_modules`
  changes when somebody edits code and ships it. One URL per deployment is
  precisely the stable grouping the rule is protecting.

The cost, stated plainly: the blob lands in every access-log line of every
intermediary, and the request-line limit (around 8 KB in both nginx and
Apache by default) becomes the ceiling rather than a header limit. At xor16
that is on the order of two thousand message types, and even a conservative 2 KB
budget leaves several hundred. Note this is exactly **why the filter is
load-bearing**: spelled-out module names at 25–40 bytes each would exhaust a
2 KB request line at around sixty types, which is an entirely plausible
`message_modules` list. The filter is what makes the URL viable, and the URL is
what makes browser consumers viable.

**The parameter.** `?filter=<blob>`, where the blob is the filter's
`{seed, fingerprints}` tuple — the 64-bit seed followed by the fingerprint
array, big-endian — encoded as **unpadded base64url**
([RFC 4648 §5](https://www.rfc-editor.org/rfc/rfc4648#section-5); standard
base64's `+` and `/` would need percent-encoding and `=` padding is noise in a
query string). No length field is needed: the array length falls out of the
decoded byte count given the fixed fingerprint width, and the segment length
the three hash positions are derived from falls out of that. **Absent means
the whole log**, which keeps v1's behaviour as the default for any client that
does not ask. A blob that is not valid base64url, or whose decoded length is
not a seed plus a whole number of fingerprints, is a `400` — the same standard
`Last-Event-ID` and the pacing headers are held to.

**The encoding must be pinned bit-for-bit.** The blob carries only the seed
and the array; everything else — the fingerprint width, the hash
construction, the byte order, and the fact that the hashed key is the UTF-8
`message_type` string — is fixed by DMEx and carried by neither side. None of it
may be negotiated. This is the one place in the protocol where producer and
consumer must agree exactly, because disagreement does not produce a garbled
filter — it produces **false negatives**, which is silent message loss and
precisely the failure the whole scheme was chosen for not having. It also has
to be written down as a spec
rather than left to the Elixir implementation, since a non-Elixir consumer has
to reproduce it exactly.

**What a browser still cannot do is pace itself.** `DMEx-Batch-Window` and
`DMEx-Max-Batch-Size` remain headers, so an `EventSource` takes the producer's
defaults (see [Transport](#transport-sse-over-http)). That asymmetry is the
rule working rather than an inconsistency in it: pacing is a tuning knob whose
defaults have to be sane anyway, while selection decides what crosses the wire
at all, and a client with no way to express it pays for that on every message
for as long as it stays connected.

### What it would not change

- **Not a partitioning of the log.** There is still exactly one
  totally-ordered log per producer and one id sequence; a selective
  subscription sees a sparse subset of that same id space, so every consumer's
  `last_seen` stays directly comparable with every other's regardless of what
  it selected.
- **Not visible at the publish call site.** `publish/3` keeps its signature
  and writes the same columns, and message modules keep taking no options. A
  publisher does not know, and does not need to know, who is selecting its
  message types.
- **Not a change to delivery semantics.** Selection changes which rows a
  connection is sent, and nothing about ordering, duplicate suppression, or
  the handler/transaction/`last_seen` pipeline.

### Where it would attach

The seams are already in place, and naming them is most of what deferring the
work is meant to protect:

- **Subscribe** — no new option. `message_modules` gains a second role, and
  `DMEx.Consumer.Stream` derives the wire values from it.
- **Transport** — the `filter` query parameter, per the wire form above.
  Fixed for a connection's lifetime, for the same reason pacing is: a consumer
  wanting a different selection reconnects.
- **Read path** — the loop's query grows one predicate
  (`WHERE message_type = ANY($1) AND id > $2`), which brings back both the
  composite-index requirement (`(message_type, id)`) and the sparse-cursor
  question retired in [Producer side](#producer-side).
- **Wakeups** — the routing key is the published row's own `message_type`, and
  connections register under the types they selected. That is *exact*: a
  publish wakes precisely the connections that asked for that type, with no
  intermediate grouping to be approximate about it. This is the
  read-amplification lever described in [Wakeup fan-out](#wakeup-fan-out).
- **Producer-side validation** — there is none, deliberately. A consumer may
  legitimately name a type the producer has never published (a consumer
  deployed ahead of the producer that will emit it), so the filter is resolved
  against whatever the producer knows and never checked for plausibility: no
  rejecting a filter that matches nothing, and emphatically no atomizing of
  any consumer-supplied string,
  which is the same unbounded-atom-table risk the consumer side already
  refuses to take (see [Message Specifications](#message-specifications)). A name
  that matches nothing simply matches nothing.
- **Consumer** — the skip path in [Consuming API](#consuming-api) becomes a
  defence rather than the normal case. A selective stream should only ever
  carry decodable types, so an unknown one now indicates a real mismatch and
  is worth surfacing rather than quietly dropping.

### Open, if and when it lands

- **Whether the producer expands the filter or tests row by row.** A filter
  cannot be pushed into a SQL index; membership is a computation, not
  something an index can seek on. The producer can sidestep that by expanding
  the filter *once at connect time* against its own compiled message modules,
  turning it into the `message_type = ANY($1)` list the read path wants. Testing
  per row instead leaves the read as v1's unfiltered scan with filtering in
  the application: no index seek, but no regression against v1 either — and no
  expansion to keep current, which the next point is about.
- **If it expands, it must expand over the producer's message modules, not over
  its log contents.** The two sources look interchangeable and are not.
  Expanding over the types the log has actually seen
  (`SELECT DISTINCT message_type ...`) goes stale silently: a producer's code
  can know a type it has not yet emitted, and the first emission of it
  involves no restart and no reconnect, so an open connection keeps an
  expansion missing a type its consumer genuinely selected. Expanding over the
  producer's own compiled message modules has no such gap — that set changes
  only when the producer deploys, and a deploy takes down the web server and
  with it every SSE connection, since the request process *is* the stream (see
  [Producer side](#producer-side)). Every affected consumer reconnects and
  re-expands against the new set, so the repair is forced by the same property
  that makes producer restarts uneventful, with no re-expansion machinery to
  build or get wrong.

  Worth recording the trap, since it is the intuitive reading: that a type the
  producer has only just begun publishing must have been unknown to the
  consumer too, and so could only ever be a false positive. It does not hold.
  The two vocabularies are independent — a consumer deployed ahead of the
  producer legitimately names types the producer has not yet emitted, which is
  exactly why validation was dropped above — so such a type can be a true
  positive, and dropping it would be a real false negative. What saves it is
  the deploy boundary, not the definition.

  The one residue is a rolling multi-node deploy, where a connection pinned to
  a node running old code outlives a node running new code that has begun
  publishing the new type. That belongs to the existing cross-node clustering
  question (see [Open Questions](#open-questions)), not to this one.
- **Wakeup routing.** With expansion, routing works exactly as it would for an
  explicit list of types. Without it, the `Dispatcher` must test every
  connection's filter against each published type — a few hashes per
  connection per publish, cheap in absolute terms but `O(C)` again, which is
  the amplification the routing existed to remove.
- **Whether the producer echoes the resolved type list.** If it expands, it
  already knows exactly which types tested positive, so it can echo them on
  the `200` the same way it echoes the pacing values it actually applied (see
  [Transport](#transport-sse-over-http)). That would hand back the
  observability an opaque filter destroys — an explicit list says what a
  subscription is at a glance in a proxy log, and a base64 blob says nothing
  and cannot be reversed. With an echo a consumer can see its own subscription
  resolved, spot a false-positive type that is flooding it, and detect an
  encoding mismatch immediately rather than through missing messages. Open:
  echo the full list (bounded by the producer's own type count, but
  potentially large), a count, or a digest — and whether it is always on or a
  debug affordance.
- **The default fingerprint width, and whether consumers may choose it.**
  xor16 is the sensible default for the reason given in
  [the wire form](#the-wire-form-an-xor-filter-in-the-query-string), but a
  consumer that knows its producer's traffic shape may rationally want xor8,
  and one reading a log with a single enormous type may want wider. If it is
  selectable it has to be carried in the header and validated, which is one
  more thing that must not be got wrong.
- **Request-line limits.** URLs are capped by the web server and by every
  intermediary — nginx's `large_client_header_buffers` and Apache's
  `LimitRequestLine` are both around 8 KB by default — which at xor16 plus
  base64url puts the practical ceiling in the low thousands of message types.
  That is far beyond any plausible `message_modules` list, but the number should
  be documented and the limits configured deliberately rather than discovered
  when someone crosses one. At least the failure is loud: a `414` at connect
  time, not a silently truncated selection.
- **Filter construction failure.** XOR construction is randomized and can fail
  to peel, retrying with a fresh seed; it succeeds with high probability and
  the sets here are tiny, so this is a consumer-side detail rather than a
  protocol one. Open only in that a bounded retry count, and what to do when
  it is exhausted, want specifying — falling back to sending no filter at all
  (whole-log delivery, i.e. v1) rather than failing the subscription.
- **Whether selection should be narrowable below `message_modules`** — a
  consumer that can *decode* twenty types but only *wants* five would
  otherwise have to receive fifteen it discards, or split into two handlers
  and therefore two cursors. Does `subscribe/2` want an explicit override, or
  is "your module list is your subscription" the whole rule? The rule is
  simpler; the override is the escape hatch for a handler whose decode
  vocabulary is genuinely wider than its interest.
- **Second cursor, again** — with a sparse subset the last-*sent* id no longer
  implies the rows in between were examined and rejected. Whether that needs a
  second cursor or is answered entirely by the `(message_type, id)` index (an
  index seek costing the same whether the next matching row is one id away or
  ten million) has to be settled with the index, not before it.
- **Non-Elixir consumers** — the selector is a list of Elixir module names, so
  a consumer in another language must know those strings out-of-band. That is
  the same coupling as
  [sharing message modules across independent applications](#open-questions),
  now showing up in the URL as well as in the decode step, and it strengthens
  the case for the schema-endpoint option discussed there.

## Settled by the v1 implementation

Questions this document raised that the code has since answered. Each is a
decision that could be revisited, not a law — but revisiting one now means
changing shipped behaviour rather than choosing freely.

- **Every constant.** `DMEx.Config` is the single home for defaults and bounds,
  documented there in one table with a line of reasoning per value, and
  overridable per producer and per subscription. The load-bearing ones:
  `batch_window` 200 ms (floor 50, ceiling 5 000), `max_batch_size` 500 rows
  (floor 1, ceiling 5 000), `heartbeat_ms` 15 000, poll interval 1 000 ms,
  frame byte cap 1 MiB producer-side and 4 MiB consumer-side, three local retry
  attempts, jittered reconnect backoff from 500 ms capped at 30 s, and `k` = 3
  missed heartbeats. Two of these are **not in this document**, and were added
  because their absence was a hole rather than a default: a *ceiling* on
  `batch_window` (it sits inside the advertised effective heartbeat, so an
  unbounded window lets a consumer push its own liveness deadline past every
  proxy idle timeout) and a *floor* on `max_batch_size` (the adaptive law must
  always have something legal to converge to).

- **The producer's app key.** Derived from the producer module's own name,
  underscored and flattened (`MyApp.Producer` → `my_app_producer`), overridable
  with `app:`, validated at compile time against `~r/[a-z][a-z0-9_]*/` and 58
  bytes — Postgres's 63-byte identifier limit less the `dmex_` channel prefix.
  The module name rather than the OTP app because it needs no Mix at compile
  time, works in umbrellas, and is stable in a release. The advisory lock is the
  two-int form: a fixed DMEx namespace `classid` keeping its lock space disjoint
  from any other library sharing the database, and `phash2(app_key)` as `objid`.

- **The consumer-side schema.** `dmex_cursors` keys on `handler` as its primary
  key rather than a surrogate id — the handler *is* the subscription's identity,
  and that unique index enforces "one live subscription per handler module" in
  storage as well as in the process registry. `dmex_dead_messages` is keyed on
  `(handler, id)` rather than the `(producer_url, id)` this document suggested:
  the table is shared across subscriptions, and two handlers may legitimately
  give up on the same producer row, so keying without the handler would make the
  second a spurious conflict — while a handler has exactly one live
  subscription, so `producer_url` is provenance rather than identity. A handler
  rename is `DMEx.Consumer.Cursors.rename!/3` called from a migration, not
  implicit magic.

- **`message_type` index.** Not shipped. Every v1 read is a bare `id > cursor`
  range scan that the primary key serves directly, so an index would exist
  purely for operators querying by hand — and appends are serialized per app, so
  its maintenance would sit on the one write path that cannot be parallelized
  away. `DMEx.Migration`'s documentation tells operators how to add one.

- **`:crash` versus supervisor restart intensity.** `DMEx.Consumer.Supervisor`
  sets an effectively infinite budget, and the rate limiting moves *inside*
  `DMEx.Consumer.Stream` as jittered backoff applied before each connect. A
  `DynamicSupervisor`'s default three-in-five-seconds is blown by a crash-loop
  in under a second, and when it is, the supervisor dies and takes every other
  subscription in the application with it. This way a poison message
  crash-loops forever on its own subscription, at a pace that does not hammer a
  different application's database, and its siblings never notice.

- **Where a subscription's cursor is written.** This document's `subscribe/2`
  sketch names no storage at all. Resolved in three layers with no global
  config: the `DMEx.Consumer` child spec's `repo:` is the default, a
  subscription entry may override it, and a bare `subscribe/2` resolves from
  the running consumer. With none of those it raises and names both fixes.

- **The publish and subscribe call sites.** Both forms exist, deliberately:
  `DMEx.Publisher.publish/3` and `MyApp.Producer.publish/2`, and
  `{DMEx.Consumer, subscriptions: [...]}` and `DMEx.Consumer.subscribe/2`. The
  declarative form is what an application writes; the imperative one is for
  generic code holding a producer or deciding at runtime.

- **Telemetry vocabulary**, pinned because renaming it later breaks every
  dashboard: `[:dmex, :publish, :start|:stop|:exception]`,
  `[:dmex, :stream, :connect]`, `[:dmex, :stream, :frame]`,
  `[:dmex, :dispatcher, :broadcast]`,
  `[:dmex, :consumer, :message, :start|:stop|:exception]`,
  `[:dmex, :consumer, :skip]`, `[:dmex, :consumer, :lag]`,
  `[:dmex, :consumer, :reconnect]`.

Two implementation notes that amend the text above rather than answering a
question it asked:

- **The send loop reads before it waits.** The loop sketched in
  [Producer side](#producer-side) puts the `receive` first, which would make a
  freshly-connected consumer wait a full `heartbeat_ms` before its first frame
  even with a backlog already waiting. The shipped loop drains wakeups, reads,
  and only blocks when the read came back empty — which is also what makes
  "catch-up is the ordinary loop running hot" true rather than aspirational.

- **`Mint.HTTP.stream/2` re-arms `active: :once` itself.** The consumer-side
  requirement in [Consuming API](#consuming-api) reads as though the re-arm is
  directly withheld; in Mint it is expressed as `set_mode(:passive)` after
  processing and `set_mode(:active)` when headroom returns. Mint documents that
  one socket message may already be in the mailbox at that switch — a bounded
  overshoot of exactly one chunk, which the `depth + max_batch_size <=
  queue_limit` gate already tolerates.

## Open Questions

Things intentionally left open for the next round of discussion. Constants and
schema decisions this document once listed here have moved to
[Settled by the v1 implementation](#settled-by-the-v1-implementation).

- **Auth / access control** — who's allowed to read a given producer's SSE
  endpoint, and how is that enforced?
- **Compression** — the mechanism is settled in
  [Compression](#compression) and is deliberately **not implemented in v1**. It
  is orthogonal to everything else and its cost is paid in the same currency as
  the producer's memory bound, so it lands as its own stage. Proposed defaults
  when it does: `windowBits` 12 / `memLevel` 4 (~32 KB per connection rather
  than ~256 KB), and the frame byte cap measured *before* compression, which is
  simpler and bounds the producer's own buffers.
- **Payload byte fidelity** — `data` and `metadata` are `jsonb` (see
  [Message log](#message-log)), chosen for in-database querying of payloads,
  which is a first-class use case for a log people inspect by hand. The
  accepted cost is that `jsonb` normalizes: key order is not preserved,
  duplicate keys are dropped, insignificant whitespace is lost. Because the
  wire envelope is re-serialized from storage (see
  [Message Envelope](#message-envelope)), the bytes a consumer receives are
  therefore not guaranteed to be the bytes the publisher encoded. **The v1
  implementation confirms this is visible on the wire, not merely theoretical**:
  Postgres renders `jsonb::text` with a space after each colon and in its own
  key order, so a frame carries `{"total": null, "order_id": 7}` where the
  publisher encoded `{"order_id":7,"total":null}`. Same values, different bytes.
  There is an integration test asserting it, so the caveat cannot quietly stop
  being true. Open: does
  this need documenting as a hard caveat — no signing or hashing of raw
  payload bytes end-to-end — or does some future use case justify a
  side-by-side exact-bytes column?
- **Adaptive control-law constants** — **now chosen and shipped**, in
  `DMEx.Config` with the rest: EWMA `0.2`, 20 samples before the controller may
  act, 60 s before an estimate counts as stale, recovery after 3 quiet windows
  in steps of 25% of the configured `max_batch_size`, and 30 s minimum between
  changes. The water marks are expressed relative to `max_batch_size` as this
  document asked — `DMEx.Consumer.Queue.high_water/1` *is* the re-arm headroom,
  so "depth above the mark" and "the socket is being held off" are one
  condition rather than two kept in step. What remains open is whether these
  numbers are right under real load; they were picked from the reasoning below
  and have only been exercised against a deliberately slow test handler. The
  original framing: the shape is settled (AIMD over
  reconnects, and service time as the capacity primitive, see
  [Backpressure & adaptive batching](#backpressure--adaptive-batching)); the
  numbers are not. What are the multiplicative back-off and additive recovery
  steps, the number of quiet windows before recovery starts, and the minimum
  interval between re-parameterizations? On the measurement side: the EWMA
  smoothing factor over service-time samples, the minimum sample count before
  the controller may act, and how long an estimate may go unsampled before it
  counts as stale and must be re-warmed. The high- and low-water marks want
  expressing **relative to `max_batch_size`, not as bare fractions of
  `queue_limit`** — what the high-water mark has to exclude is the transient of
  one normal frame arriving, so the quantity it must sit above is the arrival
  quantum. `queue_limit` itself is settled at `2 × max_batch_size`. Related:
  should the queue also be bounded in *bytes*, for the same reason the producer
  wants a frame byte cap (see below)? A `queue_limit` in rows says nothing about
  memory when payloads vary by three orders of magnitude.
- **Per-row commits on a mostly-skipped stream** — **now measured, and the
  answer argues for building it.** `bench/e2e/catchup_drain.exs` drains the same
  seeded log three ways. A client that reads and discards sustains ~2 400 rows/s,
  so the producer's read-encode-send path is not the constraint. Through a real
  consumer with a no-op handler that collapses to ~155 rows/s, and a stream
  where *every* row is skipped manages only ~250 rows/s. Skipping is barely
  1.6x faster than doing real work, because what both are actually paying for
  is one transaction commit per row — roughly 4-6 ms of it. So a consumer with
  modules for a small fraction of a producer's types is very nearly commit-bound
  on work it discards, and the consumer is ~15x slower than the producer
  feeding it. This also puts a number on replay: at ~155 rows/s a million-row
  log takes roughly two hours to replay end to end, which is the figure the
  "arbitrarily replayable" claim should be quoted with. The
  counter-argument below still stands — server-side selection removes the
  motivation entirely — so the real question is which lands first. In v1 a
  subscription
  receives the producer's whole log, and a skipped row still gets its own
  transaction and `last_seen` advance (see
  [Consuming API](#consuming-api)). On a stream where the consumer has modules
  for a small fraction of the types, the drain is then dominated by commits for
  rows nothing was done with — which caps measured capacity, and so caps the
  batch sizes the controller will ask for, on work that is pure overhead. Open:
  is it worth coalescing a run of consecutive skips into a single `last_seen`
  advance? It is safe on its face — no handler ran, so there is nothing to roll
  back and the cursor only ever lands on a row boundary — but it makes
  `last_seen` advance in variable-size jumps, and it is the first place in the
  design where a batch means anything to processing rather than only to framing
  (see
  [Batching & framing](#batching--framing-batch_window-max_batch_size)). Note
  that [Message-Type Selection](#message-type-selection-post-v1) removes the
  motivation entirely, which is an argument for not building it.
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
- **Read amplification ceiling** — **partly answered, and better than feared.**
  `bench/e2e/fanout_reads.exs` holds C connections open against one producer and
  counts frames from the producer's own telemetry. Two properties hold exactly
  as designed: each connection is capped at **one frame per `batch_window`**
  (measured 5.2/s against a 200 ms window) regardless of publish rate, and an
  **idle log costs one query per poll interval per *app*** — 1/s flat, whether C
  is 10 or 200 — which is the thundering-herd guard doing its job. No pool
  checkout queueing appeared at 200 connections on a developer machine, so the
  ceiling this question asks for is somewhere above that and was not reached.
  Still open: where it actually is, and whether the pool wants partitioning so
  stream reads cannot starve publish writes. The original framing: each
  connection issuing its own reads
  (see [Producer side](#producer-side)) trades bounded memory for query
  volume. In v1 every connection sees every publish (see
  [Wakeup fan-out](#wakeup-fan-out)) — routing wakeups by interest is deferred
  with [Message-Type Selection](#message-type-selection-post-v1) — so the ceiling
  is the
  full `C × P`, and the practical limit is DB connection-pool contention.
  Open: what is the realistic connection ceiling per producer app, does the
  pool need partitioning so stream reads can't starve publish writes, and is a
  shared cache of recent rows at the tip ever worth the shared mutable state
  it reintroduces? This subsumes the older "multi-consumer fan-out"
  question: many consumers reading one stream independently already works
  (each tracks its own `last_seen` and gets its own connection process), so
  the only remaining limit is read load, not memory.
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
  its intended shape (the consumer's `message_modules` list on the wire as an
  `message_type` selector) and its own open questions written up in
  [Message-Type Selection](#message-type-selection-post-v1). The
  question that belongs *here* is when to build it: shipping v1 without it
  means every consumer pays for every message its producer publishes, in
  bandwidth and in a decode-and-discard pass, and every producer pays `C × P`
  reads (see [Producer side](#producer-side)). Open: is the trigger a measured
  one — bandwidth on a fat log, read load at a connection count — or is it
  simply the first non-trivial adopter with a log whose consumers each want a
  slice?
- **Append serialization cost** — **now measured.**
  `bench/e2e/publish_throughput.exs` compares `publish/3` against the identical
  insert without the lock. At concurrency 1–2 the lock costs about **1.13x** —
  it is nearly free when nothing contends for it. At concurrency 8 the ratio is
  ~4.8x and at 32 it is ~5.8x, and locked throughput *falls* as concurrency
  rises rather than flattening: contention costs more than serialization alone.
  So the practical advice is that publish concurrency buys an application
  nothing, and a caller that wants throughput should batch its work into fewer,
  larger messages rather than more concurrent publishes. What remains open is
  the rest of the original question: per-app advisory-locked appends (see
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
- **Queue bounded in bytes as well as rows** — `queue_limit` is a row count,
  which says nothing about memory when payloads vary by three orders of
  magnitude. The consumer already caps a single *frame* at `max_frame_bytes`, so
  the exposure is bounded at `queue_limit / max_batch_size` frames' worth rather
  than being unbounded — but that is a bound on frames, not on bytes, and a
  subscription reading a log of 16 KB payloads holds far more memory at the same
  depth than one reading 200-byte payloads. Open: is a byte bound worth a second
  threshold the controller has to reason about, or is documenting the product
  enough?
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
- **`dead_messages` retention** — inspection, replay and discard now exist
  (`DMEx.Consumer.DeadMessages`), and they are cheap precisely because a dead
  row is a log row. What is still open is pruning: the table grows unbounded
  like the log itself, and the only time it carries is the *original publish*
  time, since storing when a message died would have meant storing something
  about the failure. Age-since-death is therefore not an available axis, which
  is a deliberate trade and possibly the wrong one. Options if it bites: prune
  by the message's own `inserted_at`, prune by count per handler, or reconsider
  whether "when this row was written" counts as failure data at all.
- **`:skip` visibility** — `:skip` silently drops a message with no DMEx-side
  record of the failure (unlike `:dead_message`). Is that intentional as a
  "you must supply your own telemetry" mode, or should DMEx always emit a
  telemetry event/log line on skip regardless of `on_failure`?
- **Message schema versioning** — each message type has a dedicated Elixir
  module, but there's no versioning scheme yet for evolving a struct's
  fields (e.g. `order_placed_v2`) without breaking consumers pinned to the
  old shape.
- **Sharing message modules across independent applications** — the whole
  [Message Specifications](#message-specifications) design assumes the consumer
  has the *same* Elixir module (`MyApp.Messages.OrderPlaced`) the producer
  used, so it can `use DMEx.Message`-derive the same `message_type` and decode
  `data` into the same struct. That's a given inside one umbrella app, but
  producer and consumer are, in general, independent applications — possibly
  different teams, repos, deploys, even languages. How does a consumer get
  its hands on the producer's message modules?

  One constraint to carry into this decision: dropping `origin` (see
  [Producer](#producer)) rests on `message_type` being app-prefixed, so that
  `MyApp.Messages.OrderPlaced` identifies its publisher. An org-scoped shared
  package (`MyOrg.Messages.OrderPlaced`, published once and depended on by
  several producer apps) breaks that — the same `message_type` could then be
  published by more than one application, and "which app published this"
  would no longer be derivable from the wire. Any option below that leads to
  shared, non-app-prefixed module names reopens the `origin` question.

  A few options worth weighing against each other:
  - **Shared library/package** — the producer app publishes its message
    modules as a small, dependency-light package (private Hex package or git
    dep) that both sides add as a dependency. Keeps the "one Elixir module is
    the schema" model intact end to end; closest to how
    [Commanded](https://github.com/commanded/commanded)-based systems
    typically share message structs. Costs: a real versioned-release/publish
    workflow, and it's Elixir-only — no path for a non-Elixir consumer.
  - **Self-describing producer / schema endpoint** — alongside the SSE
    endpoint, the producer exposes something like
    `GET /stream/schema` listing its known `message_type`s and field
    shapes (e.g. as JSON Schema). Consumers could codegen structs from it,
    or just validate/introspect at runtime. Decouples deploys and opens the
    door to non-Elixir consumers, at the cost of a second interface DMEx has
    to keep honest and versioned alongside the actual message modules.
  - **Loose/dynamic decode fallback** — let a subscription skip
    `message_modules` entirely and receive the raw decoded JSON `map` plus
    `message_type` string instead of a typed struct. No shared module needed
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
  `message_modules` list by hand per `subscribe/2` call; is there a
  lighter-weight registration mechanism (e.g. an application-level
  config/registry) worth adding once there are many message types?
- **Cross-node clustering** — if a producer application itself runs as a
  multi-node OTP cluster, how is "the" append-only log, and the
  `LISTEN`/`NOTIFY`-based `DMEx.Dispatcher`, kept single-writer consistent
  across nodes?
