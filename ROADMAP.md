# DMEx — Roadmap

> Status: design direction, not shipped behaviour. Everything in this document
> is deliberately **not in v1**. It is recorded so the v1 decisions in
> [ARCHITECTURE.md](ARCHITECTURE.md) stay compatible with it and so the
> reasoning isn't rediscovered later. ARCHITECTURE.md remains the spec of
> record for what DMEx actually does; this file is the spec of intent for what
> it is expected to grow.

Three things are planned. The first two share a route namespace rather than a
mechanism; the third shares neither and is about how much a producer will
serve rather than what:

- **[Message-Type Selection](#message-type-selection)** — server-side selective
  delivery, so a consumer receives the part of a producer's log it can actually
  handle instead of all of it. Carried as `GET /stream/<filter blob>`.
- **[Stream Stats](#stream-stats-get-streamstats)** — `GET /stream/stats`, a
  plain JSON description of the message types a log actually contains and the
  id range each one spans.
- **[Global Resource Limits](#global-resource-limits)** — an aggregate
  connection budget and admission control, seated on the watcher that already
  runs per producer.

## Message-Type Selection

Server-side selective delivery — a consumer receiving part of a producer's log
rather than all of it — is a real requirement, and this is the shape it is
expected to take. **None of it is in v1.** v1 streams whole logs and filters
consumer-side (see
[Consuming API](ARCHITECTURE.md#consuming-api)).

**There are no topics** — not as a column (see
[Message log](ARCHITECTURE.md#message-log)), and not as a separate named
mechanic either. A topic is a name standing for a set of message types, and a
name is a second vocabulary somebody has to define, publish, version, discover,
and keep in sync with the types it names. Every one of those costs is paid to
describe something the consumer has already said.

Because it has. A subscription already has a message list — the modules it can
decode (see [Message Specifications](ARCHITECTURE.md#message-specifications)),
derived at compile time from the structs its `handle_message/2` clauses match
on — and it is not optional: without it a consumer cannot turn a `message_type`
back into a struct at all. **That list is the selection**, and nobody wrote it
down for this purpose or any other. DMEx maps it through each
module's `type/0`, encodes the resulting `message_type` strings into a compact
filter, and puts it in the path of the `GET` it opens; the producer resolves
that filter against its own message types and reads only matching rows. Nothing
new is declared anywhere, by anyone.

### What that buys

- **One list, two jobs, no drift.** A separately-defined grouping can always
  disagree with reality — naming a type the consumer cannot decode, or
  omitting one it can, either of which is a silent bug. A selection *derived
  from* that list is exactly the set of types the handler is equipped
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
  ["producer holds nothing"](ARCHITECTURE.md#consumer) property survives intact
  — a producer-side registry of named groupings would have been the first thing
  to chip at it.

One consequence to know before it bites: **adding a clause for a new type
selects that type from the cursor forward, not from the beginning.** The
cursor is a single id and only ever moves forward, so historical rows of the
newly-selected type — everything below `last_seen`, published while the
consumer wasn't asking for it — are not revisited. A consumer that needs that
history has to reset its cursor deliberately, and
[Stream Stats](#stream-stats-get-streamstats) exists partly so it can work out
*how far* back to reset rather than going to the beginning of the log.

### The wire form: an XOR filter in the path

Two decisions, settled: **the selection travels as an XOR filter**, and it
travels in the **URL, as a path segment — `GET /stream/<blob>`**.

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
set is **static**: built once from the derived list and never added to for a
connection's lifetime. That is exactly the constraint XOR filters trade
mutability for, and they are ~15% smaller than a Bloom filter at equal error
and faster to query (three lookups, no probing loop). Binary fuse filters are
the closely-related successor and smaller still; XOR is chosen for having the
wider set of implementations across languages, which matters for the
non-Elixir consumers discussed below.

The error direction is what makes any of this acceptable: **no false
negatives**, so every type the consumer selected is delivered, and a false
positive only costs an unwanted row crossing the wire to be dropped by the
skip path v1 already specifies (see
[Consuming API](ARCHITECTURE.md#consuming-api)).

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

**Why the URL, and why the rule in
[Transport](ARCHITECTURE.md#transport-sse-over-http) survives unamended.** That
rule puts what a subscription *is* in the URL and how it is *paced* in headers.
A type selector is identity, so it lands in the URL — and two things make that
the right answer rather than merely the consistent one:

- **A browser `EventSource` can select.** It sets no request headers beyond
  `Last-Event-ID`, but the URL is entirely its own. Carrying the filter in a
  header would have left every browser client receiving its producer's whole
  log and discarding most of it; carrying it in the URL gives a bare
  `EventSource` real server-side selection with no client library at all. This
  is the deciding argument.
- **Selection changes at deploy cadence, not connection cadence.** The
  URL-stability half of the rule has force in proportion to how often a value
  changes, and that is what separates the two cases: an adaptive consumer
  re-negotiates `batch_window`/`max_batch_size` repeatedly *within* one
  subscription's lifetime (see
  [Backpressure & adaptive batching](ARCHITECTURE.md#backpressure--adaptive-batching)),
  so those in a URL would mint new URLs continuously, while the message list
  changes when somebody edits code and ships it. One URL per deployment is
  precisely the stable grouping the rule is protecting.

**Why a path segment rather than `?filter=`.** Both are "in the URL", so this
is the narrower question of which half of the URL, and four things decide it:

- **A different selection is a different resource, not the same one with a
  modifier.** That is exactly what a path expresses and what a query string
  conventionally does not: `?` is for parameters that qualify a resource, and
  the filter does not qualify the stream, it decides which messages exist for
  it at all. Carrying identity in the path is the identity argument above
  taken to its conclusion rather than stopped one step short.
- **It needs no routing change.** The v1 mount is already a prefix forward —
  `forward "/stream", to: DMEx.Producer.Plug` (see
  [Transport](ARCHITECTURE.md#transport-sse-over-http)) — which matches the
  prefix and hands the remaining segments to the plug, so `/stream/<blob>` is
  routable by the same one-line mount that already exists, in Phoenix,
  `Plug.Router` and a bare Bandit/Cowboy pipeline alike. The plug reads
  `conn.path_info`, which it is handed, rather than fetching query params.
- **Intermediaries treat paths as load-bearing and query strings as
  negotiable.** Caches, CDNs and access-log formats routinely drop, reorder,
  normalize or key-strip query strings, and some proxies are configured to
  discard them entirely; nothing does that to a path. The filter is not a
  hint — losing it silently converts a selective subscription back into a
  whole-log one, which is a bandwidth incident rather than an error — so it
  belongs on the part of the URL that is never quietly rewritten.
- **base64url is already path-safe, so the move costs nothing.** The alphabet
  (`A–Z`, `a–z`, `0–9`, `-`, `_`) is entirely unreserved in a path segment
  under [RFC 3986](https://www.rfc-editor.org/rfc/rfc3986#section-2.3), so the
  blob needs no percent-encoding in a path any more than it did in a query
  string, and no segment of it can ever be read as a path separator.

**Absent means the whole log.** A bare `GET /stream` with no extra segment is
v1's behaviour exactly, and stays the default for any client that does not ask
— a browser `EventSource` pointed at the plain URL, `curl`, anything written
before selection existed.

**The `/stream` namespace, and the one cost the path form adds.** Putting the
blob in the path means `/stream/...` becomes a namespace, and a namespace can
collide. The rule is that **reserved names are matched first**: `stats` (see
[Stream Stats](#stream-stats-get-streamstats)) is a named sub-resource of the
producer, not a candidate filter, and the reserved list is closed, documented
and matched before any decode is attempted. That ordering is what makes the
guarantee structural rather than probabilistic — though it is worth knowing
that the two vocabularies could hardly collide anyway, since the shortest valid
blob is a 64-bit seed plus at least one fingerprint, i.e. ≥ 12 base64url
characters, and every reserved name is a short readable word. A segment that is
neither a reserved name nor a decodable blob is a `400`, the same standard
`Last-Event-ID` and the pacing headers are held to.

**The blob.** The filter's `{seed, fingerprints}` tuple — the 64-bit seed
followed by the fingerprint array, big-endian — encoded as **unpadded
base64url** ([RFC 4648 §5](https://www.rfc-editor.org/rfc/rfc4648#section-5);
standard base64's `+` and `/` would need percent-encoding and `=` padding is
noise in a URL). No length field is needed: the array length falls out of the
decoded byte count given the fixed fingerprint width, and the segment length
the three hash positions are derived from falls out of that. A blob that is not
valid base64url, or whose decoded length is not a seed plus a whole number of
fingerprints, is a `400`.

**The encoding must be pinned bit-for-bit.** The blob carries only the seed
and the array; everything else — the fingerprint width, the hash
construction, the byte order, and the fact that the hashed key is the UTF-8
`message_type` string — is fixed by DMEx and carried by neither side. None of it
may be negotiated. This is the one place in the protocol where producer and
consumer must agree exactly, because disagreement does not produce a garbled
filter — it produces **false negatives**, which is silent message loss and
precisely the failure the whole scheme was chosen for not having. It also has
to be written down as a spec rather than left to the Elixir implementation,
since a non-Elixir consumer has to reproduce it exactly.

The cost, stated plainly: the blob lands in every access-log line of every
intermediary, and the request-line limit (around 8 KB in both nginx and
Apache by default) becomes the ceiling. At xor16 that is on the order of two
thousand message types, and even a conservative 2 KB budget leaves several
hundred. Note this is exactly **why the filter is load-bearing**: spelled-out
module names at 25–40 bytes each would exhaust a 2 KB request line at around
sixty types, which is an entirely plausible handler. The filter
is what makes the URL viable, and the URL is what makes browser consumers
viable.

**What a browser still cannot do is pace itself.** `DMEx-Batch-Window` and
`DMEx-Max-Batch-Size` remain headers, so an `EventSource` takes the producer's
defaults (see [Transport](ARCHITECTURE.md#transport-sse-over-http)). That
asymmetry is the rule working rather than an inconsistency in it: pacing is a
tuning knob whose defaults have to be sane anyway, while selection decides what
crosses the wire at all, and a client with no way to express it pays for that
on every message for as long as it stays connected.

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

- **Subscribe** — no new option, and no new declaration: the derived message
  list gains a second role, and `DMEx.Consumer.Stream` builds the wire values
  from it.
- **Transport** — the filter path segment, per the wire form above. Fixed for
  a connection's lifetime, for the same reason pacing is: a consumer wanting a
  different selection reconnects.
- **Read path** — the loop's query grows one predicate
  (`WHERE message_type = ANY($1) AND id > $2`), which brings back both the
  composite-index requirement (`(message_type, id)`) and the sparse-cursor
  question retired in [Producer side](ARCHITECTURE.md#producer-side).
- **Wakeups** — the routing key is the published row's own `message_type`, and
  connections register under the types they selected. That is *exact*: a
  publish wakes precisely the connections that asked for that type, with no
  intermediate grouping to be approximate about it. This is the
  read-amplification lever described in
  [Wakeup fan-out](ARCHITECTURE.md#wakeup-fan-out).
- **Producer-side validation** — there is none, deliberately. A consumer may
  legitimately name a type the producer has never published (a consumer
  deployed ahead of the producer that will emit it), so the filter is resolved
  against whatever the producer knows and never checked for plausibility: no
  rejecting a filter that matches nothing, and emphatically no atomizing of
  any consumer-supplied string, which is the same unbounded-atom-table risk the
  consumer side already refuses to take (see
  [Message Specifications](ARCHITECTURE.md#message-specifications)). A name
  that matches nothing simply matches nothing.
- **Consumer** — the skip path in
  [Consuming API](ARCHITECTURE.md#consuming-api) becomes a defence rather than
  the normal case. A selective stream should only ever carry decodable types,
  so an unknown one now indicates a real mismatch and is worth surfacing rather
  than quietly dropping.

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
  (`SELECT DISTINCT message_type ...`, which is also what
  [Stream Stats](#stream-stats-get-streamstats) reports) goes stale silently: a
  producer's code can know a type it has not yet emitted, and the first
  emission of it involves no restart and no reconnect, so an open connection
  keeps an expansion missing a type its consumer genuinely selected. Expanding
  over the producer's own compiled message modules has no such gap — that set
  changes only when the producer deploys, and a deploy takes down the web
  server and with it every SSE connection, since the request process *is* the
  stream (see [Producer side](ARCHITECTURE.md#producer-side)). Every affected
  consumer reconnects and re-expands against the new set, so the repair is
  forced by the same property that makes producer restarts uneventful, with no
  re-expansion machinery to build or get wrong.

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
  question (see [Open Questions](ARCHITECTURE.md#open-questions)), not to this
  one.
- **Wakeup routing.** With expansion, routing works exactly as it would for an
  explicit list of types. Without it, the `Dispatcher` must test every
  connection's filter against each published type — a few hashes per
  connection per publish, cheap in absolute terms but `O(C)` again, which is
  the amplification the routing existed to remove.
- **Whether the producer echoes the resolved type list.** If it expands, it
  already knows exactly which types tested positive, so it can echo them on
  the `200` the same way it echoes the pacing values it actually applied (see
  [Transport](ARCHITECTURE.md#transport-sse-over-http)). That would hand back
  the observability an opaque filter destroys — an explicit list says what a
  subscription is at a glance in a proxy log, and a base64 blob says nothing
  and cannot be reversed. With an echo a consumer can see its own subscription
  resolved, spot a false-positive type that is flooding it, and detect an
  encoding mismatch immediately rather than through missing messages. Open:
  echo the full list (bounded by the producer's own type count, but
  potentially large), a count, or a digest — and whether it is always on or a
  debug affordance.
- **The default fingerprint width, and whether consumers may choose it.**
  xor16 is the sensible default for the reason given in
  [the wire form](#the-wire-form-an-xor-filter-in-the-path), but a consumer
  that knows its producer's traffic shape may rationally want xor8, and one
  reading a log with a single enormous type may want wider. If it is selectable
  it has to be carried alongside the blob — a second path segment, or a leading
  byte inside it — and validated, which is one more thing that must not be got
  wrong. A leading byte is the tempting option and the risky one: it is
  indistinguishable from a filter whose seed happens to start that way unless
  the framing is pinned as carefully as the hash construction is.
- **Request-line limits.** URLs are capped by the web server and by every
  intermediary — nginx's `large_client_header_buffers` and Apache's
  `LimitRequestLine` are both around 8 KB by default — which at xor16 plus
  base64url puts the practical ceiling in the low thousands of message types.
  That is far beyond any plausible handler's list, but the number should
  be documented and the limits configured deliberately rather than discovered
  when someone crosses one. At least the failure is loud: a `414` at connect
  time, not a silently truncated selection.
- **Filter construction failure.** XOR construction is randomized and can fail
  to peel, retrying with a fresh seed; it succeeds with high probability and
  the sets here are tiny, so this is a consumer-side detail rather than a
  protocol one. Open only in that a bounded retry count, and what to do when
  it is exhausted, want specifying — falling back to requesting the bare
  `/stream` (whole-log delivery, i.e. v1) rather than failing the subscription.
- **Whether selection should be narrowable below the derived list** — a
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
  [sharing message modules across independent applications](ARCHITECTURE.md#open-questions),
  now showing up in the URL as well as in the decode step. It is the strongest
  argument for [Stream Stats](#stream-stats-get-streamstats) shipping first, and
  it strengthens the case for the fuller schema-endpoint option discussed there.

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

`max_id` at the top level is the log's own latest id — the same number that
already rides in every frame's `data.metadata` (see
[Message Envelope](ARCHITECTURE.md#message-envelope)), repeated here so a
single request answers "how big is this log" as well as "what is in it".
`message_types` is sorted by `message_type`, so two fetches of an unchanged
log compare cleanly.

**Why it earns a place.** Four questions currently have no answer short of
reading the whole log, and one endpoint answers all of them:

- **Where to rewind to after adding a message type.** This is the concrete
  payoff, and it repairs the sharpest edge in
  [Message-Type Selection](#message-type-selection): adding a module selects
  that type from the cursor forward, so a consumer that wants the history has
  to move its cursor back — and without stats its only safe choice is *the
  beginning of the log*, replaying every already-processed message of every
  other type on the way. `min_id` for the newly-added type is exactly how far
  back it needs to go, and nothing further.
- **Whether the producer publishes the type at all.** Selection is
  deliberately unvalidated (see
  [Where it would attach](#where-it-would-attach)) — a filter naming a type the
  producer has never emitted matches nothing, silently, which is correct
  behaviour and an awful debugging experience. Stats is where "I am selecting a
  type this producer does not publish" becomes visible, out of band, without
  weakening the no-validation rule on the stream itself.
- **What the type vocabulary even is, for a non-Elixir consumer.** A consumer
  in another language has to know the fully-qualified module-name strings to
  build a filter at all, and today it learns them out-of-band or not at all.
  Stats is the smallest thing that makes a DMEx producer self-describing, and
  it stops well short of the full schema endpoint discussed under
  [Open Questions](ARCHITECTURE.md#open-questions) — it says what types exist
  and where they live, never what fields they have.
- **Whether `start: :latest` is about to skip something.** An operator pointing
  a new consumer at a producer can see how much history exists per type before
  deciding between `:beginning`, `:latest`, and an explicit id (see
  [Consuming API](ARCHITECTURE.md#consuming-api)).

**What it is not.** Three limits, and the first is load-bearing:

- **It is not a source for filter expansion.** Stats reports what the *log
  contains*; expansion must resolve against what the producer's *code knows*,
  and the two differ exactly when a deployed type has not been published yet.
  Using stats as the expansion source reintroduces the staleness failure
  argued out under
  [Open, if and when it lands](#open-if-and-when-it-lands) — silent false
  negatives, the one failure mode the whole selection design is built to
  exclude. The endpoint is for humans and for consumer-side tooling, never for
  the producer's own read path.
- **It is not a schema.** No field shapes, no JSON Schema, no versioning of
  payloads. Those belong to the separate schema-endpoint question, which stats
  is compatible with and does not pre-empt: `GET /stream/schema` could land
  later beside it under the same namespace rule.
- **It is not live.** A point-in-time snapshot, computed per request. There is
  no streaming variant and no push; a caller that wants a current answer asks
  again. This keeps it entirely outside the frame/heartbeat/backpressure
  machinery — it is an ordinary JSON `200` with a body and an end, not a
  stream, which is also why nothing about SSE applies to it.

**Cost, and why it needs deciding rather than assuming.** The query is
`SELECT message_type, min(id), max(id) FROM dmex_messages GROUP BY
message_type`. In v1's schema **the primary key is the only index** (see
[Message log](ARCHITECTURE.md#message-log)), so that is a sequential scan of
the entire log — on a log big enough to make the endpoint interesting, the most
expensive query DMEx would issue anywhere, against the same database taking the
producer's writes. Two things change that, and they are the same two:

- The `(message_type, id)` composite index that
  [Message-Type Selection](#message-type-selection) needs anyway turns it into
  a per-distinct-type index probe rather than a scan — min and max of `id`
  within each type are the two ends of a contiguous index range. Postgres will
  do this well with a skip/loose scan and poorly without one, which is worth
  measuring rather than assuming.
- Caching. The answer changes only when a publish introduces a new type or
  extends a range, and callers are humans and deploy-time tooling, not a hot
  path — so a short producer-side TTL is almost free and removes the whole
  question of a consumer hammering it.

Given that, **stats is expected to ship with selection rather than before it**,
even though it is independently useful: they want the same index, and shipping
stats alone would mean either adding the index for one endpoint or serving a
full scan.

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
  [Message log](ARCHITECTURE.md#message-log)), and putting it in an endpoint
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
[Stalled connections](ARCHITECTURE.md#stalled-connections)). What neither
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
  [Transport](ARCHITECTURE.md#transport-sse-over-http)). A single advertised
  delay applied literally by every rejected consumer converts a load spike into
  a synchronized retry storm, which is precisely what admission control is
  supposed to prevent.
- **An aggregate budget beyond count.** Bytes in flight is the honest quantity
  — frames are capped per connection, so `open × frame_max_bytes` is already a
  bound, and a budget denominated the same way composes with it. Summing
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
[Stalled connections](ARCHITECTURE.md#stalled-connections)). That is what makes
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
