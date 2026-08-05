# SIMPLIFY

What DMEx could stop doing if the target is **Elixir on both ends, no browser
`EventSource`, no third-party producer or consumer, 100% control of both
halves**.

Read next to [ARCHITECTURE.md](ARCHITECTURE.md): every entry names the sentence
that justified the construct, so the "this no longer applies" step is auditable
rather than asserted. Analysis only — no code has changed.

Three decisions scope it:

| | |
| --- | --- |
| payload | **ETF end to end** — `term_to_binary` on the wire *and* in storage, `data`/`metadata` become `bytea` |
| substrate | HTTP and `DMEx.Log.Plug` stay; SSE framing → length-prefixed binary |
| id chain | invariant 12 dropped entirely |

Sections 1–2 follow from the wire assumptions and are independent of the payload
format. Section 3 is the payload decision on its own: what it deletes, the one
sub-decision it opens, and what it costs.

---

## 1. Tier 1 — follows directly

### 1.1 The XOR filter, and everything hanging off it

**`DMEx.Handler.Fingerprint` (347 lines) → deleted.** Its premise is stated at
`lib/dmex/handler/fingerprint.ex:12-16`: a namespaced `message_type` runs 25–40
bytes, sixty of them overrun a request line, and the filter "is what makes the
URL — and therefore a bare browser `EventSource` with real server-side selection
— viable at all". With one Elixir consumer the selection is a list of strings in
a header or a request body, exact, with no encoding to pin and no ceiling worth
engineering around.

What goes with it:

| | |
| --- | --- |
| `guides/fingerprint-spec.md` | 236 lines of normative encoding, existing solely so a non-Elixir consumer can reproduce it |
| `bench/fingerprint_sweep.exs` | 182 lines measuring a false-positive rate that becomes zero by construction |
| `test/dmex/handler/fingerprint_test.exs` | 208 lines |
| `dmex-matched-messages` | `log/plug.ex:93-95` — a diagnostic that exists only because "the blob cannot be reversed" |
| 404-on-empty-expansion | an explicit list needs no such signal: the producer can name the types it did not recognise |
| `selective: false` | `handler.ex:96,671-678` — an escape hatch for telling an encoding mismatch from a quiet producer. With an exact list there is no encoding to mismatch |
| the false-positive skip path | the skip path survives for the *other* reason (§4), but stops being a correctness dependency |

**`DMEx.message_types/1`, `refresh_message_types/1`, the `:persistent_term`
cache and `DMEx.Message.discover/1` (`lib/dmex.ex:174-243`) → deleted.** They
exist only to expand a filter against compiled modules — including the whole
argument at `lib/dmex.ex:180-198` for why `SELECT DISTINCT message_type` goes
stale. An explicit type list goes straight into `message_type = ANY($1)`; the
producer resolves nothing and needs to know nothing about its own modules at
connect time.

`DMEx.discovery/1` itself survives — handler discovery still uses it.

### 1.2 Framing

**`DMEx.Transport.SSE.Decoder` (125) → ~40 lines.** Gone: the eight-way
blank-line delimiter table (`decoder.ex:38`, which was itself wrong until
[SLOP.md §5](SLOP.md)), the chunk-boundary reassembly, the iodata-accumulation
trick that exists because `buffer <> chunk` is quadratic. A length prefix makes
"do I have a whole frame" a subtraction. `max_frame_bytes` survives as a check on
the length header instead of a check on a growing buffer.

**`DMEx.Transport.SSE.Frame` (210) → ~30 lines.** Encoding becomes fixed-width
slots and length-prefixed payloads. Gone: `parse_line`, `collect_field`,
`put_id`, `put_pair`, CR/LF/CRLF splitting, the `retry:` frame, the comment
heartbeat, and the two-part encoding invariant at `frame.ex:26-37` — never
pretty-print, splice only what came out of an encoder or a constrained column.
Both clauses are SSE-escaping rules and nothing else.

**The `message_type` charset constraint.** `message.ex:44` — "Restricted to
`[A-Za-z0-9_.]+`, because the type goes onto an SSE `event:` line and SSE field
lines have no escaping". That reaches the DDL in both adapters
(`storage/postgres/migration.ex:104`, `storage/sqlite/migration.ex:106`, the
latter a `NOT GLOB` workaround for SQLite having no regex operator) and
`@type_format` in `DMEx.Message`. A length-prefixed frame has no lines.

### 1.3 The request contract

**`DMEx.Log.Params` (171) → ~30 lines.** The three-form `Last-Event-ID`
vocabulary exists because a generic SSE client echoes a frame's `id:` back
verbatim and a browser sends nothing else. One Elixir consumer sends one thing.
Gone: `@digits` and its two call sites, ISO-8601 parsing, the `previous/current`
split, duplicate-header handling, `describe/1`'s error prose, and the 400s those
feed.

**`DMEx.Handler.Request` (87) → ~30 lines.** Header construction, instant
encoding, `scheme/1`, and the fingerprint path-join all go. `position/2`'s
`- 1` — the one piece of real arithmetic — survives, and so does its property of
living in exactly one place for both transports.

**Browser and proxy ceremony in `DMEx.Log.Plug`.** `text/event-stream`,
`cache-control: no-cache`, `x-accel-buffering: no` (nginx-specific),
the HTTP/1.1-only `connection: keep-alive` branch, `accept: text/event-stream`
on the client, and the `405` + `Allow: GET` clause whose stated purpose is
catching a router typo.

### 1.4 The id chain (invariant 12)

Its justification is explicit and threefold: "a mangling intermediary, a
third-party producer whose read skips rows, and
`DMEx.Transport.SSE.Frame.parse/1`, which discards any block missing `id:`,
`event:` or `data:`". Assumption two kills the second, §1.2 kills the third, and
the first is a bet you are choosing to take.

Deleted: `verify_chain`, `check_link`, `log_gap`, `expected_previous`
(`transport/sse.ex:208-261`), the `previous/current` encoding on both ends,
`:previous_id` from `t:DMEx.Storage.row/0`, the third of `c:DMEx.Storage.read/5`'s
four "correctness, not style" contracts, and
`test/integration/sequence_gap_test.exs` (124).

The concrete win is in the hot path, not the line count: **the
`lag(id, 1, $1) OVER (ORDER BY id)` window function comes out of the read query
in both adapters** (`storage/postgres.ex:93`, `storage/sqlite.ex:197`) — a
window function evaluated on every row of every batch of every connection,
purely to feed a check that no longer runs.

The cost, stated plainly: ARCHITECTURE's "No-loss is checked, not assumed"
becomes "no-loss is assumed". The cursor is then the only record of what was
processed, and a row lost between two that arrived has its id put permanently
behind the cursor by the next advance — silently. TCP does not drop bytes
mid-stream, so what remains uncovered is a bug in DMEx's own read path and
anything in the network that is not TCP.

---

## 2. Tier 2 — unlocked, but a judgement call

**`type:` → the module name.** With the same modules on both ends, the declared
string looks redundant with `%MyApp.Messages.OrderPlaced{}` itself. Dropping it
would kill `Message.types!/1`, the collision raise, the `@dmex_message_type`
marker and `message?/1`.

**Recommend keeping it, and §3.4 makes the case stronger, not weaker.** It costs
one line per module and buys invariant 13 — renaming or moving a module is free.
Under ETF it is also what keeps a stored payload resolvable to *whichever* module
currently declares that type, which is the whole of §3.4. The machinery *around*
`type:` (discovery, expansion, the cache) dies with the fingerprint regardless,
which is where the weight actually was.

**Atom-safety machinery.** `cast_default`'s field-walk (`message.ex:248-264`)
and the "never `String.to_atom/1` on whatever the log sent" rule
(`pipeline.ex:257-258`) assume an untrusted peer. The peer is now trusted — but
the network between them is not, and §3.5 shows ETF makes this *more* load-bearing
rather than less. **Recommend keeping the posture**; what changes is that it is
spelled `binary_to_term(bin, [:safe])` instead of a hand-rolled field walk.

**Raw TCP with `{:packet, 4}`** — the road not taken. It would delete the
decoder *entirely* rather than shrinking it: the VM does framing and buffer
bounding as a socket option, `packet_size` replaces `max_frame_bytes`, and
`active: :once` gives the same exact backpressure invariant 8 already relies on.
The cost is HTTP: TLS termination, auth, proxies, and the `forward "/stream"`
mounting story all become yours again. Rejected for that reason, recorded
because the trade is close.

**Dropping SQLite, or the notifier axis** — adjacent, and **not** implied by
these assumptions. Noted only so it is not conflated with them.

---

## 3. ETF end to end

`data` and `metadata` become `bytea` (`blob` in SQLite) holding
`term_to_binary` output, spliced verbatim into frames exactly as the JSON text
is today. The wire format and the storage format stay coupled, which is what
keeps the splice a memcpy.

### 3.1 What it deletes

- **`c:DMEx.Message.cast/1` — the callback, `cast_default/2`, `defoverridable`,
  and every user-written override.** It exists because "the default can only be
  right for what JSON models directly" (`message.ex:55-70`): a `DateTime` comes
  back a string, a `Decimal` a string, a nested struct a plain map. ETF has no
  such gap.
- **`@derive JSON.Encoder` from the generated struct**, and with it three
  constraints that are pure JSON-protocol facts: the derive must precede
  `defstruct`; a module defined at runtime cannot be derived at all; a second
  `@derive` is a silent no-op so `defimpl` is the only override
  (`message.ex:20-24,82-85`).
- **The `Protocol.UndefinedError` footgun** (`message.ex:72-80`) — a nested
  struct that derives nothing raises inside `append/2`, before the row is written
  and before the telemetry event. It is the sharpest edge in the message API and
  `term_to_binary` encodes any term.
- **`use DMEx.Message` collapses from ~230 lines of macro to ~40**: the marker
  attribute, `defstruct`, `type/0`, `t/0`.
- **`DMEx.Log.merge_metadata/2` and its ~40 lines of JSON text surgery** —
  `splice/2`, `pairs/2`, three `pair/2` clauses, two `to_iso8601/1` clauses. See
  §3.3.
- **`Pipeline.encode/1`** (`handler/pipeline.ex:401-402`), which re-encodes a
  decoded payload before writing a dead message. The received bytes are stored
  verbatim instead, so a dead message becomes byte-identical to what arrived.
- **`Transport.Local.row/1`'s two `JSON.decode!` calls** → one `binary_to_term`.
- **One of `c:DMEx.Storage.read/5`'s four contracts shrinks to nothing.**
  `storage.ex:44-52` warns that "an adapter that lets its driver decode a JSON
  column into a map hands `Frame` a map where it expects text". A `bytea`/`blob`
  driver returns a binary, always — the hazard is structurally impossible and the
  contract becomes "return the stored bytes".

### 3.2 What it fixes

- **Exact term fidelity.** `DateTime`, `Decimal`, atoms, tuples, nested structs
  and raw binaries all round-trip. The rule that "enrichment past that line
  belongs on the read side, in `cast/1`" disappears because there is nothing left
  to enrich.
- **"Payload byte fidelity" closes** — an ARCHITECTURE open question, not a
  nice-to-have. Today `jsonb` normalizes key order, drops duplicate keys and
  loses whitespace, and the envelope is re-serialized from storage, so "the bytes
  a consumer receives are not the bytes the publisher encoded". Under ETF they
  are, on every hop, which also makes end-to-end signing of raw payload bytes
  possible where the question said it explicitly was not.
- **The splice stays a memcpy.** `c:read/5` returns binaries, the frame splices
  them, and "paid once per connection for the same rows" still holds. This is
  the property that a JSON-storage/ETF-wire split would have thrown away.

### 3.3 The metadata lift stops being string surgery

`merge_metadata/2` splices lifted columns into the metadata JSON *text*
precisely to avoid a decode-and-re-encode per row. An encoded term cannot be
spliced into that way — but it does not need to be: the lift set is *log*
configuration, fixed for a connection's lifetime, so it goes in the hello frame
once and the consumer assembles the map from fields the frame already carries
(`id`, `message_type`, and `inserted_at` as a fixed-width slot). Per-row cost
goes from string surgery to zero, and `merge_metadata/2` and its helpers delete.

Invariant 11 is unaffected — the handler still sees a message and a metadata map,
and the lift still changes what is in the map and never the callback's shape.

### 3.4 Store the struct, or store its fields?

The one sub-decision this opens, and it matters more than it looks.

- **(a) `term_to_binary(message)`** — the hardest collapse. But the stored bytes
  embed the module name, so **renaming a message module orphans every row already
  appended**, even though `type:` still keys the row. That retracts invariant 13
  for payloads: renaming stops being free and becomes a data migration over
  `dmex_messages`.
- **(b) `term_to_binary(Map.from_struct(message))`**, rebuilt as
  `struct(module, fields)` with the module resolved from `type:` exactly as it is
  resolved today. Keeps invariant 13 intact and every fidelity property in §3.2,
  and `cast/1` still goes — replaced by one `struct/2` call in the pipeline with
  nothing per-message-module to write, derive or override.

**Recommend (b).** It costs one `struct/2` per row and preserves the property the
design deliberately paid for. Either way one caveat holds: a *nested* struct
embeds its own module name, so rename freedom is top-level only.

### 3.5 The costs

- **SQL cannot see inside a payload.** No `data->>'order_id'`, no GIN index, no
  ad-hoc query. `storage/postgres/migration.ex:52` states the property being
  traded away in as many words — "jsonb so operators can query inside payloads
  directly". README's "technologically simple — a SQL table and an HTTP endpoint"
  needs qualifying: the table stays ordinary, the payload column becomes opaque.
- **Anything reading the table that is not this Elixir application goes blind** —
  CDC into a warehouse, Metabase, a Grafana SQL panel, an ops script.
  "No third-party consumers" was an assumption about the *wire*; this reaches
  *operator* tooling, which is a separate bet and worth making consciously.
- **An inspection tool stops being optional.** A `mix dmex.inspect` or a
  `MyApp.Log.inspect(id)` doing `binary_to_term` is ~20 lines and has to exist
  before this lands, covering `dmex_messages` and `dmex_dead_messages` both.
- **`[:safe]` decoding requires the module loaded first.**
  `binary_to_term(bin, [:safe])` is not optional — the peer is trusted, the
  network is not — and it refuses to create atoms. A message module's struct name
  and field atoms enter the atom table when that module is *loaded*, so a
  subscription must `Code.ensure_loaded!/1` every module in its compile-time
  message set at start. Cheap, but new, and getting it wrong surfaces as an
  `ArgumentError` from the decoder rather than as a clear error.
- **Atom *values* in payloads decode only if the consumer already knows them.**
  `status: :paid` needs `:paid` to exist in the consumer's atom table, which it
  does only if some loaded consumer code mentions it. JSON's constraint was "no
  atoms at all" and was enforced at encode time; ETF's is subtler and fails at
  decode time, in production, on the one message that carried a value no clause
  matches. The working rule for message fields: atoms for values the consumer's
  own clauses match on, strings otherwise.
- **Payload size is unmeasured.** ETF is usually more compact than JSON text for
  numeric payloads and less so for string-heavy ones, and `jsonb` has its own
  overhead. No claim either way without a `bench/` artifact — the mistake
  [SLOP.md](SLOP.md) exists to record.

---

## 4. Not droppable, whatever the assumptions

Stated explicitly so this document cannot be read as license.

**Invariants 1–11 and 13 all survive untouched.** They are properties of
Postgres, the BEAM and TCP, not of the wire or payload format: the append
advisory lock and its assignment-order-equals-commit-order requirement; one batch
of memory per connection bounded in rows and bytes; reads gated on sends
completing; the request process *being* the stream; every blocking step under a
deadline with `DMEx.Log.Watcher` behind it; a wakeup as a hint and never a
carrier; a drop not being a delivery event; the consumer having no buffer; pacing
having one owner; batching being a write batch; the handler seeing a message and
a map.

Also surviving:

- **The skip path.** Selection is by type while a clause head may narrow further
  — `%OrderPlaced{status: "paid"}` declares `OrderPlaced` and no producer can
  send only the paid ones. Losing the filter's false positives does not make this
  dead code.
- **Type-to-module resolution.** Under §3.4(b) the pipeline still resolves
  `message_type` to a module before building the struct, so `__dmex_cast__/3`'s
  compiled dispatch stays — it just stops calling a per-module `cast/1`.
- **Duplicate suppression by id**, and the one-transaction handler/cursor commit
  that at-least-once rests on.
- **Jittered backoff.** Correlated reconnects are a property of many consumers
  and one producer restart, not of the wire format.

---

## 5. Score

| | lines |
| --- | --- |
| `lib/` today | 8 603 |
| deleted outright | ~940 (`fingerprint.ex` 347, the id chain, `dmex.ex`'s expansion block ~100, `message.ex`'s cast/derive half ~200, `merge_metadata` and helpers ~40) |
| replaced by something much smaller | ~1 000 → ~250 (`frame.ex` 210, `decoder.ex` 125, `params.ex` 171, `request.ex` 87, plus the `plug.ex` and `sse.ex` paths that feed them) |
| `test/` today | 11 022 |
| tests deleted | ~1 280 (fingerprint 208, frame 234, decoder 222, params 144, request 134, wire contract 216, sequence gap 124), plus the `cast/1` half of the message suite |
| guides | `fingerprint-spec.md` (236) deleted outright |
| bench | `fingerprint_sweep.exs` (182) deleted outright |

Roughly **a quarter of `lib/` and a tenth of the suite**, concentrated in three
subsystems: the filter, the framing, and the message codec.

**ARCHITECTURE.md sections that get rewritten:** *Message-type selection*
(entirely), *The wire contract* (entirely), *The log*'s DDL block and its
`data jsonb` reasoning, invariant 12 (deleted), *Delivery semantics*' "No-loss
is checked, not assumed" (retracted). In `DMEx.Storage`: the module's third
contract ("`read/5` must return `data` and `metadata` as JSON *text*") is
rewritten to "return the stored bytes", and `read/5`'s own third bullet ("every
row states its predecessor") is deleted.

**Open questions that close:** *Payload byte fidelity* — §3.2, and it closes in
the strong direction, with signing becoming possible. *Third-party client
backpressure* — invariant 8 becomes a property of the only client there is.
*Sharing message modules across independent applications* — resolved by
assumption, since both ends compile the same modules; what survives is the
`message_type` uniqueness note, which `type:` already makes a producer's own
choice.

**Open questions that do not close:** *auth/access control*, *cross-node
clustering*, *retention/compaction*, *global resource limits*, and every pacing
question — none of them were about who was on the other end.
