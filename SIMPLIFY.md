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
| payload | ETF on the wire; `data`/`metadata` stay `jsonb` in storage |
| substrate | HTTP and `DMEx.Log.Plug` stay; SSE framing → length-prefixed binary |
| id chain | invariant 12 dropped entirely |

Section 3 is the one place a decision does not buy what it looks like it buys.

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

**`DMEx.Transport.SSE.Frame` (210) → ~30 lines.** Encoding becomes
`<<byte_size(bin)::32, bin::binary>>`. Gone: `parse_line`, `collect_field`,
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
`:previous_id` from `t:DMEx.Storage.row/0`, and
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
string is redundant with `%MyApp.Messages.OrderPlaced{}` itself. It would kill
`Message.types!/1`, the collision raise, the `@dmex_message_type` marker,
`message?/1`, and `handler.ex:446`'s per-type `__dmex_cast__/3` dispatch on a
string.

**Recommend keeping it.** It costs one line per module and buys invariant 13 —
renaming or moving a module is free, and producer and consumer refactor on their
own schedules. Dropping it makes a module rename a data migration over
`dmex_messages`. The machinery *around* `type:` (discovery, expansion, the
cache) dies with the fingerprint regardless, which is where the weight actually
was.

**Atom-safety machinery.** `cast_default`'s field-walk (`message.ex:248-264`)
and the "never `String.to_atom/1` on whatever the log sent" rule
(`pipeline.ex:257-258`) assume an untrusted peer. The peer is now trusted — but
the network between them is not, so ETF decoding wants
`:erlang.binary_to_term(bin, [:safe])` either way. **Recommend keeping the
posture, dropping nothing here**; it is one option flag, not a subsystem.

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

## 3. Where ETF-on-the-wire-with-`jsonb`-in-storage stops short

The wire format and the storage format are coupled by the splice, and picking
different ones for each keeps the coupling's cost while giving up its benefit.

The round trip is still:

    struct → JSON → jsonb → map → ETF → map → struct

So:

- **`c:DMEx.Message.cast/1` cannot be deleted.** A `DateTime` field still
  arrives as a string, a `Decimal` as a string, a nested struct as a plain map.
  Exact struct fidelity is gated on the *storage* format, not the wire format.
  `@derive JSON.Encoder`, the `Protocol.UndefinedError` footgun on a nested
  struct, and `cast/1`'s overridability all stay exactly as they are.
- **The verbatim splice is gone.** `storage.ex:44` — "`c:read/5` must return
  `data` and `metadata` as JSON *text*" — exists so `encode_row/1` is a memcpy,
  "paid once per connection for the same rows". ETF framing replaces that with a
  JSON decode plus a `term_to_binary` per row per connection, and that callback
  contract has to be restated.
- **"Payload byte fidelity" stays open**, since `jsonb` still normalizes key
  order, drops duplicate keys and loses whitespace.

What it does buy: no charset or escaping constraint anywhere on the wire,
arbitrary binaries in payloads, and one fewer parse on the consumer.

**The upgrade is isolated and stays available.** Moving `data`/`metadata` to
`bytea` touches the two storage adapters, the migration, and `DMEx.Message` —
nothing else — and at that point all three bullets above invert: `cast/1`, the
derive, the footgun and the fidelity question all go, and the splice is a memcpy
again. What it costs is `SELECT data->>'order_id'` in psql, and the "one
ordinary SQL table" claim in the README.

If the JSON-in-storage property is worth keeping, **JSON on the wire is the
cheaper answer than ETF on the wire** — every §1 simplification holds either
way, because none of them are about JSON. They are about SSE.

---

## 4. Not droppable, whatever the assumptions

Stated explicitly so this document cannot be read as license.

**Invariants 1–11 and 13 all survive untouched.** They are properties of
Postgres, the BEAM and TCP, not of the wire contract: the append advisory lock
and its assignment-order-equals-commit-order requirement; one batch of memory
per connection bounded in rows and bytes; reads gated on sends completing; the
request process *being* the stream; every blocking step under a deadline with
`DMEx.Log.Watcher` behind it; a wakeup as a hint and never a carrier; a drop not
being a delivery event; the consumer having no buffer; pacing having one owner;
batching being a write batch; the handler seeing a message and a map.

Also surviving:

- **The skip path.** Selection is by type while a clause head may narrow further
  — `%OrderPlaced{status: "paid"}` declares `OrderPlaced` and no producer can
  send only the paid ones. Losing the filter's false positives does not make
  this dead code.
- **Duplicate suppression by id**, and the one-transaction handler/cursor
  commit that at-least-once rests on.
- **Jittered backoff.** Correlated reconnects are a property of many consumers
  and one producer restart, not of the wire format.

---

## 5. Score

| | lines |
| --- | --- |
| `lib/` today | 8 603 |
| deleted outright | ~700 (`fingerprint.ex` 347, the id chain, `dmex.ex`'s expansion block ~100) |
| replaced by something much smaller | ~1 000 → ~250 (`frame.ex` 210, `decoder.ex` 125, `params.ex` 171, `request.ex` 87, plus the `plug.ex` and `sse.ex` paths that feed them) |
| `test/` today | 11 022 |
| tests deleted | ~1 280 (fingerprint 208, frame 234, decoder 222, params 144, request 134, wire contract 216, sequence gap 124) |
| guides | `fingerprint-spec.md` (236) deleted outright |
| bench | `fingerprint_sweep.exs` (182) deleted outright |

Roughly **a fifth of `lib/` and a tenth of the suite**, concentrated almost
entirely in two subsystems: the filter and the framing.

**ARCHITECTURE.md sections that get rewritten:** *Message-type selection*
(entirely), *The wire contract* (entirely), invariant 12 (deleted), *Delivery
semantics*' "No-loss is checked, not assumed" (retracted).

**Open questions that close:** *Third-party client backpressure* — invariant 8
becomes a property of the only client there is. *Sharing message modules across
independent applications* — resolved by assumption, since both ends compile the
same modules; what survives of it is the `message_type` uniqueness note, which
`type:` already makes a producer's own choice.

**Open questions that do not close:** *Payload byte fidelity* (§3),
*auth/access control*, *cross-node clustering*, *retention/compaction*, *global
resource limits*, and every pacing question — none of them were about who was on
the other end.
