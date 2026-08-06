# SLOP

Defects surfaced by the documentation review that are **not** documentation
problems. The original list was written without an Elixir toolchain and asked
for verification before fixing.

Every item has now been verified by running it against a real Postgres. Eight
were real and are fixed; one was a false positive; three further defects turned
up during verification and are fixed too. Status per item below, then
[what remains](#what-remains).

---

## 1. `mix bench.e2e` could not produce three of the figures ARCHITECTURE quotes

**Confirmed, and worse than reported: `catchup_drain` could not finish at all.**
The report identified stage (c) as hanging. Stage (a) hung *first*, so (b) and
(c) never ran on any machine — see 1e.

The drift in the *numbers* has one root cause: `DMEx.Test.IntegrationLog` is now
`max_batch_frequency: 100`, and ARCHITECTURE's figures were recorded at `5`.
That reproduces both old numbers exactly — a 200 ms window is 5 writes/s, and
20 000 rows at 500 per read over 40 reads at 5/s is ~2 500 rows/s.

### 1a. `fanout_reads` counted an event nothing emits — fixed

`[:dmex, :stream, :frame]` does not exist; the send loop emits
`[:dmex, :stream, :write]` with a `:rows` measurement. Now attached to that,
counting writes and summing rows into separate slots.

### 1b. `fanout_reads`'s pool-contention probe recorded nothing — fixed

`:counters.add(counter, 2, 0)` added zero, and to the *query* slot. The counter
is now four slots wide with queueing in its own, and the threshold is
`System.convert_time_unit(5, :millisecond, :native)` rather than a bare
`5_000_000`.

**This overturns the ARCHITECTURE claim it was supporting.** Measured over two
runs, pool queueing appears well before 200 connections: at C=200/P=100, ~50 000
of ~61 000 queries waited over 5 ms and per-connection writes fell from ~66/s to
~47/s. At C=50, ~120 of ~30 000; at C=10, none. ARCHITECTURE now states this
instead of "no pool queueing at 200 concurrent streams".

### 1c. `catchup_drain` stage (c) hung — fixed

Confirmed exactly as reported: the runtime-defined `Bench.UnusedMessage` cannot
appear in `DMEx.Message.types/0`'s scan of *compiled* application modules, so a
selective fingerprint matched zero types → `404` → retry forever. Now
`selective: false`, which the file's own header already documented as stage (c)'s
design.

### 1d. `fanout_reads`'s "5.0/s" label — fixed

Hardcoded *and* wrong by 20×: the log is configured at 100/s. Now read from
`Log.__dmex__().config.max_batch_frequency`, and the line says writes/s rather
than frames/s.

### 1e. `catchup_drain` stage (a) never counted a single row — fixed (not in the original report)

Stage (a) counted `"id":` in the chunk stream. The row's id travels in the SSE
`id:` field; the JSON payload has no `"id"` key, and this
benchmark's rows carry `order_id`. Verified directly: **710 690 bytes arrived and
`seen` stayed 0.**

It did not fail loudly because the `after 30_000` timeout was longer than the
15 s heartbeat interval, so heartbeats reset it forever. The stage spun until
killed, which is why (b) and (c) never ran.

Fixed by counting `data: {"data":` — one per row frame, absent from heartbeats
and the `retry:` frame — carrying the chunk tail across boundaries so a marker
split by a chunk boundary is not lost, and shortening the timeout below
`heartbeat_ms` so a miscount reports instead of hanging.

### 1f. The coalescing ratio was fabricated when no checkpoint fired — fixed (not in the original report)

`Float.round(backlog / max(written, 1), 1)` printed "20 000 rows per write" —
perfect coalescing — for a run where **zero** cursor writes happened.

Stage (c) now has its own `skip_backlog` of 200 000, because the two stages want
opposite things: (b) is commit-bound and 20 000 rows already costs 75 s, while
(c) runs at wire speed and drained 20 000 *inside* one `checkpoint_interval`.
The ratio is a measurement again — **200 000 skipped rows cost 5 cursor
writes**, one per checkpoint across a 5.15 s drain. The zero branch survives for
the case it describes and says why rather than inventing a number.

### 1g. Both remaining drains waited forever — fixed (not in the original report)

Stages (b) and (c) spun on `:counters.get/2` with no deadline, which is how 1c
presented as a hang instead of an error. Both now share a bounded `await` that
returns what it actually saw and prints `!! (c) reached only N of M`. It earned
its keep immediately: a contaminated run reported `7000 of 200000` in 300 s
instead of hanging, which is how I caught that the integration suite and the
benchmarks share `dmex_integration_test` and **cannot be run concurrently** —
`DMEx.IntegrationCase` truncates on every test setup, mid-drain.

### Result

`catchup_drain` completes, stable across four runs: (a) ~36 000 rows/s,
(b) ~265 rows/s, (c) ~39 000 rows/s, 5 cursor writes for 200 000 skips.
ARCHITECTURE updated.

## 2. `DMEX_POSTGRES_URL` could silently point both suites at one database — fixed

Confirmed. The derivation now replaces the whole database segment via
`URI.parse/1` rather than rewriting a trailing `/dmex_test`, so it holds for any
name while preserving host, port, credentials and query. Verified against
`.../dmex_test`, `.../whatever` and `postgres://h/db`. An explicit
`DMEX_POSTGRES_INTEGRATION_URL` equal to the unit URL now raises at config load
rather than truncating the unit database.

## 3. `DMEx.Log.Plug` arms the same deadline three times at connect — **false positive**

**The report's premise is wrong.** `resolve_cursor/2` sits between the
registration and the arm at line 164, and for an absent `Last-Event-ID` — which
`DMEx.Log.Params` resolves to `:now` — it issues a `max_id` query, which takes a
pool checkout like any other read. The arm is not
dead: the registration's arm covers that query, and this one gives the header
write its own full budget. Commented rather than removed.

## 4. `DMEx.Handler.Stream` tracked a timer reference it never reads — fixed

Confirmed. `:checkpoint_timer` was written and never read or cancelled. Dropped
the field; `schedule_checkpoint/1` no longer stores the reference, and a comment
records why this timer differs from `:liveness_timer`.

## 5. `DMEx.Transport.SSE.Decoder` missed legal SSE blank lines — fixed

Confirmed, and **five sequences were missing, not two**. With CR, LF and CRLF as
terminators, a blank line is any two back to back except CR+LF, which the grammar
reads as one CRLF — eight sequences in total. The decoder had three. Added
`"\r\r\n"`, `"\n\r\n"`, `"\r\n\r"`, `"\r\n\n"` and `"\n\r"`.

Safe because `:binary.match/2` takes the leftmost match and the longest at that
position, so `"\r\n\r\n"` still beats the `"\r\n\r"` starting on the same byte,
and `"\r\n\n"` beats the `"\n\n"` one byte later — both verified. Covered by
three new tests, including that a lone CRLF does *not* end a frame.

## 6. `DMEx.Test.Storage` diverged from the behaviour it stands in for — fixed

- `fail/3` and `hang/2` took an ignored `fun`. Dropped it rather than honouring
  it: reads are the only call these tests inject faults into, and an argument
  naming a callback that is never consulted is a silent no-op.
- `dead_messages/2` did not return `t:row/0`. It now stamps `:inserted_at` in
  `dead_message/2` — the adapter's job, as the callback docs say — and returns
  exactly the `t:row/0` keys, matching what the Postgres adapter does.
- `dead_messages/1`, `appends/1` and `add_rows/2` had no callers; removed, along
  with the `:appends` field that only they fed.

## 7. Handler discovery was not exercised by the integration suite — fixed

Confirmed. `DMEx.Test.Server.logs/1` now binds `:other` alongside `:test`, which
is what makes discovery safe to turn on — `Fanned` names `:other`, and a
discovered handler naming an unbound log stops the boot.

New `test/integration/discovery_test.exs` starts a supervisor with
`otp_app: :dmex` and asserts a handler nobody subscribed delivers end to end, and
that `Fanned` commits a cursor per subscription.

## 8. Smaller items

- **`Decoder.new/1`'s default was a third copy of `max_frame_bytes`** — fixed.
  It now reads `DMEx.Config.defaults().handler[:max_frame_bytes]`. Note it is a
  *handler* default, not a log one. Removing the default outright was rejected:
  ~15 test and benchmark call sites use `Decoder.new()`.
- **`DMEx.Storage.Migration.down/1` never drops a `:prefix` schema** — confirmed
  intentional. `up/1` creates it with `IF NOT EXISTS`, so it may have existed
  first and may hold tables that are not DMEx's. The reason is now in the doc.
- **`bench_helper.exs`'s `reset_log!/0` did not truncate `dmex_dead_messages`** —
  fixed; it truncates all three tables.

## 9. `max_batch_frequency` now defaults to 100 (follow-up decision, not a report item)

The report's open question — whether the benchmark log should keep pacing 20×
faster than the default — was settled by **raising the default from 5 to 100**
rather than slowing the benchmark down. `DMEx.Test.IntegrationLog`'s override is
gone; it takes the default like every other log, which is what stops the two
drifting apart again.

Consequences, all deliberate:

- Added latency `1 / 2f` drops from 100 ms to 5 ms.
- `effective_heartbeat` drops from `heartbeat_ms + 200` to `+ 10`.
- **Worst-case reads per second per connection goes up 20×.** This is the same
  pressure 1b measured queueing at, so the default now sits on the near side of
  the pool ceiling: fine for a handful of streams, and the first thing to lower
  for many. `DMEx.Config`, ARCHITECTURE invariant 9 and `guides/deployment.md`
  all say so explicitly now, with the measurement to back it.

`test/integration/heartbeat_test.exs` asserted `@heartbeat + 200`. It now derives
the interval from `DMEx.Config.batch_interval/1` — a literal there was the same
kind of latent drift as the figures this whole exercise was about.

---

## Verification

Run after `4fee83e` (the explicit-`:type` work, which landed separately and is
not part of these changes):

| | |
| --- | --- |
| `mix test` | 501 passing |
| `mix test.integration` | 72 passing |
| `mix dialyzer` | 0 errors |
| `mix format --check-formatted` | clean |
| `bench/e2e/fanout_reads.exs` | runs; every column non-zero |
| `bench/e2e/catchup_drain.exs` | all three stages report; coalescing ratio measured |
| `bench/e2e/append_throughput.exs` | runs; all four concurrency levels |
| `mix bench.fingerprint` | runs; 0 false negatives, every figure reproduced |

Run the suites and the benchmarks **serially** — they share
`dmex_integration_test`, and the suite truncates it.

---

## The rest of *What has been measured*, re-verified

The earlier round left ARCHITECTURE's other figures resting on their original
single recording — the same footing that made the first three wrong. They have
now been re-run. All three hold; only the append-lock numbers moved, and only
within noise.

| claim | recorded | re-measured |
| --- | --- | --- |
| append lock, concurrency 1 / 2 | ~1.13× | 1.11× / 1.12× |
| append lock, concurrency 8 | ~4.8× | 4.55× |
| append lock, concurrency 32 | ~5.8× | 5.77× |
| locked throughput falls with concurrency | yes | 252 → 134 → 32 → 8 appends/s |
| fingerprint false negatives | 0 / 45 660 | 0 / 45 660 |
| fingerprint false positives | 14.65 ± 1.85 per million | identical (fixed seed) |
| bytes per type | 2.49 raw, 3.33 base64url | 2.493 / 3.329 |
| `recovered/encoded` | 1.0 at 84/86, worst 1.00069 | identical |
| request-line ceiling | ~2 400 types | ~2 460 |

The **`(type, id)` index claim** had no artifact in the repo at all — one
whole-log `EXPLAIN` assertion in `postgres_test.exs`, nothing for the skewed
case. Re-derived against a fresh 500 000-row log at 99% / 0.9% / 0.1%:

- 0.1% (500 rows) → `Index Scan using dmex_messages_type_id_index`
- 0.9% (4 500 rows) → `Index Scan using dmex_messages_pkey`
- 99% (495 000 rows) → `Index Scan using dmex_messages_pkey`

Exactly the documented crossover. ARCHITECTURE's append-lock line is updated to
the re-measured values; nothing else needed changing.

---

## What remains

Nothing from the report is outstanding, and the follow-ups are closed. What is
left is scope I deliberately did not take on.

1. **The benchmark numbers are developer-machine, small-sample.**
   `catchup_drain` was reproduced four times and pool queueing three, so the
   shapes are solid; the constants are not. The pool-contention figures in
   particular scale with `pool_size: 10` rather than with anything DMEx chooses,
   and the append-lock and index results are single runs on one Postgres.

2. **The buffer half of the index claim is still unverified.** ARCHITECTURE says
   the primary-key plan wins at ≥1% selectivity "at fewer buffers than the index
   plan would cost". The *plan choice* is confirmed; comparing buffer counts
   against a forced index plan needs planner hints the database does not have
   installed, so that clause rests on the original analysis.

3. **The integration suite and the benchmarks share one database and will
   corrupt each other if run at the same time.** `DMEx.IntegrationCase`
   truncates on every test setup; a benchmark mid-drain sees its log vanish.
   Found the hard way (see 1g). Nothing warns you — separating them would mean a
   third database or a lock, which is a bigger change than this pass warranted.

4. **`end_to_end_latency.exs` and the six `bench/micro/` files were not run.**
   ARCHITECTURE quotes no figures from them, so nothing there could be stale in
   the way the rest was; they are simply unexercised by this pass.
