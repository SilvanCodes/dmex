# SLOP

Defects surfaced by the documentation review that are **not** documentation
problems. Each was found by reading code against its prose and confirmed by
reading the code; none has been reproduced by running it, because the review
ran in an environment with no Elixir toolchain. Verify before fixing.

Ordered by consequence.

---

## 1. `mix bench.e2e` cannot produce three of the figures ARCHITECTURE quotes

ARCHITECTURE's *What has been measured* attributes numbers to benchmarks that
cannot currently compute them. The numbers may well have been real when
recorded; the benches have drifted since.

### 1a. `fanout_reads` counts an event nothing emits

`bench/e2e/fanout_reads.exs:38` attaches to `[:dmex, :stream, :frame]`. No such
event exists — the send loop emits `[:dmex, :stream, :write]`
(`lib/dmex/log/stream.ex`), carrying a `:rows` measurement. Counter slot 1
therefore stays at 0 and the frames/s output is always `0`.

ARCHITECTURE's *"5.2 writes/s against a 200 ms window regardless of append
rate"* cannot come from this file as written.

Fix: attach to `[:dmex, :stream, :write]`; count writes in slot 1 and sum
`measurements.rows` separately.

### 1b. `fanout_reads`'s pool-contention probe records nothing

`bench/e2e/fanout_reads.exs:49`:

```elixir
if Map.get(measurements, :queue_time, 0) > 5_000_000, do: :counters.add(counter, 2, 0)
```

`add(counter, 2, 0)` adds **zero**, and slot 2 is the query counter
(incremented at line 47) rather than a queueing slot. The counter is only
`:counters.new(2, ...)`, so there is no third slot to hold it. The file's own
header bullet — "where pool checkout queueing appears" — is unmeasured.

ARCHITECTURE's *"No pool queueing appeared at 200 concurrent streams"* rests on
this no-op.

Fix: widen the counter, give queueing its own slot, print it — or delete the
bullet and the ARCHITECTURE claim.

### 1c. `catchup_drain` stage (c) hangs

`bench/e2e/catchup_drain.exs:142-154` defines `Bench.UnusedMessage` and
`Bench.SkippingHandler` at **runtime**, and the handler takes the default
`selective: true`. So its fingerprint covers only `Bench.UnusedMessage`, which
the producer's expansion (`DMEx.Log.Messages`, scanning compiled application
modules) cannot contain — matched count 0, `404`, and the consumer retries
forever by design. `await_skips` at line ~193 never returns.

ARCHITECTURE's *"a fully-skipping consumer sustains ~2 515 rows/s"* and
*"20 000 skipped rows cost 7 cursor writes"* describe a run this file can no
longer perform.

Fix: `use DMEx.Handler, logs: [:bench], id: :skipping, selective: false` — which
is also what makes the stage's "every row skipped" framing true, since a
selective stream would not carry those rows at all.

### 1d. Also in `fanout_reads`

Line 108 labels the output *"max_batch_frequency caps this at 5.0/s"*, but the
capped quantity is reads/writes, not frames — one write carries up to
`max_batch_size` frames. The `5.0` is hardcoded rather than read from
`DMEx.Config`.

---

## 2. `DMEX_POSTGRES_URL` can silently point both suites at one database

`config/test.exs`:

```elixir
url -> String.replace_trailing(url, "/dmex_test", "/dmex_integration_test")
```

The derivation only fires for a URL ending in `/dmex_test`. Point
`DMEX_POSTGRES_URL` at any other database name and it is a no-op, so
`DMEx.Test.Repo` and `DMEx.Test.IntegrationRepo` resolve to the **same**
database — and the integration repo is deliberately un-sandboxed, so
`mix test.integration` truncates and writes against it directly.

The prose has been corrected to state the actual rule, but the failure mode is
in the code. Options: derive by replacing the whole database segment; or refuse
to start when the two URLs are equal and `DMEX_POSTGRES_INTEGRATION_URL` was not
set explicitly.

---

## 3. `DMEx.Log.Plug` arms the same deadline three times at connect

`lib/dmex/log/plug.ex`:

- line 160 — `Registry.register/3`, which calls `Status.new/1` and arms
- line 164 — `Status.arm(status, config.stall_timeout)`
- line 211 — `advertise_retry/3` arms again before the `retry:` chunk

The arm at 164 looks dead: nothing between it and the registration can block.
`DMEx.Log.Status`'s docs say a status is "armed rather than blank, so a
connection is watched from the instant it registers", which is 160's job.

Either drop 164, or keep it and say what it re-arms for.

---

## 4. `DMEx.Handler.Stream` tracks a timer reference it never reads

`lib/dmex/handler/stream.ex:107` initialises `checkpoint_timer: nil` and line
338 writes it in `schedule_checkpoint/1`. Nothing ever reads or cancels it —
unlike `:liveness_timer`, which is cancelled in `reconnect/2` and
`schedule_liveness/1`.

Harmless (the timer fires into `handle_info(:checkpoint, …)` regardless), but
either cancel it on `terminate/2` or drop the field.

---

## 5. `DMEx.Transport.SSE.Decoder` misses two legal SSE blank lines

`lib/dmex/transport/sse/decoder.ex`:

```elixir
@delimiters ["\r\n\r\n", "\n\n", "\r\r"]
```

Per the SSE grammar a line terminator is CR, LF **or** CRLF, so a blank line can
also be `LF`+`CRLF` (`"\n\r\n"`) or `CRLF`+`CR` (`"\r\n\r"`). Neither matches,
so a frame from a producer mixing terminators is never dispatched and the buffer
grows to `max_frame_bytes` before the connection is dropped.

Harmless against DMEx's own producer, which always emits `"\n\n"` — but the
moduledoc explicitly scopes the decoder to generic producers, and
`guides/fingerprint-spec.md` invites third-party implementations. The doc claim
has been softened; the gap is still there.

---

## 6. `DMEx.Test.Storage` (the stub) diverges from the behaviour it stands in for

`test/support/storage_stub.ex`:

- **`fail/3` and `hang/2` take a `fun` argument that is ignored.** Only
  `do_read/5` consults `:fail`/`:hang`, so `fail(opts, :append)` silently does
  nothing. Either honour the argument in every callback or drop it.
- **`dead_messages/2` does not return `t:row/0`.** It `Map.take`s
  `[:id, :message_type, :data, :metadata, :inserted_at]` from attrs that
  `dead_message/2` never stamps `:inserted_at` onto (the real adapter stamps it),
  and never adds `:previous_id`. `c:DMEx.Storage.dead_messages/2` requires
  `t:row/0`.
- **`dead_messages/1`, `appends/1` and `add_rows/2` have no callers** in `test/`
  or `bench/`.

---

## 7. `Bench.Fanned`-style multi-log fixtures only work because discovery is off

`test/support/handlers.ex` — `Fanned` declares `logs: [:test, :other]`, but
`DMEx.Test.Server.logs/1` binds only `:test`. Per `DMEx.Supervisor`, a
discovered handler naming an unbound log **stops the boot**. It does not fire
today only because every integration supervisor passes `otp_app: nil`, disabling
discovery entirely; `:other` is bound in `test/dmex/supervisor_test.exs` alone.

That means the integration suite is not exercising handler discovery at all —
the mechanism README describes as the whole of adding a subscription. Worth
either binding `:other` in `Server.logs/1` and turning discovery on for one
integration test, or noting deliberately that discovery is unit-tested only.

---

## 8. Smaller items

- **`Decoder.new/1`'s default argument is a third copy of `max_frame_bytes`.**
  `decoder.ex` hardcodes `4_194_304` in both the struct default and the function
  head; the only caller always passes `config.max_frame_bytes`. Drop the default
  so `DMEx.Config` is the single home for the number.
- **`DMEx.Migration.down/1` never drops a `:prefix` schema** that `up/1` created.
  Documented as intentional for now; confirm that is the intent.
- **`bench/support/bench_helper.exs`'s `reset_log!/0` does not truncate
  `dmex_dead_messages`.** A benchmark run that produces dead messages leaves them
  for the next run. The doc now names the two tables it does truncate.
