# Deploying a DMEx producer

DMEx runs inside your application and uses your web server's listener and your
database. §1 is the only thing you have to size; §2 is optional tightening; §3
and §4 are outside DMEx's reach.

None of it applies to consumers: a consumer opens ordinary outbound HTTP
connections and manages its own timeouts.

## 1. Stuck connections are reclaimed for you

Every blocking step in the send loop states a deadline before it enters
([every blocking step has a deadline](../ARCHITECTURE.md#every-blocking-step-has-a-deadline)),
and `DMEx.Log.Watcher` — one process per log — kills whatever is past one:

```elixir
defmodule MyApp.Log do
  use DMEx.Log,
    repo: MyApp.Repo,
    # Both optional. `DMEx.Config` is where the defaults live and why they are
    # what they are; these are the two knobs, not two values to copy.
    stall_timeout: 30_000,
    stall_check_interval: 1_000
end
```

A stall is reclaimed within `stall_timeout + stall_check_interval`, with no
transport configuration whatsoever. A sweep is one atomics read per open
connection and no query, so leaving the interval at a second costs nothing
measurable. `stall_timeout: :infinity` opts out and starts no watcher.

**Sizing `stall_timeout`.** The budget covers one blocking step, and the step
that matters is a write of at most `max_batch_bytes`. So the question it answers
is how slowly the slowest link you serve may drain one write. Tighten it for fast
links; loosen it, or lower `max_batch_bytes`, for slow ones — tightening too far
drops healthy slow consumers, which at scale is a reconnect storm with no
admission control to absorb it.

**Dropping a connection is not a delivery event**
([ARCHITECTURE](../ARCHITECTURE.md#dropping-a-connection-is-not-a-delivery-event)):
nothing is lost and nothing is skipped.

**What to alert on.** Every reclaim emits `[:dmex, :stream, :reclaim]` with an
`overdue_by` measurement, and logs a warning. Occasional ones are a slow
consumer; a steady rate is either a consumer that cannot keep up or a
`stall_timeout` set tighter than your links deserve.

### Two bounds on a read

`max_batch_rows` and `max_batch_bytes` — rows, and `data` + `metadata` + `type`
octets — bound the same read, and whichever binds first ends it.

Both are memory bounds over different heap costs — the byte cap counts database
octets, the row cap the fixed ~1 KB per row of terms — see
[producer memory is one batch](../ARCHITECTURE.md#producer-memory-is-one-batch-bounded-in-rows-and-bytes)
for why there are two, and `DMEx.Config` for the defaults.

To size it, add both: `max_batch_bytes` plus `max_batch_rows` kilobytes is the
worst-case transient read memory of one open stream, and open streams multiply it.

### Sizing `max_batch_rows` against `stall_timeout`

A consumer processes rows **inline, as they arrive**, so the producer's write
does not complete until the consumer has drained it through its handler. One
write's drain time is therefore a real term in the stall budget:

```
max_batch_rows / (slowest consumer's rows per second)  <  stall_timeout
```

`bench/e2e/catchup_drain.exs` measures the ceiling for a handler that commits at
all — a few hundred rows per second, so at the defaults one write's drain is a
couple of seconds against a 30 s budget. It breaks down when a handler spends
tens of milliseconds per message; run that benchmark against your own handler if
you are near it.

If your handlers are that slow, lower `max_batch_rows` rather than raising
`stall_timeout`: a smaller batch also lowers the latency floor and the
per-connection memory, while a larger timeout only delays noticing a genuinely
wedged connection. This is the one place a producer's setting has to account for
consumers it does not control.

## 2. Socket timeouts — optional

Neither is required. Both make the reclaim tighter than one watcher tick, and the
second covers a case nothing inside the VM can see. Both are web-server and
kernel settings rather than DMEx ones, so the names are here and the spelling is
your server's documentation.

**`send_timeout` (with `send_timeout_close`), on the listener's transport
options.** `gen_tcp:send` returns `:ok` once data reaches the kernel buffer, so
this timeout only engages when the output queue is genuinely full — exactly the
stalled-consumer condition. It fires at the socket without waiting for a sweep,
so it is strictly faster than the watcher on the one failure it can see. Size it
against `max_batch_bytes` the way `stall_timeout` is sized: the write has to
drain over the slowest link you serve. `send_timeout_close` is not optional —
without it the timeout leaves the socket in an undefined state.

**TCP keepalive, tuned to the order of `heartbeat_interval`.** A peer can vanish
without sending FIN or RST — power loss, a NAT rebind, a dropped flow. Writes
into a socket whose peer is gone keep returning `:ok` until the kernel gives up,
so deadlines keep being met and the watcher sees a healthy connection. **This is
the one stall DMEx structurally cannot detect.** TCP handles it eventually:
heartbeats mean there is always unacked data in flight, so retransmission
timeouts error the socket after some minutes. Keepalive turns "some minutes" into
seconds. The OS default is useless here — Linux waits two hours before the first
probe — so the interval and probe count have to be set, not merely enabled.

## 3. Proxies must not buffer

DMEx sets `X-Accel-Buffering: no` on every stream response, which nginx honours.
It is a fallback, not a substitute for configuring the proxy:

```nginx
location /stream {
    proxy_pass http://app;
    proxy_buffering off;        # the important one
    proxy_read_timeout 300s;    # must exceed heartbeat_interval + one batch interval
    proxy_http_version 1.1;
}
```

nginx buffers responses by default, holding bytes until its buffer fills or the
response ends. On an SSE stream that converts `max_batch_frequency` pacing into
"whenever the buffer happens to fill" and swallows heartbeats entirely — so a
healthy stream looks dead to both the consumer's liveness check and the proxy's
own idle timeout.

`proxy_read_timeout` must comfortably exceed the producer's **effective**
heartbeat interval — `heartbeat_interval` plus one batch interval, because the
loop's throttle runs after the heartbeat branch. Do not recompute it: the
producer advertises exactly that figure as SSE's `retry:`, so `curl -N` against
your own mount reads it off the wire.

Any other intermediary doing its own response buffering has to be turned off,
not tuned.

## 4. Database connections

Every open stream issues its own reads, and `max_batch_frequency` bounds it:

```
worst-case reads per second  =  open streams x max_batch_frequency
```

**That ceiling does not depend on your append rate**, which is what makes it
knowable before you hit it. An idle stream reads far less — it parks until an
append or the heartbeat interval, and a wholly idle log costs one query per poll
interval per *app*. The binding constraint in practice is **pool contention**,
not raw query load.

- **Size the pool for streams as well as requests.** Two hundred streams at the
  default frequency is tens of thousands of reads a second, each a short
  primary-key range scan, on top of whatever else the application does. **This is
  the setting most likely to need lowering**: measured on a 10-connection pool,
  checkout queueing appears somewhere between 50 and 200 busy streams at the
  default, and at 200 it dominates — `bench/e2e/fanout_reads.exs` has the numbers.
  The cost of lowering it is latency, which rises to about
  `1 / (2 x max_batch_frequency)` on average. Consumers do not ask for pacing and
  cannot override it.
- **The producer needs one extra connection beyond the pool.** `LISTEN` cannot
  share a pooled connection, since it must stay checked out for its whole life,
  so `DMEx.Notifier.Postgres` — the default notifier for Postgres storage —
  opens a dedicated `Postgrex.Notifications` connection per log. Budget for it in
  `max_connections`; `DMEx.Notifier.Beam` needs none.

A reclaimed stream returns whatever it had checked out: the pool monitors its
client, so killing a request process wedged on a query or a checkout hands that
connection back rather than leaking it.

## What DMEx deliberately does not do

Protection is per-connection; there is no aggregate budget and no admission
control. See
[ARCHITECTURE.md](../ARCHITECTURE.md#what-dmex-deliberately-does-not-do) for
what that leaves uncovered and
[Global Resource Limits](../ROADMAP.md#global-resource-limits) for the design.
