# Deploying a DMEx producer

DMEx runs inside your application and uses your web server's listener and your
database. One thing it handles itself and you may want to size, one you can
tighten if you care to, and two it cannot do for you.

Nothing here applies to consumers: a consumer opens ordinary outbound HTTP
connections and manages its own timeouts.

## 1. Stuck connections are reclaimed for you

The send loop bounds memory by construction — it holds at most the rows it is
writing, and the next read cannot happen until the previous write returns. What
it cannot do is reclaim itself: **a process blocked in `chunk/2` cannot time
itself out, because it is not running**, and neither can one blocked in a read
against a database that stopped answering.

So every blocking step states a deadline before it enters, and
`DMEx.Log.Watcher` — one process per log — kills whatever is past its deadline:

```elixir
defmodule MyApp.Log do
  use DMEx.Log,
    repo: MyApp.Repo,
    stall_timeout: 30_000,       # budget for any one blocking step
    stall_check_interval: 1_000  # how often the watcher sweeps
end
```

A stall is reclaimed within `stall_timeout + stall_check_interval`, with no
transport configuration whatsoever. A sweep is one atomics read per open
connection and no query, so leaving the interval at a second costs nothing
measurable. `stall_timeout: :infinity` opts out and starts no watcher.

**Sizing `stall_timeout`.** The budget covers a single blocking step, and the
step that matters is a write of at most `max_batch_bytes` (1 MiB by default) — a
real ceiling, because that bound is applied in the read rather than by trimming
a batch already in memory. The default 30 000 ms tolerates a consumer draining a
full write at roughly 280 kbit/s. Tighten it if your consumers are on fast
links; loosen it, or lower `max_batch_bytes`, if they are not. Tightening too
far drops a healthy slow consumer, which costs it a reconnect and costs you a
fresh catch-up read — survivable, but at scale it is a reconnect storm with no
admission control to absorb it.

**Dropping a connection is not a delivery event.** Nothing is lost and nothing
is skipped: the producer holds no consumer state, the consumer's `last_seen` is
durable, and resumption is the same `id > last_seen` read a cold connect does.

**What to alert on.** Every reclaim emits `[:dmex, :stream, :reclaim]` with an
`overdue_by` measurement, and logs a warning. Occasional ones are a slow
consumer; a steady rate is either a consumer that cannot keep up or a
`stall_timeout` set tighter than your links deserve.

### Two bounds on a read

`max_batch_size` (500 rows) and `max_batch_bytes` (1 MiB of payload) bound the
same read from different directions, and whichever is reached first ends it.

The row count binds on ordinary payloads. **The byte budget is the memory
bound**, and it is the one that matters if your publishers emit large messages:
500 rows is half a megabyte at 1 KB each and half a gigabyte at 1 MB each, and
nothing about a row count notices. Both are enforced inside the query, so an
oversized batch is never materialised and then trimmed. One row larger than the
whole budget is still delivered, on its own — refusing it would wedge that
subscription permanently.

This is the per-open-stream memory ceiling, so multiply by the connections you
expect: 200 streams at the 1 MiB default is 200 MiB of worst-case transient read
memory.

### Sizing `max_batch_size` against `stall_timeout`

A consumer processes rows **inline, as they arrive**, so the producer's write
does not complete until the consumer has drained it through its handler. One
write's drain time is therefore a real term in the stall budget:

```
max_batch_size / (slowest consumer's rows per second)  <  stall_timeout
```

At the defaults that is `500 / 265 ≈ 1.9 s` against a 30 s budget — about
fifteen times the headroom, using 265 rows/s as a deliberately slow handler. It
breaks down below roughly **17 rows/s**, a handler spending 60 ms per message.

If your handlers are that slow, lower `max_batch_size` rather than raising
`stall_timeout`: a smaller batch also lowers the latency floor and the
per-connection memory, while a larger timeout only delays noticing a genuinely
wedged connection. This is the one place a producer's setting has to account for
consumers it does not control.

## 2. Socket timeouts — optional

Neither is required. Both make the reclaim tighter than one watcher tick, and
the second covers a case nothing inside the VM can see.

### `send_timeout`

```elixir
# Bandit
{Bandit,
 plug: MyApp.Router,
 thousand_island_options: [
   transport_options: [send_timeout: 5_000, send_timeout_close: true]
 ]}

# Cowboy, via Plug.Cowboy
{Plug.Cowboy,
 scheme: :http,
 plug: MyApp.Router,
 transport_options: [socket_opts: [send_timeout: 5_000, send_timeout_close: true]]}
```

`gen_tcp:send` returns `:ok` once data reaches the kernel buffer, so this
timeout only engages when the output queue is genuinely full — exactly the
stalled-consumer condition. It fires at the socket without waiting for a sweep,
so it is strictly faster than the watcher on the one failure it can see.

`send_timeout_close: true` is not optional: without it the timeout leaves the
socket in an undefined state. Size it against `max_batch_bytes` the same way as
`stall_timeout` — 5 000 ms tolerates a 1 MiB write draining at ~1.6 Mbit/s.

### TCP keepalive — for the case nothing in-process can reach

```elixir
transport_options: [
  socket_opts: [
    keepalive: true,
    raw: [
      # TCP_KEEPIDLE, TCP_KEEPINTVL, TCP_KEEPCNT — Linux values
      {6, 4, <<15::native-32>>},
      {6, 5, <<5::native-32>>},
      {6, 6, <<3::native-32>>}
    ]
  ]
]
```

A peer can vanish without sending FIN or RST — power loss, a NAT rebind, a
dropped flow. Writes into a socket whose peer is gone keep returning `:ok` until
the kernel gives up, so deadlines keep being met and the watcher sees a healthy
connection. **This is the one stall DMEx structurally cannot detect.**

TCP handles it eventually: heartbeats mean there is always unacked data in
flight, so retransmission timeouts error the socket after some minutes.
Keepalive tuned to the order of the heartbeat interval turns "some minutes" into
seconds. The OS default is useless here — Linux waits two hours before the first
probe.

## 3. Proxies must not buffer

DMEx sets `X-Accel-Buffering: no` on every stream response, which nginx honours.
It is a fallback, not a substitute for configuring the proxy:

```nginx
location /stream {
    proxy_pass http://app;
    proxy_buffering off;        # the important one
    proxy_read_timeout 300s;    # must exceed heartbeat_ms + one batch interval
    proxy_http_version 1.1;
}
```

nginx buffers responses by default, holding bytes until its buffer fills or the
response ends. On an SSE stream that converts `max_batch_frequency` pacing into
"whenever the buffer happens to fill" and swallows heartbeats entirely — so a
healthy stream looks dead to both the consumer's liveness check and the proxy's
own idle timeout.

`proxy_read_timeout` must comfortably exceed the producer's **effective**
heartbeat interval, `heartbeat_ms` plus one batch interval (15 200 ms with the
defaults), because the loop's throttle runs after the heartbeat branch. The
producer advertises that figure as SSE's `retry:`, so you can read it off the
wire rather than recomputing it.

Any other intermediary doing its own response buffering has to be turned off,
not tuned.

## 4. Database connections

Every open stream issues its own reads, and `max_batch_frequency` bounds it:

```
worst-case reads per second  =  open streams x max_batch_frequency
```

**That figure does not depend on your append rate.** A connection reads at most
`max_batch_frequency` times a second whether the log is idle or saturated, which
is what makes the ceiling knowable before you hit it. The binding constraint in
practice is **pool contention**, not raw query load.

- **Size the pool for streams as well as requests.** 200 streams at the default
  5 reads/s is about 1 000 reads/s, each a short primary-key range scan, on top
  of whatever else the application does. If that is more than your pool wants,
  lower `max_batch_frequency` — the cost is latency, which rises to about
  `1 / (2 x max_batch_frequency)` on average. Consumers do not ask for pacing and
  cannot override it.
- **The producer needs one extra connection beyond the pool.** `LISTEN` cannot
  share a pooled connection, since it must stay checked out for its whole life,
  so `DMEx.Storage.Postgres` opens a dedicated `Postgrex.Notifications`
  connection per producer. Budget for it in `max_connections`.

A reclaimed stream returns whatever it had checked out: the pool monitors its
client, so killing a request process wedged on a query or a checkout hands that
connection back rather than leaking it.

Measured on a developer machine, 200 concurrent streams showed no pool queueing;
`bench/e2e/fanout_reads.exs` finds where your own ceiling is.

## What DMEx deliberately does not do

There is no aggregate budget and no admission control: no cap on how many
streams one producer serves, no aggregate memory budget, and no
`503`/`Retry-After` path.

What is covered is per-connection, without asking anything of you: the send loop
makes unbounded memory growth unrepresentable, and the watcher reclaims
connections that stop making progress. What is *not* covered is aggregate
pressure from many individually-healthy connections, and a consumer that is
pathologically slow while still technically progressing — which no per-step
deadline can fire on.

The bet is that those only matter past a connection count where a producer has
bigger capacity questions anyway, and the machinery is cheap to add when they
do. See [Global Resource Limits](../ROADMAP.md#global-resource-limits).
