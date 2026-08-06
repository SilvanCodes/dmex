# Deploying a DMEx producer

DMEx runs inside your application and uses your web server's listener and your
database. §1 is the only thing you have to size; §2 is optional tightening; §3
and §4 are outside DMEx's reach.

None of it applies to consumers: a consumer opens ordinary outbound HTTP
connections and manages its own timeouts.

## 1. Stuck connections are reclaimed for you

The send loop cannot reclaim itself — **a process blocked in `chunk/2` is not
running**, and neither is one blocked in a read against a database that stopped
answering. So every blocking step states a deadline before it enters, and
`DMEx.Log.Watcher` — one process per log — kills whatever is past one:

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

**Sizing `stall_timeout`.** The budget covers one blocking step, and the step
that matters is a write of at most `max_batch_bytes`. The 30 s default tolerates
a 1 MiB write draining at ~280 kbit/s. Tighten it for fast links; loosen it, or
lower `max_batch_bytes`, for slow ones — tightening too far drops healthy slow
consumers, which at scale is a reconnect storm with no admission control to
absorb it.

**Dropping a connection is not a delivery event** (ARCHITECTURE invariant 7):
nothing is lost and nothing is skipped.

**What to alert on.** Every reclaim emits `[:dmex, :stream, :reclaim]` with an
`overdue_by` measurement, and logs a warning. Occasional ones are a slow
consumer; a steady rate is either a consumer that cannot keep up or a
`stall_timeout` set tighter than your links deserve.

### Two bounds on a read

`max_batch_size` (500 rows) and `max_batch_bytes` (1 MiB of `data` + `metadata`
+ `type` octets) bound the same read, and whichever binds first ends it.

Both are memory bounds, over different costs (ARCHITECTURE invariant 2): the
byte cap counts what the database can see, and the row cap bounds the fixed
~1 KB per row that turning it into terms costs, which no budget in database
octets can.

To size it, add both: 500 rows at 1 MiB of payload is ~1.5 MiB per open stream,
so 200 streams is ~300 MiB of worst-case transient read memory.

### Sizing `max_batch_size` against `stall_timeout`

A consumer processes rows **inline, as they arrive**, so the producer's write
does not complete until the consumer has drained it through its handler. One
write's drain time is therefore a real term in the stall budget:

```
max_batch_size / (slowest consumer's rows per second)  <  stall_timeout
```

265 rows/s is the measured ceiling for a handler that commits at all (see
ARCHITECTURE's *What has been measured*), so at the defaults that is
`500 / 265 ≈ 1.9 s` against a 30 s budget. It breaks down below roughly
**17 rows/s** — a handler spending 60 ms per message.

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
heartbeat interval, `heartbeat_interval` plus one batch interval (15 010 ms with the
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

**That ceiling does not depend on your append rate**, which is what makes it
knowable before you hit it. An idle stream reads far less — it parks until an
append or the heartbeat interval, and a wholly idle log costs one query per poll
interval per *app*. The binding constraint in practice is **pool contention**,
not raw query load.

- **Size the pool for streams as well as requests.** 200 streams at the default
  100 reads/s is a worst case of 20 000 reads/s, each a short primary-key range
  scan, on top of whatever else the application does. **This is the setting most
  likely to need lowering**: measured on a 10-connection pool, checkout queueing
  appears somewhere between 50 and 200 busy streams at the default, and at 200
  it dominates — see ARCHITECTURE's *What has been measured*. The cost of
  lowering it is latency, which rises to about `1 / (2 x max_batch_frequency)`
  on average — 5 ms at the default, 100 ms at 5. Consumers do not ask for pacing
  and cannot override it.
- **The producer needs one extra connection beyond the pool.** `LISTEN` cannot
  share a pooled connection, since it must stay checked out for its whole life,
  so `DMEx.Notifier.Postgres` — the default notifier for Postgres storage —
  opens a dedicated `Postgrex.Notifications` connection per log. Budget for it in
  `max_connections`; `DMEx.Notifier.Beam` needs none.

A reclaimed stream returns whatever it had checked out: the pool monitors its
client, so killing a request process wedged on a query or a checkout hands that
connection back rather than leaking it.

Pool checkout queueing appears somewhere between 50 and 200 concurrent streams
on a 10-connection pool; `bench/e2e/fanout_reads.exs` finds where your own
ceiling is.

## What DMEx deliberately does not do

Protection is per-connection; there is no aggregate budget and no admission
control. See
[ARCHITECTURE.md](../ARCHITECTURE.md#what-dmex-deliberately-does-not-do) for
what that leaves uncovered and
[Global Resource Limits](../ROADMAP.md#global-resource-limits) for the design.
