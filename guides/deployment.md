# Deploying a DMEx producer

DMEx runs inside your application and uses your web server's listener and your
database. This is what that means for you as an operator: one thing DMEx now
handles itself and you may want to size, one thing you can tighten if you care
to, and two it genuinely cannot do for you.

Nothing here applies to consumers. A consumer opens ordinary outbound HTTP
connections and manages its own timeouts.

## 1. Stuck connections are reclaimed for you

The producer's send loop bounds memory by construction: it holds at most the
rows it is currently writing, and the next read cannot happen until the
previous write returns. What it cannot do is reclaim itself — **a process
blocked in `chunk/2` cannot time itself out, because it is not running**, and
neither can one blocked in a read against a database that has stopped
answering.

So every blocking step of the loop states a deadline before it enters, and
`DMEx.Producer.Watcher` — one process per producer, started with it — kills
whatever is past its deadline. Two settings, on your producer module:

```elixir
defmodule MyApp.Producer do
  use DMEx.Producer,
    repo: MyApp.Repo,
    stall_timeout: 30_000,       # budget for any one blocking step
    stall_check_interval: 1_000  # how often the watcher sweeps
end
```

A stall is therefore reclaimed within `stall_timeout + stall_check_interval`,
with no transport configuration whatsoever. A sweep is one atomics read per
open connection and no query, so leaving the interval at a second costs
nothing measurable.

**Sizing `stall_timeout`.** The budget covers a single blocking step, and the
step that matters is a write of at most `write_max_bytes` (1 MiB by default).
The default 30 000 ms therefore tolerates a consumer draining a full write at
roughly 280 kbit/s. Tighten it if you know your consumers are on fast links and
want stalls cleared sooner; loosen it, or lower `write_max_bytes`, if you serve
consumers on genuinely slow ones. The failure mode of tightening too far is a
healthy slow consumer being dropped, which costs it a reconnect and costs you a
fresh catch-up read — survivable, but at scale it is a reconnect storm with no
admission control to absorb it.

`stall_timeout: :infinity` opts out and starts no watcher at all.

**Dropping a connection is not a delivery event.** Nothing is lost and nothing
is skipped: the producer holds no consumer state, the consumer's `last_seen` is
durable, and resumption is the same `id > last_seen` read a cold connect does.

**What to alert on.** Every reclaim emits `[:dmex, :stream, :reclaim]` with an
`overdue_by` measurement, and logs a warning. Occasional ones are a slow
consumer; a steady rate is either a consumer that cannot keep up or a
`stall_timeout` set tighter than your links deserve.

## 1a. Sizing `max_batch_size` against `stall_timeout`

A consumer processes rows **inline, as they arrive**, so the producer's write
does not complete until the consumer has drained it through its handler. That
makes one write's drain time a real term in the producer's stall budget:

```
max_batch_size / (slowest consumer's rows per second)  <  stall_timeout
```

At the defaults that is `500 / 265 ≈ 1.9 s` against a 30 s budget — about
fifteen times the headroom, using 265 rows/s as a deliberately slow handler.
It breaks down below roughly **17 rows/s**, which is a handler spending 60 ms
per message.

If your handlers are that slow, lower `max_batch_size` rather than raising
`stall_timeout`: a smaller batch also lowers the latency floor and the
producer's per-connection memory, while a larger timeout only delays noticing a
genuinely wedged connection. This is the one place where a producer's setting
has to account for consumers it does not control, which is why it is written
down here rather than left implicit.

## 2. Socket timeouts — optional, and what they add

Neither of these is required. Both make the reclaim tighter than one watcher
tick, and the second covers a case nothing running inside the VM can see.

### `send_timeout`

```elixir
# Bandit
{Bandit,
 plug: MyApp.Router,
 thousand_island_options: [
   transport_options: [send_timeout: 5_000, send_timeout_close: true]
 ]}
```

```elixir
# Cowboy, via Plug.Cowboy
{Plug.Cowboy,
 scheme: :http,
 plug: MyApp.Router,
 transport_options: [socket_opts: [send_timeout: 5_000, send_timeout_close: true]]}
```

`gen_tcp:send` returns `:ok` once data reaches the kernel buffer, so this
timeout only engages when the output queue is genuinely full — which is exactly
the stalled-consumer condition. It fires at the socket, without waiting for a
sweep and without anything needing to be scheduled, so it is strictly faster
than the watcher on the one failure it can see.

If you set it, `send_timeout_close: true` is not optional: without it the
timeout leaves the socket in an undefined state. And size it against
`write_max_bytes` the same way as `stall_timeout` above — 5 000 ms tolerates a
consumer draining a 1 MiB write at roughly 1.6 Mbit/s.

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
dropped flow. Writes into a socket whose peer is gone keep returning `:ok`
until the kernel gives up, so deadlines keep being met and the watcher sees a
perfectly healthy connection. **This is the one stall DMEx structurally cannot
detect**, and no in-process mechanism could.

TCP handles it eventually on its own: heartbeats mean there is always unacked
data in flight, so retransmission timeouts error the socket after some minutes.
Keepalive tuned to the order of the heartbeat interval turns "some minutes"
into seconds. The OS default is useless here — Linux waits two hours before the
first probe.

## 3. Proxies must not buffer

DMEx sets `X-Accel-Buffering: no` on every stream response, which nginx honours.
It is a fallback, not a substitute for configuring the proxy properly:

```nginx
location /stream {
    proxy_pass http://app;
    proxy_buffering off;        # the important one
    proxy_read_timeout 300s;    # must exceed heartbeat_ms + batch_window
    proxy_http_version 1.1;
}
```

nginx buffers responses by default, holding bytes until its buffer fills or the
response ends. On an SSE stream that converts `batch_window` pacing into
"whenever the buffer happens to fill" and swallows heartbeats entirely — so a
perfectly healthy stream looks dead to both the consumer's liveness check and
the proxy's own idle timeout.

`proxy_read_timeout` must comfortably exceed the producer's **effective**
heartbeat interval, which is `heartbeat_ms + batch_window` — 15 200 ms with the
defaults — because the loop's throttle runs after the heartbeat branch. The
producer advertises that same figure as SSE's `retry:`, so you can read it off
the wire rather than recomputing it.

Any other intermediary doing its own response buffering has to be turned off,
not tuned.

## 4. Database connections

Every open stream issues its own reads, so `C` connections at publish rate `P`
cost up to `C x P` reads per second — bounded per connection at one read per
`batch_window`, so really `C / batch_window`. The binding constraint in practice
is **pool contention**, not raw query load.

Two consequences:

- **Size the pool for streams as well as for requests.** A producer serving 200
  streams at the default 200 ms window issues about 1 000 reads/s, each a short
  primary-key range scan, on top of whatever else the application does. Raising
  `batch_window` is the lever, and it is entirely yours: consumers do not ask
  for pacing and cannot override it.
- **The producer needs one extra connection beyond the pool.** `LISTEN` cannot
  share a pooled connection, since it must stay checked out for its whole life,
  so `DMEx.Storage.Postgres` opens a dedicated `Postgrex.Notifications`
  connection per producer. Budget for it in `max_connections`.

A reclaimed stream returns whatever it had checked out: the pool monitors its
client, so killing a request process wedged on a query or a checkout hands that
connection back rather than leaking it.

Measured on a developer machine, 200 concurrent streams showed no pool queueing;
`bench/e2e/fanout_reads.exs` is there to find where your own ceiling is.

## What DMEx deliberately does not do

There is no aggregate budget and no admission control: no cap on how many
streams one producer will serve, no aggregate memory budget, and no
`503`/`Retry-After` path that turns consumers away under load.

What is covered is per-connection, and covered without asking anything of you:
the send loop makes unbounded memory growth unrepresentable, and the watcher
reclaims connections that stop making progress, whatever stopped them. What is
*not* covered is aggregate pressure from many individually-healthy connections,
and a consumer that is pathologically slow while still technically progressing —
which no per-step deadline can ever fire on.

Those are real. The bet is that they only matter past a connection count where a
producer application has bigger capacity questions anyway, and the machinery is
now cheap to add when they do: the watcher already runs per producer and already
holds the live set, so a budget is a counter and a policy on a process that
exists. See [Global Resource Limits](../ROADMAP.md#global-resource-limits).
