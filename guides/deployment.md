# Deploying a DMEx producer

DMEx runs inside your application and uses your web server's listener and your
database. Three things it therefore **cannot** configure for you, and the stall
story does not hold without them.

Nothing here applies to consumers. A consumer opens ordinary outbound HTTP
connections and manages its own timeouts.

## 1. Socket timeouts — reclaiming stalled connections

The producer's send loop bounds memory by construction: it holds at most the
frame it is currently writing, and the next read cannot happen until the
previous write returns. What it cannot do is reclaim a connection that has
stopped making progress — **a process blocked in `chunk/2` cannot time itself
out, because it is not running.**

That is left entirely to the socket layer, and DMEx runs no watcher process to
compensate.

### `send_timeout` — the primary mechanism

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
the stalled-consumer condition: the peer stops reading, its receive window
closes, the buffer fills, the write blocks, and after `N` ms it fails and closes
the socket.

No external observer and no process-state inference, and it fires wherever the
stall begins — **including in the middle of a data frame, which is both the most
likely place and the case a heartbeat-based check structurally cannot see.**

`send_timeout_close: true` is not optional. Without it the timeout leaves the
socket in an undefined state, and the whole mechanism depends on the request
process dying with its connection.

**Sizing.** DMEx caps a frame at `frame_max_bytes` (1 MiB by default), so
5 000 ms tolerates a consumer draining that frame at roughly 1.6 Mbit/s. Raise
the timeout, or lower the cap, if you serve consumers on genuinely slow links —
the two are traded against each other, and a `send_timeout` set too tight drops
healthy slow consumers, which is a reconnect storm with no admission control to
absorb it.

### TCP keepalive — for the case `send_timeout` cannot reach

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

On a low-traffic log a peer can vanish without sending FIN or RST — power loss,
a NAT rebind, a dropped flow — leaving an idle connection whose only traffic is
heartbeats. Those are a few bytes, the buffer never fills, every `send` returns
`:ok`, so `send_timeout` never engages and the producer holds a request process
for a peer that no longer exists.

**The OS default is useless here**: Linux waits two hours before the first
probe. Set it to something on the order of the heartbeat interval.

## 2. Proxies must not buffer

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

## 3. Database connections

Every open stream issues its own reads, so `C` connections at publish rate `P`
cost up to `C x P` reads per second — bounded per connection at one read per
`batch_window`, so really `C / batch_window`. The binding constraint in practice
is **pool contention**, not raw query load.

Two consequences:

- **Size the pool for streams as well as for requests.** A producer serving 200
  streams at the default 200 ms window issues about 1 000 reads/s, each a short
  primary-key range scan, on top of whatever else the application does.
- **The producer needs one extra connection beyond the pool.** `LISTEN` cannot
  share a pooled connection, since it must stay checked out for its whole life,
  so `DMEx.Storage.Postgres` opens a dedicated `Postgrex.Notifications`
  connection per producer. Budget for it in `max_connections`.

Measured on a developer machine, 200 concurrent streams showed no pool queueing;
`bench/e2e/fanout_reads.exs` is there to find where your own ceiling is.

## What DMEx deliberately does not do

There is no global shedding and no admission control: no sweeper tracking
per-connection progress, no aggregate memory or connection budget, and no
`503`/`Retry-After` path that turns consumers away under load.

The per-connection mechanisms above cover the failure that actually threatens
the host application — the send loop makes unbounded memory growth
unrepresentable, and `send_timeout` plus keepalive reclaim connections that stop
progressing. What is *not* covered is aggregate pressure from many
individually-healthy connections, and a loop wedged for a non-socket reason such
as a hung query or a pool checkout that never returns.

Those are real. The bet is that they only matter past a connection count where a
producer application has bigger capacity questions anyway, and that the
machinery is cheaper to add later than to carry unused. Revisit when connection
counts reach the point where an aggregate budget is a live constraint.
