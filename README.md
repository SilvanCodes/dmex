# DMEx

A decentralized, broker-less message queue over plain HTTP and Server-Sent Events.

Every application that publishes messages owns and persists its own append-only
message log — one ordinary SQL table — and streams it directly to consumers over
HTTP. There is no broker process, no cluster to operate, and no shared
infrastructure between applications.

- **Technologically simple** — a SQL table and an HTTP endpoint.
- **No single point of failure** — no shared bus whose outage takes down every
  producer/consumer pair. A stream's availability depends only on its own
  application and its own database.
- **Persistently replayable** — consumption never removes anything. Any consumer
  can start, or restart, from any point in history.

> **Status: pre-release, under active development.** Not yet published to Hex.
> See [ARCHITECTURE.md](ARCHITECTURE.md) for the design — the wire contract, the
> invariants everything rests on, and where each one lives in the code — and
> [ROADMAP.md](ROADMAP.md) for what is designed but deliberately not in v1:
> server-side message-type selection (`GET /stream/<filter blob>`), a
> `GET /stream/stats` endpoint, aggregate resource limits, and compression.

## What works today

Publish → `LISTEN`/`NOTIFY` → SSE frame → decode → handler → durable cursor,
with resumption, duplicate suppression, catch-up, heartbeats, bounded retries
and all three failure modes — covered end to end over a real socket.

```elixir
# migration
def up, do: DMEx.Migration.up()

# producer
defmodule MyApp.Producer do
  use DMEx.Producer, repo: MyApp.Repo
end

defmodule MyApp.Messages.OrderPlaced do
  use DMEx.Message

  @derive JSON.Encoder
  defstruct [:order_id, :customer_id, :total]
end

# in a Phoenix router, a Plug.Router, or a bare Bandit/Cowboy pipeline
forward "/stream", to: DMEx.Producer.Plug, init_opts: [producer: MyApp.Producer]

MyApp.Producer.publish(%MyApp.Messages.OrderPlaced{order_id: 7})
#=> {:ok, 1042}
```

```elixir
# consumer
defmodule MyApp.OrderProjection do
  use DMEx.Consumer.Handler, producer: :orders, id: :order_projection

  @impl true
  def handle_message(%MyApp.Messages.OrderPlaced{} = message, _metadata), do: :ok
end

{DMEx.Consumer,
 repo: MyApp.Repo,
 producer: %{orders: "https://orders.internal/stream"},
 handlers: [MyApp.OrderProjection]}
```

The handler module is the whole declaration. The message types are not written
down twice: they are read at compile time from the structs `handle_message/2`
matches on, and each is checked to be a `DMEx.Message` there — so a handler
subscribed to a type it has no clause for is unrepresentable rather than a
runtime `FunctionClauseError`. The consumer binds names to URLs, so a URL
appears once rather than at every subscription site.

A subscription's durable identity is the declared `"orders/order_projection"`
pair rather than the module name — so renaming the module is free, and only
changing the declaration itself costs a cursor migration.

Start with the `DMEx` moduledoc, then `DMEx.Config` for every default and why it
has the value it does.

### Failure handling

A handler that raises is retried in process a few times with jittered backoff —
the message is still in hand, so a retry is just calling it again. Once those
are exhausted, `on_failure` decides:

- `:dead_message` (default) — record it, in the same transaction as the advance,
  and carry on. One message a handler cannot process is not a reason for every
  later message to stop arriving, nor a reason to lose the failing one.
- `:crash` — the subscription crash-loops on that message instead, at
  reconnect-backoff pace, without disturbing its siblings. For when a stuck
  subscription is the better failure.
- `:skip` — advance past it; the failure is logged and emitted as telemetry, and
  that is the only trace.

`dmex_dead_messages` is **structurally a message log**: the same `id`,
`message_type`, `data` and `metadata`, with the subscription id alongside. A
dead row *is* a log row, so replay is handing it back rather than rebuilding a
message from parts:

    DMEx.Consumer.DeadMessages.list(MyApp.OrderProjection, repo: MyApp.Repo)
    DMEx.Consumer.DeadMessages.replay(MyApp.OrderProjection, repo: MyApp.Repo)

Nothing about the failure is stored — no exception, no attempt count. A failure
is an event, not a property of the message, so it lives in your logs and in
`[:dmex, :consumer, :failed]`. The one column that differs from the log is
`inserted_at`: on this table it is when the row landed here, which is the axis
anything pruning it has to work along.

### Backpressure

A consumer that falls behind holds the socket off rather than buffering without
limit: rows land in a bounded queue, and the TCP receive window closes when it
fills because the consumer stops re-arming. That protects the consumer's memory
but on its own leaves it in a reconnect loop, since the producer will keep
handing over frames at a rate it cannot drain.

So the consumer also measures itself and asks for less. It times each message's
pipeline pass — **service time, not throughput**, because throughput on an
unsaturated consumer is just the arrival rate — and on sustained pressure
requests no more per window than it was measured to empty per window. Recovery
is additive and unwinds in the reverse order, reclaiming `batch_window` before
`max_batch_size`, because a widened window costs latency on every message while
an oversized batch costs nothing on a quiet stream.

Set `adaptive: false` on a subscription that would rather have predictable
framing than an automatically fitted one.

### Not yet built

A SQLite adapter and compression. The `DMEx.Storage` behaviour is defined in
full and everything routes through it — including how an adapter delivers
wakeups — so SQLite is a new adapter rather than a new seam.

## Operating it

A producer reclaims connections that stop making progress itself — see
[the deployment guide](guides/deployment.md) for how to size that, the socket
options that tighten it, and the proxy settings SSE depends on. `DMEx.Telemetry`
lists every event DMEx emits.

## Development

The dev container ships a Postgres service. After
`Dev Containers: Rebuild Container`:

```sh
mix deps.get
mix test              # unit suite (sandboxed; skips :database tests if none is reachable)
mix test.integration  # integration suite (needs a real, non-sandboxed database)
mix test.all          # everything
```

Point `DMEX_POSTGRES_URL` elsewhere to use your own instance; the integration
database is derived from it unless `DMEX_POSTGRES_INTEGRATION_URL` is set too.

The integration suite is separated because two things in the design cannot be
observed inside an Ecto sandbox transaction: `LISTEN`/`NOTIFY` only fires at
COMMIT, and the append-ordering test needs genuinely concurrent transactions.

## Benchmarks

```sh
mix bench.micro   # per-row and per-frame costs on the hot paths
mix bench.e2e     # throughput and latency against a real database and socket
```

These are decision tools, not regression tests, so they do not run in CI. Each
file states the design claim or open question it exists to answer — what the
append lock actually costs, where a consumer's per-row commit starts to
dominate, whether splicing stored JSON really beats re-encoding it.
