# DMEx

A decentralized, broker-less message queue over plain HTTP and Server-Sent
Events.

Every application that appends messages owns and persists its own append-only
message log — one ordinary SQL table — and streams it directly to consumers over
HTTP. There is no broker process, no cluster to operate, and no shared
infrastructure between applications.

- **Technologically simple** — a SQL table and an HTTP endpoint.
- **No single point of failure** — a stream's availability depends only on its
  own application and its own database.
- **Persistently replayable** — consumption removes nothing. Any consumer can
  start, or restart, from any point in history.

> **Status: pre-release, under active development.** Not yet published to Hex.
> [ARCHITECTURE.md](ARCHITECTURE.md) is the design — the wire contract, the
> invariants, and where each lives in the code. [ROADMAP.md](ROADMAP.md) is what
> is designed but not built.

## Appending

```elixir
# migration
def up, do: DMEx.Migration.up()

defmodule MyApp.Log do
  use DMEx.Log, repo: MyApp.Repo
end

defmodule MyApp.Messages.OrderPlaced do
  use DMEx.Message, [:order_id, :customer_id, :total]
end

# in a Phoenix router, a Plug.Router, or a bare Bandit/Cowboy pipeline
forward "/stream", to: DMEx.Log.Plug, init_opts: [log: MyApp.Log]

MyApp.Log.append(%MyApp.Messages.OrderPlaced{order_id: 7})
#=> {:ok, 1042}
```

## Reading

```elixir
defmodule MyApp.OrderProjection do
  use DMEx.Handler, logs: [:orders], id: :order_projection

  @impl true
  def handle_message(%MyApp.Messages.OrderPlaced{} = message, _metadata), do: :ok
end

# the whole supervision-tree declaration
{DMEx.Supervisor,
 otp_app: :my_app,
 repo: MyApp.Repo,
 logs: [
   catalogue: MyApp.Log,                        # owned: started and served here
   orders: "https://orders.internal/stream"     # someone else's: read
 ]}
```

Handler modules are never listed: every one compiled into an application that
depends on `:dmex` is discovered and started — one subscription per log in its
`logs:`, each with its own cursor — and one naming a log that `logs:` does not
bind stops the boot rather than silently never running.

The binding also chooses how the log is read, which is why nothing configures a
transport: a **module** is read in this VM — a query and a handler call, no
socket and no framing — and a **URL** is read over HTTP. A handler cannot tell
which it got.

Message types are read at compile time from the structs `handle_message/2`
matches on, and each is checked to be a `DMEx.Message` there. Delivery is
**selective server-side**: a subscription receives the types its clauses match
on and nothing else is read, framed or sent. Nothing is declared to make that
happen. See [guides/fingerprint-spec.md](guides/fingerprint-spec.md) for the
wire encoding, which a non-Elixir consumer can implement against.

A subscription's durable identity is the declared `"orders/order_projection"`
pair rather than the module name, so renaming the module is free.

Start with the `DMEx` moduledoc, then `DMEx.Config` for every default and why it
has the value it does.

## Failure handling

A handler that raises is retried in process a few times with jittered backoff.
Once those are exhausted, `on_failure` decides:

- `:dead_message` (default) — record it, in the same transaction as the advance,
  and carry on.
- `:crash` — the subscription crash-loops on that message instead, at
  reconnect-backoff pace, without disturbing its siblings.
- `:skip` — advance past it; the failure is logged and emitted as telemetry, and
  that is the only trace.

`dmex_dead_messages` is **structurally a message log**: the same `id`,
`message_type`, `data` and `metadata`, with the subscription id alongside. A
dead row *is* a log row, so replay is handing it back rather than rebuilding a
message from parts:

    DMEx.Handler.DeadMessages.list(MyApp.OrderProjection, repo: MyApp.Repo)
    DMEx.Handler.DeadMessages.replay(MyApp.OrderProjection, repo: MyApp.Repo)

Nothing about the failure is stored — no exception, no attempt count. A failure
is an event, not a property of the message, so it lives in your logs and in
`[:dmex, :handler, :failed]`. The one column differing from the log is
`inserted_at`: here it is when the row landed, which is the axis anything
pruning it has to work along.

## Backpressure

**A consumer has no buffer at all**, and that is the whole mechanism. Rows run
through the pipeline inline, on the socket path, as they are decoded — so while
a handler is running the socket is not re-armed, the TCP receive window closes,
and the producer's `chunk/2` blocks on it.

The producer's send loop is gated the same way: the next read cannot happen
until the previous write returns, so a stalled consumer stalls its own
connection and nothing else.

There is nothing to configure on the consumer side, because there is nothing to
size. How fast a stream is fed is `max_batch_frequency`, `max_batch_size` and
`max_batch_bytes` on the **producer** — the party whose database, memory and
request processes those protect. `max_batch_frequency` is a ceiling rather than
a reaction to load, so a producer can multiply it by its connection count and
know its own worst case in advance. The byte budget is the one worth setting
deliberately: a row count is not a memory bound when payloads vary, and it is
applied *in the read* rather than by trimming a batch already in memory.

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

The integration suite is separate because two things cannot be observed inside
an Ecto sandbox transaction: `LISTEN`/`NOTIFY` only fires at COMMIT, and the
append-ordering test needs genuinely concurrent transactions.

## Benchmarks

```sh
mix bench.micro   # per-row costs on the hot paths
mix bench.e2e     # throughput and latency against a real database and socket
```

Decision tools, not regression tests, so they do not run in CI. Each file states
the design claim or open question it exists to answer.
