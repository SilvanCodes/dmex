# DMEx

A decentralized, broker-less message queue over plain HTTP and Server-Sent Events.

Every application that appends messages owns and persists its own append-only
message log — one ordinary SQL table — and streams it directly to consumers over
HTTP. There is no broker process, no cluster to operate, and no shared
infrastructure between applications.

- **Technologically simple** — a SQL table and an HTTP endpoint.
- **No single point of failure** — no shared bus whose outage takes down every
  producer/consumer pair. A stream's availability depends only on its own
  application and its own database.
- **Persistently replayable** — consumption never removes anything. Any consumer
  can start, or restart, from any point in history.

> **Status: pre-release, under active development.** Not yet appended to Hex.
> See [ARCHITECTURE.md](ARCHITECTURE.md) for the design — the wire contract, the
> invariants everything rests on, and where each one lives in the code — and
> [ROADMAP.md](ROADMAP.md) for what is designed but deliberately not built yet:
> a `GET /stream/stats` endpoint, aggregate resource limits, and compression.

## What works today

Append → `LISTEN`/`NOTIFY` → SSE frame → decode → handler → durable cursor,
with resumption, duplicate suppression, catch-up, heartbeats, bounded retries
and all three failure modes — covered end to end over a real socket.

Delivery is **selective server-side**: a subscription receives the message types
its handler's `handle_message/2` clauses match on, and nothing else is read,
framed or sent. Nothing is declared to make that happen — the clauses are the
selection, encoded into the request path at compile time. See
[guides/fingerprint-spec.md](guides/fingerprint-spec.md) for the wire encoding,
which a non-Elixir consumer can implement against.

```elixir
# migration
def up, do: DMEx.Migration.up()

# appending
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

```elixir
# reading
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
   catalogue: MyApp.Log,                              # owned: started and served here
   orders: "https://orders.internal/stream"           # someone else's: read
 ]}
```

The handler module is the whole declaration, and it is never listed anywhere:
every handler compiled into an application that depends on `:dmex` is discovered
and started — one subscription per log in its `logs:`, each with its own cursor —
and one naming a log that `logs:` does not bind stops the boot rather than
silently never running.

The binding also chooses how the log is read, which is why nothing configures a
transport: a **module** is read in this VM — a query and a handler call, no
socket and no framing — and a **URL** is read over HTTP. A handler cannot tell
which it got.

The message types are not written down twice either — they are read at compile
time from the structs `handle_message/2` matches on, and each is checked to be a
`DMEx.Message` there, so a handler subscribed to a type it has no clause for is
unrepresentable rather than a runtime `FunctionClauseError`. `logs:` binds names
to modules and URLs, so a URL appears once rather than at every subscription
site.

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

    DMEx.Handler.DeadMessages.list(MyApp.OrderProjection, repo: MyApp.Repo)
    DMEx.Handler.DeadMessages.replay(MyApp.OrderProjection, repo: MyApp.Repo)

Nothing about the failure is stored — no exception, no attempt count. A failure
is an event, not a property of the message, so it lives in your logs and in
`[:dmex, :handler, :failed]`. The one column that differs from the log is
`inserted_at`: on this table it is when the row landed here, which is the axis
anything pruning it has to work along.

### Backpressure

**A consumer has no buffer at all**, and that is the whole mechanism. Rows run
through the pipeline inline, on the socket path, as they are decoded — so while
a handler is running the socket is not being re-armed, the TCP receive window
closes, and the producer's `chunk/2` blocks on it. Backpressure is what happens
by default rather than something detected and handled.

The producer's send loop is already gated the same way: the next read cannot
happen until the previous write returns, so a stalled consumer stalls its own
connection and nothing else. It reads no more rows, allocates no more memory,
and issues no more queries.

There is nothing to configure on the consumer side, because there is nothing to
size. How fast a stream is fed is `max_batch_frequency`, `max_batch_size` and
`max_batch_bytes` on the **producer** — the party whose database, memory and
request processes those protect — and consumers neither ask for pacing nor are
told what any of them is.

`max_batch_frequency` is how hard one connection may hammer the database, in
reads per second. It is a ceiling rather than a reaction to load, so a producer
can multiply it by its connection count and know its own worst case in advance.

The byte budget is the one worth setting deliberately. A row count is not a
memory bound when payloads vary, and it is applied *in the read* rather than by
trimming a batch already in memory, so `open streams × max_batch_bytes` is a
real ceiling rather than an aspiration.

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
mix bench.micro   # per-row costs on the hot paths
mix bench.e2e     # throughput and latency against a real database and socket
```

These are decision tools, not regression tests, so they do not run in CI. Each
file states the design claim or open question it exists to answer — what the
append lock actually costs, where a consumer's per-row commit starts to
dominate, whether splicing stored JSON really beats re-encoding it.
