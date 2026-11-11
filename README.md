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
def up, do: DMEx.Storage.Migration.up()

defmodule MyApp.Log do
  use DMEx.Log, repo: MyApp.Repo
end

defmodule MyApp.Messages.OrderPlaced do
  use DMEx.Message, type: "order_placed", fields: [:order_id, :customer_id, :total]
end

# in a Plug.Router
forward "/stream", to: DMEx.Log.Plug, log_module: MyApp.Log

MyApp.Log.append(%MyApp.Messages.OrderPlaced{order_id: 7})
#=> {:ok, 1042}
```

## Reading

```elixir
defmodule MyApp.OrderProjection do
  use DMEx.Handler, logs: [:orders], name: :order_projection

  @impl true
  def handle_message(%MyApp.Messages.OrderPlaced{} = message, _metadata), do: :ok
end

# the whole supervision-tree declaration
{DMEx.Supervisor,
 otp_app: :my_app,
 repo: MyApp.Repo,
 bindings: [
   catalogue: MyApp.Log,                        # owned: started here
   orders: "https://orders.internal/stream"     # someone else's: read
 ]}
```

A single-repo application can name its repo once instead — `config :dmex, repo:
MyApp.Repo` — and omit `repo:` from `use DMEx.Log`, the supervisor, and the
operator-facing reads below.

Handler modules are never listed: every one compiled into an application that
depends on `:dmex` is discovered and started — one subscription per log in its
`logs:`, each with its own cursor — and one naming a log that `bindings:` does not
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

Identities are declared, never derived from a module name, so renaming or moving
a module is free: a subscription's is the `"orders/order_projection"` pair, and a
message's is its `type:` — which is what lets a producer and its consumers
reorganise their message modules on their own schedules.

Start with the `DMEx` moduledoc, then `DMEx.Config` for every default and why it
has the value it does.

## Querying

Reading without subscribing — a console, an operator script, a backfill:

```elixir
DMEx.Query.all(MyApp.Messages.OrderPlaced, repo: MyApp.Repo)
#=> {:ok, [%MyApp.Messages.OrderPlaced{order_id: "0191f3c8-…"}]}
```

A message can also declare which of its fields hold **entity identifiers**,
which are indexed as it is appended:

```elixir
defmodule MyApp.Messages.OrderShipped do
  use DMEx.Message,
    type: "order_shipped",
    fields: [:order_id, :carrier],
    entities: [:order_id]
end

# one entity's history, across every type that mentions it
DMEx.Query.all(:all, entities: [order_id])

# the same, lazily, for a history longer than memory
DMEx.Query.stream(:all, entities: [order_id], limit: 500)
```

An identifier is stored exactly as the message declared it — DMEx composes
nothing into it and parses nothing out — which is what lets two message types
join on one, and why identifiers are strings with a UUID as the expected shape.
`entities/1` is generated and cannot be overridden, so the index stays a
projection of the message's own fields and is rebuildable from the log.

This is not a consumption path: no cursor is read or written and nothing is
retried. The entity axis is storage-side only and never reaches the wire —
[ARCHITECTURE.md](ARCHITECTURE.md) has the four reasons.

## Failure handling

A handler that raises is retried in process up to `max_attempts` (3) with
jittered backoff. Once those are exhausted, `on_failure` decides:

- `:dead_message` (default) — record it, in the same transaction as the advance,
  and carry on.
- `:crash` — the subscription crash-loops on that message instead, at
  reconnect-backoff pace, without disturbing its siblings.
- `:skip` — advance past it; the failure is logged and emitted as telemetry, and
  that is the only trace.

`dmex_dead_messages` is **structurally a message log**, so replay hands a row
back rather than rebuilding a message from parts:

    DMEx.Handler.DeadMessages.list(MyApp.OrderProjection, repo: MyApp.Repo)
    DMEx.Handler.DeadMessages.replay!(MyApp.OrderProjection, repo: MyApp.Repo)

## Backpressure

**A consumer has no buffer at all**, and that is the whole mechanism: rows run
through the pipeline inline on the socket path, so while a handler is running the
socket is not re-armed, the TCP receive window closes, and the producer's
`chunk/2` blocks on it. Pacing is the producer's — the party whose database,
memory and request processes it protects — and `DMEx.Config` has the three
settings. See [ARCHITECTURE.md](ARCHITECTURE.md)'s "reads are gated on sends",
"the consumer has no buffer" and "pacing has one owner".

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

The SQLite half of the storage suite needs no server — `mix test --only sqlite`
runs it, and it is one of the two adapters the conformance suite covers.

Point `DMEX_POSTGRES_URL` at your own instance. The integration database is
derived from it by replacing the database name with `dmex_integration_test`, so
any name works. Set `DMEX_POSTGRES_INTEGRATION_URL` to override it; pointing both
at one database raises at config load, since the integration one is not
sandboxed.

The integration suite is separate because it cannot use the Ecto sandbox — see
`config/test.exs`.

## Benchmarks

```sh
mix bench.micro        # per-row costs on the hot paths
mix bench.e2e          # throughput and latency against a real database and socket
mix bench.fingerprint  # the selection filter's error rate and size
```

Decision tools, not regression tests, so they do not run in CI. Each file states
the design claim or open question it exists to answer.
