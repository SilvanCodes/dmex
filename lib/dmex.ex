defmodule DMEx do
  @moduledoc """
  A decentralized, broker-less message queue over plain HTTP and Server-Sent Events.

  Every application that appends messages owns and persists its own append-only
  message log — one ordinary SQL table — and streams it directly to its readers
  over HTTP. See `ARCHITECTURE.md` for the design.

  ## Installing

  DMEx's tables live in your database, so they are yours to migrate. Pass
  `tables: [:log]` or `tables: [:subscriptions]` if the application only plays
  one of the two roles; see `DMEx.Migration`.

      defmodule MyApp.Repo.Migrations.AddDMEx do
        use Ecto.Migration

        def up, do: DMEx.Migration.up()
        def down, do: DMEx.Migration.down()
      end

  ## Appending

  An application that appends declares a log module and one module per message
  type, mounts the endpoint, and writes rows:

      defmodule MyApp.Log do
        use DMEx.Log, repo: MyApp.Repo
      end

      defmodule MyApp.Messages.OrderPlaced do
        use DMEx.Message,
          type: "order_placed",
          fields: [:order_id, :customer_id, :total]
      end

      # in a Phoenix router, a Plug.Router, or a bare Bandit/Cowboy pipeline
      forward "/stream", to: DMEx.Log.Plug, init_opts: [log: MyApp.Log]

      MyApp.Log.append(%MyApp.Messages.OrderPlaced{order_id: 7})
      #=> {:ok, 1042}

  Generic code without a particular log module in hand calls it through a
  variable — `log.append(message)` — rather than through a second API.

  ## Reading

  A handler module declares its subscriptions in full: which logs, under what
  name, and — from the structs its clauses match on — which message types.

      defmodule MyApp.OrderProjection do
        use DMEx.Handler, logs: [:orders], id: :order_projection

        @impl true
        def handle_message(%MyApp.Messages.OrderPlaced{} = message, _metadata) do
          :ok
        end
      end

  ## Starting it

  One child, naming every log this application knows about — its own as modules,
  everyone else's as URLs:

      children = [
        MyApp.Repo,
        {DMEx.Supervisor,
         otp_app: :my_app,
         repo: MyApp.Repo,
         logs: [
           catalogue: MyApp.Log,
           orders: "https://orders.internal/stream"
         ]}
      ]

  Handler modules are **not** listed: every one compiled into an application
  depending on `:dmex` is discovered and started, one subscription per log it
  names, and one naming a log that `logs:` does not bind stops the boot.
  `DMEx.Supervisor.subscribe/2` starts one at runtime instead.

  **The binding chooses the transport, and nothing else configures one.** A log
  bound to a module is read in this VM (`DMEx.Transport.Local`) — a query and a
  handler call, no socket and no round trip through this application's own web
  server. A log bound to a URL is read over HTTP (`DMEx.Transport.SSE`). A
  handler cannot tell the difference. Those URLs are static configuration of the
  reading application: a discovery service in that path would reintroduce
  exactly the shared infrastructure DMEx exists to avoid.

  DMEx owns the read position (`last_seen`), keyed by the declared `"log/id"`
  pair rather than the module name, which is what makes renaming a handler free.

  ## Three things worth knowing before you build on it

    * **A subscription receives the types its handler matches on, and there is
      no filter option.** The selection is derived from the `handle_message/2`
      clauses and resolved by the log against its own message modules.
      Inclusion is heuristic in one direction only: never a false negative, and
      an occasional extra type arrives and is skipped. See `DMEx.Fingerprint`.

    * **Delivery is at-least-once.** The handler call and the cursor advance
      share one transaction, so side effects through the reader's own repo are
      exactly-once for free. Effects that leave that transaction must be
      idempotent.

    * **Appending is serialized per application.** An advisory lock held to
      COMMIT is what keeps every reader's `id > cursor` scan sound. It costs
      append concurrency, and an `append/2` inside a long enclosing transaction
      extends that lock for the transaction's lifetime.

  `DMEx.Telemetry` documents every event DMEx emits.
  """
end
