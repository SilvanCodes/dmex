defmodule DMEx do
  @moduledoc """
  A decentralized, broker-less message queue over plain HTTP and Server-Sent Events.

  Every application that appends messages owns and persists its own append-only
  message log — one ordinary SQL table — and streams it directly to its readers
  over HTTP. There is no broker process, no cluster to operate, and no shared
  infrastructure between applications.

  See `ARCHITECTURE.md` for the full design and the reasoning behind it.

  ## Installing

  DMEx lives in your database, so its tables are yours to migrate:

      defmodule MyApp.Repo.Migrations.AddDMEx do
        use Ecto.Migration

        def up, do: DMEx.Migration.up()
        def down, do: DMEx.Migration.down()
      end

  Pass `tables: [:log]` or `tables: [:subscriptions]` if the application only
  plays one of the two roles. See `DMEx.Migration`.

  ## Appending

  An application that appends declares a log module and one module per message
  type:

      defmodule MyApp.Log do
        use DMEx.Log, repo: MyApp.Repo
      end

      defmodule MyApp.Messages.OrderPlaced do
        use DMEx.Message

        @derive JSON.Encoder
        defstruct [:order_id, :customer_id, :total]
      end

  Mount its endpoint — from a Phoenix router, a `Plug.Router`, or a bare
  Bandit/Cowboy pipeline:

      forward "/stream", to: DMEx.Log.Plug, init_opts: [log: MyApp.Log]

  Appending is then just a row written to the application's own database:

      MyApp.Log.append(%MyApp.Messages.OrderPlaced{order_id: 7})
      #=> {:ok, 1042}

  Generic code that does not have a particular log module in hand calls it
  through a variable — `log.append(message)` — rather than through a second,
  explicit appending API.

  ## Reading

  A handler module declares its subscriptions in full — which logs, under what
  name, and, from the structs its clauses match on, for which message types:

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
  that depends on `:dmex` is discovered and started — one subscription per log
  it names — and one naming a log that `logs:` does not bind stops the boot
  rather than silently never running. `DMEx.Supervisor.subscribe/2` starts one
  at runtime instead, taking the same module.

  The URLs are static configuration of the reading application — there is
  deliberately no discovery service in that path, since it would reintroduce
  exactly the shared infrastructure DMEx exists to avoid.

  `logs:` is a list, so one handler may read several: each is its own
  subscription, with its own connection and its own cursor, running the same
  clauses.

  DMEx owns the read position (`last_seen`) on the handler's behalf, keyed by the
  declared `"log/id"` pair, and the log holds no reader state at all. Keying on
  the declaration rather than the module name is what makes renaming a handler
  free.

  ## Three things worth knowing before you build on it

    * **A subscription receives the types its handler matches on, and there is
      still no filter option.** The selection is *derived* from the
      `handle_message/2` clauses, encoded into the request path at compile time,
      and resolved by the log against its own message modules — so nothing is
      declared anywhere to make it happen, and no separate list can drift from
      the clauses. Inclusion is heuristic in one direction only: never a false
      negative, and an occasional extra type arrives and is skipped exactly as an
      unknown type always was. See `DMEx.Fingerprint` and
      `guides/fingerprint-spec.md`.

    * **Delivery is at-least-once.** The handler call and the cursor advance
      share one transaction, so side effects that go through the reader's own
      repo are exactly-once for free. Effects that leave that transaction must
      be idempotent: a crash between them landing and the commit is
      indistinguishable from never having processed the message.

    * **Appending is serialized per application.** An advisory lock held to
      COMMIT is what keeps every reader's `id > cursor` scan sound. It costs
      append concurrency, and an `append/2` inside a long enclosing transaction
      extends that lock for the transaction's lifetime.

  ## Telemetry

  `[:dmex, :append, :start | :stop | :exception]`, `[:dmex, :stream, :connect]`,
  `[:dmex, :stream, :write]`, `[:dmex, :dispatcher, :broadcast]`,
  `[:dmex, :handler, :message, :start | :stop | :exception]`,
  `[:dmex, :handler, :skip]`, `[:dmex, :handler, :lag]`,
  `[:dmex, :handler, :reconnect]`. `DMEx.Telemetry` documents all of them.

  The two most useful in practice: `[:dmex, :handler, :lag]` carries how stale
  the rows this subscription is handling are, and `[:dmex, :handler, :skip]`
  fires for every row it had no module for — without it, a subscription
  discarding most of a stream and one discarding the single type it wanted look
  identical.
  """
end
