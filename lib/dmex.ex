defmodule DMEx do
  @moduledoc """
  A decentralized, broker-less message queue over plain HTTP and Server-Sent Events.

  Every application that publishes messages owns and persists its own append-only
  message log — one ordinary SQL table — and streams it directly to consumers over
  HTTP. There is no broker process, no cluster to operate, and no shared
  infrastructure between applications.

  See `ARCHITECTURE.md` for the full design and the reasoning behind it.

  ## Installing

  DMEx lives in your database, so its tables are yours to migrate:

      defmodule MyApp.Repo.Migrations.AddDMEx do
        use Ecto.Migration

        def up, do: DMEx.Migration.up()
        def down, do: DMEx.Migration.down()
      end

  Pass `tables: [:producer]` or `tables: [:consumer]` if the application only
  plays one of the two roles. See `DMEx.Migration`.

  ## Publishing

  A producing application declares a producer module and one module per message
  type:

      defmodule MyApp.Producer do
        use DMEx.Producer, repo: MyApp.Repo
      end

      defmodule MyApp.Messages.OrderPlaced do
        use DMEx.Message

        @derive JSON.Encoder
        defstruct [:order_id, :customer_id, :total]
      end

  Add the producer to your supervision tree and mount its endpoint — from a
  Phoenix router, a `Plug.Router`, or a bare Bandit/Cowboy pipeline:

      children = [MyApp.Repo, MyApp.Producer]

      forward "/stream", to: DMEx.Producer.Plug, init_opts: [producer: MyApp.Producer]

  Publishing is then just a row written to the application's own database:

      MyApp.Producer.publish(%MyApp.Messages.OrderPlaced{order_id: 7})
      #=> {:ok, 1042}

  `DMEx.Publisher.publish/3` is the explicit form of the same call, for generic
  code that does not have a particular producer module in hand.

  ## Consuming

  A consuming application implements a handler and declares its subscriptions in
  its supervision tree:

      defmodule MyApp.OrderProjection do
        use DMEx.Consumer.Handler,
          producer: :orders,
          id: :order_projection,
          messages: [MyApp.Messages.OrderPlaced]

        @impl true
        def handle_message(%MyApp.Messages.OrderPlaced{} = message, _metadata) do
          :ok
        end
      end

      {DMEx.Consumer,
       repo: MyApp.Repo,
       producer: %{orders: "https://orders.internal/stream"},
       handlers: [MyApp.OrderProjection]}

  The handler module is the whole declaration — which producer, under what name,
  for which message types — and the consumer binds producer names to URLs.
  `DMEx.Consumer.subscribe/2` starts one at runtime instead, taking the same
  module. The URLs are static configuration of the consuming application — there
  is deliberately no discovery service in that path, since it would reintroduce
  exactly the shared infrastructure DMEx exists to avoid.

  DMEx owns the read position (`last_seen`) on the consumer's behalf, keyed by
  the declared `"producer/id"` pair, and the producer holds no consumer state at
  all. Keying on the declaration rather than the module name is what makes
  renaming a handler free.

  ## Three things worth knowing before you build on it

    * **A v1 subscription is the producer's whole log.** There is no filter
      option of any kind. A handler's `messages:` is what narrows it, locally, after
      the bytes have crossed the wire — a row whose type is not in that list
      advances the cursor and invokes nothing. Server-side selection has a
      design and is deliberately not in v1.

    * **Delivery is at-least-once.** The handler call and the cursor advance
      share one transaction, so side effects that go through the consumer's own
      repo are exactly-once for free. Effects that leave that transaction must
      be idempotent: a crash between them landing and the commit is
      indistinguishable from never having processed the message.

    * **Publishing is serialized per application.** An advisory lock held to
      COMMIT is what keeps every reader's `id > cursor` scan sound. It costs
      publish concurrency, and a `publish/2` inside a long enclosing transaction
      extends that lock for the transaction's lifetime.

  ## Telemetry

  `[:dmex, :publish, :start | :stop | :exception]`, `[:dmex, :stream, :connect]`,
  `[:dmex, :stream, :frame]`, `[:dmex, :dispatcher, :broadcast]`,
  `[:dmex, :consumer, :message, :start | :stop | :exception]`,
  `[:dmex, :consumer, :skip]`, `[:dmex, :consumer, :lag]`,
  `[:dmex, :consumer, :reconnect]`.

  The two most useful in practice: `[:dmex, :consumer, :lag]` carries how far
  behind the tip a frame left this subscription, and `[:dmex, :consumer, :skip]`
  fires for every row the consumer had no module for — without it, a consumer
  discarding most of a stream and one discarding the single type it wanted look
  identical.
  """
end
