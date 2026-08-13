defmodule DMEx do
  @moduledoc """
  A decentralized, broker-less message queue over plain HTTP and Server-Sent Events.

  Every application that appends messages owns and persists its own append-only
  message log — one ordinary SQL table — and streams it directly to its readers
  over HTTP. See [ARCHITECTURE](ARCHITECTURE.md) for the design.

  ## Installing

  DMEx's tables live in your database, so they are yours to migrate — see
  `DMEx.Storage.Migration` for the per-role subsets.

      defmodule MyApp.Repo.Migrations.AddDMEx do
        use Ecto.Migration

        def up, do: DMEx.Storage.Migration.up()
        def down, do: DMEx.Storage.Migration.down()
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

      # in a Plug.Router
      forward "/stream", to: DMEx.Log.Plug, log_module: MyApp.Log

      MyApp.Log.append(%MyApp.Messages.OrderPlaced{order_id: 7})
      #=> {:ok, 1042}

  ## Reading

  A handler module declares its subscriptions in full: which logs, under what
  name, and — from the structs its clauses match on — which message types.

      defmodule MyApp.OrderProjection do
        use DMEx.Handler, logs: [:orders], name: :order_projection

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
         bindings: [
           catalogue: MyApp.Log,
           orders: "https://orders.internal/stream"
         ]}
      ]

  `DMEx.Supervisor` has the vocabulary: what each kind of binding means, why it
  is also what chooses the transport, why handler modules are not listed, and
  every option.

  A log bound to a module is read in this VM — a query and a handler call, no
  socket and no round trip through this application's own web server — and a
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
      an occasional extra type arrives and is skipped. See
      `DMEx.Handler.Fingerprint`.

    * **Delivery is at-least-once.** The handler call and the cursor advance
      share one transaction, so side effects through the reader's own repo are
      exactly-once for free. Effects that leave that transaction must be
      idempotent.

    * **Appending is serialized per log table** — what keeps every reader's
      `id > cursor` scan sound, at the price of append concurrency.
      `DMEx.Storage` covers how each adapter buys it and what it means for an
      `append/2` inside your own transaction.

  `DMEx.Telemetry` documents every event DMEx emits.
  """

  @doc """
  Every module of every application that could hold a DMEx declaration.

  That is every loaded application declaring a dependency on `:dmex`, plus
  `otp_app` when given. A module cannot `use DMEx.Message` or `use DMEx.Handler`
  unless its application depends on `:dmex`, so the condition is exact rather
  than a heuristic — and it keeps the scan off the thousands of OTP and Elixir
  modules that could never qualify. It also makes a **shared package** work: a
  library of message modules lists `:dmex` and is scanned like any other.

  `otp_app` covers the case the rule misses — an application that does not
  itself depend on `:dmex`, such as an umbrella parent or DMEx's own test suite.
  """
  @spec discovery(atom() | nil) :: [module()]
  def discovery(otp_app \\ nil) do
    for {app, _description, _vsn} <- Application.loaded_applications(),
        app == otp_app or :dmex in Application.spec(app, :applications),
        module <- Application.spec(app, :modules),
        do: module
  end

  @doc """
  The jittered exponential backoff delay in ms before attempt `attempt`,
  counting from 1. Shared by reconnects and local retries.

  A uniform draw from `[1, ceiling]`, where `ceiling` is `base * 2^(attempt - 1)`
  capped at `max`.

  **Jitter is the point, not a refinement.** An event that fails many consumers
  at once — a producer restart, a network blip, a deploy — would otherwise bring
  them all back simultaneously, and DMEx has no admission control to absorb
  that. Full jitter decorrelates hardest.

  The same argument applies to a producer's advertised `retry:` value, which is
  why DMEx's own client keeps its own schedule rather than reconnecting at
  exactly the advertised interval.
  """
  @spec backoff(pos_integer(), pos_integer(), pos_integer()) :: pos_integer()
  def backoff(attempt, base, max) when is_integer(attempt) and attempt >= 1,
    do: :rand.uniform(ceiling(base, max, attempt - 1))

  # Doubling with an early exit rather than `min(max, base * 2^(attempt - 1))`.
  # `attempt` is unbounded — a subscription whose origin is down counts up for as
  # long as it runs, and only a successful connect resets it — and the power is
  # evaluated before the cap, so the closed form allocates a `2^n` bignum per
  # reconnect, growing forever, to answer `max` every time.
  defp ceiling(ceiling, max, _doublings) when ceiling >= max, do: max
  defp ceiling(ceiling, _max, 0), do: ceiling
  defp ceiling(ceiling, max, doublings), do: ceiling(ceiling * 2, max, doublings - 1)
end
