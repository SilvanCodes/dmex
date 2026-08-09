defmodule DMEx do
  @moduledoc """
  A decentralized, broker-less message queue over plain HTTP and Server-Sent Events.

  Every application that appends messages owns and persists its own append-only
  message log — one ordinary SQL table — and streams it directly to its readers
  over HTTP. See `ARCHITECTURE.md` for the design.

  ## Installing

  DMEx's tables live in your database, so they are yours to migrate. Pass
  `tables: [:messages]` or `tables: [:cursors, :dead_messages]` if the application only plays
  one of the two roles; see `DMEx.Storage.Migration`.

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

  Generic code without a particular log module in hand calls it through a
  variable — `log.append(message)` — rather than through a second API.

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
      an occasional extra type arrives and is skipped. See
      `DMEx.Handler.Fingerprint`.

    * **Delivery is at-least-once.** The handler call and the cursor advance
      share one transaction, so side effects through the reader's own repo are
      exactly-once for free. Effects that leave that transaction must be
      idempotent.

    * **Appending is serialized per table.** An advisory lock held to
      COMMIT is what keeps every reader's `id > cursor` scan sound. It costs
      append concurrency, and an `append/2` inside a long enclosing transaction
      extends that lock for the transaction's lifetime.

  `DMEx.Telemetry` documents every event DMEx emits.
  """

  # A log name and a handler's `name:` are the same kind of name, so the rule is
  # written once.
  @identifier ~r/\A[a-z][a-z0-9_]*\z/

  @doc "Whether `value` is a well-formed DMEx identifier."
  @spec identifier?(term()) :: boolean()
  def identifier?(value), do: is_binary(value) and Regex.match?(@identifier, value)

  # Every identifier is checked and reported here, so neither error can drift.
  # `source` names the value ("a log name", "`:name` in MyApp.Log"); `hint` is
  # whatever else that one site has to say. Atoms and strings render identically,
  # and a string comes back either way.

  @doc false
  @spec validate_identifier!(term(), String.t(), String.t()) :: String.t()
  def validate_identifier!(value, source, hint \\ "") do
    name = if is_atom(value) and not is_nil(value), do: Atom.to_string(value), else: value

    if !identifier?(name) do
      raise ArgumentError,
            "#{source} must be an atom or a string matching #{inspect(@identifier)}, " <>
              "got: #{inspect(name)}." <> hint
    end

    name
  end

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

  # Every option vocabulary in DMEx — `use DMEx.Log`, `use DMEx.Handler`,
  # `use DMEx.Message`, `DMEx.Supervisor.subscribe/2` — is checked here, so the
  # four say the same thing about a missing or unknown key and none of them can
  # drift. `source` names the call site ("`use DMEx.Log` in MyApp.Log"); `hint`
  # is whatever else that one site has to say. Returning means every key in
  # `required` is present, so the code after it fetches rather than matching.

  @doc false
  @spec validate_keys!(Macro.t(), [atom()], String.t(), String.t(), [atom()]) :: :ok
  def validate_keys!(opts, known, source, hint, required \\ []) do
    if !Keyword.keyword?(opts) do
      raise ArgumentError, "#{source} takes a keyword list, got: #{Macro.to_string(opts)}"
    end

    all = Keyword.keys(opts)
    keys = Enum.uniq(all)

    unknown = keys -- known
    repeated = Enum.uniq(all -- keys)
    missing = required -- keys

    cond do
      unknown != [] ->
        raise ArgumentError,
              "unknown option#{if length(unknown) > 1, do: "s"} #{inspect(unknown)} " <>
                "passed to #{source}. Known options: #{inspect(Enum.sort(known))}." <> hint

      repeated != [] ->
        raise ArgumentError,
              "#{source} passes #{Enum.map_join(repeated, ", ", &inspect/1)} more than once. " <>
                "Only the first is read, so a repeat could only ever mean one of them." <> hint

      missing != [] ->
        raise ArgumentError,
              "#{source} requires #{Enum.map_join(missing, ", ", &inspect/1)}." <> hint

      true ->
        :ok
    end
  end

  @doc """
  The jittered exponential backoff delay in ms before attempt `attempt`,
  counting from 1. Shared by reconnects and local retries.

  A uniform draw from `[1, ceiling]`, where `ceiling` is `base * 2^(attempt - 1)`
  capped at `max`.

  **Jitter is the point, not a refinement.** An event that fails many consumers
  at once — a producer restart, a network blip, a deploy — would otherwise bring
  them all back simultaneously, and DMEx has no admission control to absorb
  that. Full jitter (a uniform draw from `[1, capped]` rather than a band around
  the ceiling) decorrelates hardest. The draw starts at 1 so a delay is always a
  delay.

  The same argument applies to a producer's advertised `retry:` value, which is
  why DMEx's own client keeps its own schedule rather than reconnecting at
  exactly the advertised interval.
  """
  @spec backoff(pos_integer(), pos_integer(), pos_integer()) :: pos_integer()
  def backoff(attempt, base, max) when is_integer(attempt) and attempt >= 1,
    do: :rand.uniform(min(max, base * Integer.pow(2, attempt - 1)))
end
