defmodule DMEx do
  @moduledoc """
  A decentralized, broker-less message queue over plain HTTP and Server-Sent Events.

  Every application that appends messages owns and persists its own append-only
  message log — one ordinary SQL table — and streams it directly to its readers
  over HTTP. See `ARCHITECTURE.md` for the design.

  ## Installing

  DMEx's tables live in your database, so they are yours to migrate. Pass
  `tables: [:log]` or `tables: [:subscriptions]` if the application only plays
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
      forward "/stream", to: DMEx.Log.Plug, log: MyApp.Log

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
      an occasional extra type arrives and is skipped. See
      `DMEx.Handler.Fingerprint`.

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

  # A log name, a handler's `id:`, and a log's `app:` are all the same kind of
  # name, and the errors for each say so — so the rule is written once.
  @identifier ~r/\A[a-z][a-z0-9_]*\z/

  @doc """
  The charset every DMEx identifier shares — a log name, a handler `id:`, a log
  `app:`. Exposed so the three call sites cannot drift, and so each can quote it
  in its own error.
  """
  @spec identifier_format() :: Regex.t()
  def identifier_format, do: @identifier

  @doc "Whether `value` is a well-formed DMEx identifier."
  @spec identifier?(term()) :: boolean()
  def identifier?(value) when is_binary(value), do: Regex.match?(@identifier, value)
  def identifier?(_value), do: false

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
    otp_app
    |> applications()
    |> Enum.flat_map(&app_modules/1)
  end

  defp applications(otp_app) do
    dependents =
      for {app, _description, _vsn} <- Application.loaded_applications(),
          depends_on_dmex?(app),
          do: app

    Enum.uniq(List.wrap(otp_app) ++ dependents)
  end

  defp depends_on_dmex?(app) do
    case :application.get_key(app, :applications) do
      {:ok, applications} -> :dmex in applications
      _other -> false
    end
  end

  defp app_modules(app) do
    case :application.get_key(app, :modules) do
      {:ok, modules} -> modules
      _other -> []
    end
  end

  @doc """
  Every message type visible to the application owning `module`, cached.

  What a consumer's filter is resolved against: `DMEx.Log.Plug` tests every type
  here for membership once at connect, and the types that pass become the
  connection's `message_type = ANY($1)` predicate and its wakeup routing set.

  ## Why the *modules*, and not a log's contents

  Expanding over the types a log has actually seen — `SELECT DISTINCT
  message_type` — goes stale silently: a log's code can know a type it has not
  yet emitted, and first emission involves no restart and no reconnect, so an
  open connection would keep an expansion missing a type its consumer genuinely
  selected. That is a false negative, which is silent message loss.

  Compiled modules have no such gap: that set changes only on deploy, and a
  deploy takes every SSE connection with it, so every affected consumer
  reconnects and re-expands.

  ## `module` names the *application*, not the subject

  `discovery/1` scans applications, so this is never per log: two logs owned by
  one application resolve against one set, which is sound because the set only
  has to *contain* everything a log might append — an extra type matches no
  filter, a missing one is silent loss.

  The argument is how the owning application is found, via
  `:application.get_application/1`, so a caller holding a log module needs to
  know nothing else — no `otp_app:`, and no reference to whichever
  `DMEx.Supervisor` started it. That is also the cache key, so two supervisors
  running different `otp_app:` values in one node hold separate entries instead
  of overwriting each other's.

  A module belonging to no application — one defined at runtime — resolves to
  `nil`, which scans the dependent applications alone. So does a bare
  `refresh_message_types/1`.

  The lazy branch keeps every caller free of start order: a node whose
  `DMEx.Supervisor` has not run `refresh_message_types/1` — or that runs no
  supervisor at all — still answers correctly, just more slowly the first time.
  """
  @spec message_types(module()) :: MapSet.t(String.t())
  def message_types(module) when is_atom(module) do
    otp_app = owning_app(module)

    case :persistent_term.get(message_types_key(otp_app), nil) do
      nil -> refresh_message_types(otp_app)
      types -> types
    end
  end

  @doc """
  Rescans and caches an application's message types, raising if two modules
  declare one type.

  Called from `DMEx.Supervisor`'s `init/1` with its `otp_app:` — so an
  application owning no log gets the check too — and once more per application
  owning one of its logs, since that is the key `message_types/1` reads under.
  Both the boot-time collision check and the cache seed.
  """
  @spec refresh_message_types(atom() | nil) :: MapSet.t(String.t())
  def refresh_message_types(otp_app \\ nil) do
    index = otp_app |> DMEx.Message.discover() |> DMEx.Message.index!()
    types = index |> Map.keys() |> MapSet.new()

    # Two entries, one scan and one collision check: the set is what a filter is
    # expanded against, the index is what a whole-log handler resolves a type
    # through, and writing them together is what stops them disagreeing.
    :persistent_term.put(message_index_key(otp_app), index)
    :persistent_term.put(message_types_key(otp_app), types)

    types
  end

  @doc """
  The message module declaring `type`, as the application owning `module` sees
  it, or `nil`.

  How a handler decodes a type it never named: `DMEx.Handler` compiles a clause
  per type its callback did name, and — for a handler with a clause that names
  none, which asks for the whole log — a fallback that comes here. A **lookup, never `String.to_atom/1`** — the key is a string
  a log sent, so resolving it through anything that creates atoms would make an
  unbounded atom table out of attacker-controlled input.

  Same cache, same scan and same collision check as `message_types/1`; see there
  for why the modules rather than the log's contents, and why the key is the
  application rather than the log.
  """
  @spec message_module(module(), String.t()) :: module() | nil
  def message_module(module, type) when is_atom(module) and is_binary(type) do
    otp_app = owning_app(module)

    index =
      case :persistent_term.get(message_index_key(otp_app), nil) do
        nil ->
          _ = refresh_message_types(otp_app)
          :persistent_term.get(message_index_key(otp_app), %{})

        index ->
          index
      end

    Map.get(index, type)
  end

  defp message_types_key(otp_app), do: {__MODULE__, :message_types, otp_app}
  defp message_index_key(otp_app), do: {__MODULE__, :message_index, otp_app}

  @doc false
  @spec owning_app(module()) :: atom() | nil
  def owning_app(module) do
    case :application.get_application(module) do
      {:ok, app} -> app
      :undefined -> nil
    end
  end

  # Every option vocabulary in DMEx — `use DMEx.Log`, `use DMEx.Handler`,
  # `use DMEx.Message`, `DMEx.Supervisor.subscribe/2` — is checked here, so the
  # four say the same thing about an unknown key and none of them can drift.
  # `source` names the call site ("`use DMEx.Log` in MyApp.Log"); `hint` is
  # whatever else that one site has to say.

  @doc false
  @spec keyword!(Macro.t(), String.t()) :: keyword()
  def keyword!(opts, source) do
    if Keyword.keyword?(opts) do
      opts
    else
      raise ArgumentError, "#{source} takes a keyword list, got: #{Macro.to_string(opts)}"
    end
  end

  @doc false
  @spec validate_keys!(keyword(), [atom()], String.t(), String.t()) :: :ok
  def validate_keys!(opts, known, source, hint \\ "") do
    case Enum.reject(Keyword.keys(opts), &(&1 in known)) do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "unknown option#{if length(unknown) > 1, do: "s"} #{inspect(unknown)} " <>
                "passed to #{source}. Known options: #{inspect(Enum.sort(known))}." <> hint
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
  @spec backoff(pos_integer(), pos_integer(), pos_integer()) :: non_neg_integer()
  def backoff(attempt, base, max) when is_integer(attempt) and attempt >= 1 and is_integer(base) and is_integer(max) do
    ceiling = min(max, base * Integer.pow(2, attempt - 1))

    :rand.uniform(ceiling)
  end
end
