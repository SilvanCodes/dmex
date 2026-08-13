defmodule DMEx.Supervisor do
  @moduledoc """
  DMEx's entry point: every log this application knows about, and every handler
  it runs.

      {DMEx.Supervisor,
       otp_app: :my_app,
       repo: MyApp.Repo,
       logs: [
         orders: MyApp.OrdersLog,
         returns: MyApp.ReturnsLog,
         billing: "https://billing.internal/stream"
       ]}

  One list, two kinds of entry. A **module** is a log this application owns,
  started here and served over `DMEx.Log.Plug`; a **URL** is someone else's.
  Both are just names as far as a handler is concerned, which lets a log move
  between the two. The binding also **chooses the transport**, which is why
  nothing configures one: a module is read in this VM (`DMEx.Transport.Local`),
  a URL over HTTP (`DMEx.Transport.SSE`).

  Handlers are **not** listed. Every `use DMEx.Handler` module compiled into an
  application depending on `:dmex` is discovered and started, one subscription
  per log it names; all this needs to say is what each log name means. A
  discovered handler naming a log that is not bound here stops the boot, since
  the alternative is a subscription that silently never exists.

  Code deciding at runtime can still start one by hand:

      DMEx.Supervisor.subscribe(MyApp.OrderProjection, on_failure: :skip)

  ## Options

    * `:logs` — a keyword list of log name to either a `DMEx.Log` module (owned)
      or a stream URL (remote).
    * `:otp_app` — the application whose modules are scanned for handlers, and
      for the message types a *pure consumer* can see. Defaults to the Mix
      project; worth stating in a release. `DMEx.discovery/1` covers what is
      reached without it, and `otp_app: nil` scans nothing beyond that. Each
      application owning an owned log is scanned too — this one only, for a
      single-app host.
    * `:repo` — the Ecto repo holding `dmex_cursors`.
    * `:storage` — the `DMEx.Storage` adapter; defaults to `DMEx.Storage.Postgres`.
      `DMEx.Storage.SQLite` is the other shipped one.
    * `:prefix` — Postgres schema for the DMEx tables. SQLite has none and
      raises on it.
    * `:name` — names this supervisor, so an application can run more than one.

  A URL is not verifiable against reality: pointing an entry at the wrong host
  yields a subscription that looks perfectly healthy — connected, cursor
  advancing, every row skipped as an unknown type — while reading the wrong log.
  A selective subscription usually gets a `404` and says so; one whose clauses
  ask for the whole log gets a stream of skips.

  `repo:` (and optionally `storage:` / `prefix:`) is the default for every
  subscription under this supervisor. A `subscribe/2` call may override it, or
  resolve it from the supervisor named by `supervisor:`; with neither it raises
  and names both fixes. `logs:` resolves the same way.

  **Two subscriptions cannot share one identity** — `DMEx.Handler` covers the
  `{log, id}` pair. Checked across every discovered handler before any child
  starts, so a clash refuses the boot rather than clobbering a cursor, and again
  by the supervisor itself at runtime: a subscription id *is* its child id, so a
  second `subscribe/2` for one — from a differently-named handler included — is
  `{:error, {:already_subscribed, id}}` before the process exists.
  """

  use Supervisor

  alias DMEx.Config
  alias DMEx.Handler
  alias DMEx.Handler.Subscription
  alias DMEx.Handler.Subscriptions
  alias DMEx.Supervisor.Bindings

  @default_name __MODULE__
  # Every storage option and its default, in one list: the resolution at the
  # bottom of this module reads it and `subscribe/2` accepts it, so a fourth one
  # cannot be added to the reader and forgotten in the vocabulary — which would
  # reject a legal option.
  @storage_defaults [storage: DMEx.Storage.Postgres, repo: nil, prefix: nil]
  @storage_opts Keyword.keys(@storage_defaults)
  # `:handler` is deliberately absent: it is the positional argument, so a stray
  # `handler:` in the options is a mistake worth raising on.
  @subscription_opts [:supervisor, :logs]

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, @default_name),
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc false
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, @default_name)

    Supervisor.start_link(__MODULE__, {name, opts}, name: name)
  end

  @impl Supervisor
  def init({name, opts}) do
    logs = Bindings.logs!(opts)
    otp_app = otp_app(opts)

    # Before any child starts, and loudly: both failures are pure functions of the
    # declarations, so nothing has to be running to answer them.
    handlers = __startable__!(Handler.discover(otp_app), logs)

    # Both the collision check and the cache seed, and the only scan on the boot
    # path. One index for the node, but one scan per application that a scan
    # cannot reach on its own: `otp_app` unconditionally, so an application
    # owning no log — a pure consumer — is checked too, plus each application
    # owning a log, which is `otp_app` only for a single-app host. An umbrella
    # parent would otherwise be scanned lazily inside the first selective
    # connect, turning a collision into a 500. See `DMEx.Message.types/0`.
    for app <- Enum.uniq([otp_app | log_apps(logs)]), do: DMEx.Message.refresh_index(app)

    storage = storage(opts)

    children =
      owned_logs(logs) ++
        [
          # The default storage and the log bindings ride along as registry
          # metadata, seeded at start, which is why a bare subscribe/2 can find
          # them with no GenServer call.
          {Registry, keys: :unique, name: registry(name), meta: [storage: storage, logs: logs]},
          {Subscriptions, name: subscriptions(name), children: discovered(name, handlers, logs, storage)}
        ]

    # `:rest_for_one` because the registry owns what the subscriptions register
    # in, so everything after a dead child is rebuilt. The discovered
    # subscriptions are a child list rather than something a process starts, which
    # is the only shape a supervisor can put back: a `Task` or a one-shot
    # `GenServer` is gone from the child list once it has run, whatever its
    # `:restart` says, so an owned log's death used to cost every discovered
    # subscription for the lifetime of the node with nothing logged.
    #
    # A subscription started at runtime by `subscribe/2` is still lost that way —
    # it is in no child list by construction — and that is the remaining half of
    # the same problem.
    Supervisor.init(children, strategy: :rest_for_one)
  end

  # Every discovered handler's subscriptions, as child specs. The same options
  # `subscribe/2` builds, from the same function, so a discovered subscription and
  # a hand-started one cannot be configured differently.
  defp discovered(name, handlers, logs, storage) do
    for handler <- handlers,
        declaration = Handler.declaration!(handler),
        config = Config.handler!(declaration.config),
        log <- declaration.logs,
        do: {Subscription, child_opts(name, declaration, log, logs, config, storage)}
  end

  # The applications owning the logs this supervisor starts — the ones a scan
  # cannot reach on its own unless they are named.
  defp log_apps(logs), do: logs |> owned_logs() |> Enum.map(&Application.get_application/1)

  # `Enum.uniq` because two names may legitimately point at one owned log, and
  # a supervisor cannot hold the same child twice.
  defp owned_logs(logs) do
    for {_name, target} <- logs, is_atom(target), uniq: true, do: target
  end

  # Every application depending on `:dmex` is scanned anyway, so this only adds
  # one that does not: the host application in a release, the Mix project
  # everywhere else.
  defp otp_app(opts) do
    Keyword.get_lazy(opts, :otp_app, fn ->
      if Code.ensure_loaded?(Mix.Project), do: Mix.Project.config()[:app]
    end)
  end

  @doc """
  Starts the subscriptions `handler` declares — one per log it names.

  Everything about *what* is consumed comes off the module, as do its defaults
  for how it runs. The options here override those, and say where the cursor is
  written and which supervisor to attach to — never *which* logs, which is the
  module's to declare.

  Returns `{:error, {:already_subscribed, id}}` if one already has a live stream.
  Subscriptions started before it stay up: each is independent, and tearing down
  working ones to report on another helps nobody.
  """
  @spec subscribe(module(), keyword()) ::
          {:ok, [pid()]} | {:error, {:already_subscribed, String.t()}} | {:error, term()}
  def subscribe(handler, opts \\ []) when is_atom(handler) and is_list(opts) do
    supervisor = Keyword.get(opts, :supervisor, @default_name)

    # `declaration!/1` rather than a second `handler?/1` check plus a copy of its
    # message: it is the one place a module becomes its declaration, so every
    # caller rejects a non-handler identically.
    declaration = Handler.declaration!(handler)

    DMEx.validate_keys!(
      opts,
      @subscription_opts ++ @storage_opts ++ Config.handler_keys(),
      "`DMEx.Supervisor.subscribe/2`",
      " Note there is no filter or selection option here: the logs, the id, the " <>
        "message types and whether the log narrows the stream all belong to the " <>
        "handler module, not here."
    )

    bound = Bindings.resolve_logs!(opts, supervisor)
    storage = resolve_storage!(opts, supervisor)
    config = Config.handler!(Keyword.merge(declaration.config, opts))

    declaration.logs
    |> Enum.reduce_while({:ok, []}, fn log, {:ok, pids} ->
      child_opts = child_opts(supervisor, declaration, log, bound, config, storage)

      case Supervisor.start_child(subscriptions(supervisor), {Subscription, child_opts}) do
        {:ok, pid} -> {:cont, {:ok, [pid | pids]}}
        {:error, reason} -> {:halt, {:error, taken(reason, child_opts[:subscription])}}
      end
    end)
    |> case do
      {:ok, pids} -> {:ok, Enum.reverse(pids)}
      {:error, reason} -> {:error, reason}
    end
  end

  # A subscription id *is* the child id, so the supervisor catches a duplicate
  # before `DMEx.Handler.Subscription.init/1` reaches the registry — earlier, and
  # for a differently-named handler declaring the same pair too. Both spellings
  # become the one this function's `@doc` promises, since neither `:already_started`
  # nor `:already_present` says which subscription.
  defp taken({:already_started, _pid}, id), do: {:already_subscribed, id}
  defp taken(:already_present, id), do: {:already_subscribed, id}
  defp taken(reason, _id), do: reason

  # One subscription's whole input, built the same way whether `init/1` names it
  # in the child list or `subscribe/2` adds it at runtime.
  defp child_opts(supervisor, declaration, log, logs, config, storage) do
    {transport, origin} = Bindings.transport!(declaration, log, logs)

    [
      handler: declaration.handler,
      subscription: Handler.subscription_id!(declaration.handler, log),
      fingerprint: declaration.fingerprint,
      transport: transport,
      origin: origin,
      registry: registry(supervisor),
      config: config,
      storage: storage
    ]
  end

  @doc """
  Stops every subscription `handler` declares.

  The child spec goes with the process, so the same handler can be subscribed
  again — and a discovered one stays gone until the supervisor restarts, which is
  what makes this the operator's off switch rather than a pause.

  `{:error, :not_found}` when none of them is running.
  """
  @spec unsubscribe(module(), keyword()) :: :ok | {:error, :not_found}
  def unsubscribe(handler, opts \\ []) when is_atom(handler) do
    supervisor = Keyword.get(opts, :supervisor, @default_name)

    stopped =
      for id <- Handler.subscription_ids(handler),
          [{_pid, _}] <- [Registry.lookup(registry(supervisor), {:subscription, id})] do
        child = {Subscription, id}

        :ok = Supervisor.terminate_child(subscriptions(supervisor), child)
        :ok = Supervisor.delete_child(subscriptions(supervisor), child)
      end

    if stopped == [], do: {:error, :not_found}, else: :ok
  end

  @doc """
  Resolves `repo:`, `storage:` and `prefix:` into a `t:DMEx.Storage.opts/0`.

  The one place those three are read, so `subscribe/2`, `DMEx.Handler.Cursors`
  and `DMEx.Handler.DeadMessages` cannot drift. The `storage:` option names the
  adapter and lands on `:adapter`, which is what makes the result one value
  rather than a pair.

  Insists on a `repo:`. What the operator-facing modules use, since there is
  nothing running to fall back to when one is called from a migration or a
  console — where `subscribe/2` has the supervisor's default underneath it.
  """
  @spec storage!(keyword()) :: DMEx.Storage.opts()
  def storage!(opts), do: resolve_storage!(opts, nil)

  @doc "The registry name for a supervisor, where subscriptions register by id."
  @spec registry(atom()) :: atom()
  def registry(name \\ @default_name), do: Module.concat(name, "Registry")

  @doc "The dynamic supervisor name holding a supervisor's subscriptions."
  @spec subscriptions(atom()) :: atom()
  def subscriptions(name \\ @default_name), do: Module.concat(name, "Subscriptions")

  @doc """
  Returns the discovered handlers that may be started against `logs`, or raises.

  Both failures are pure functions of the declarations, so nothing has to be
  running to answer them — which is why `init/1` asks before starting anything.
  """
  @spec __startable__!([module()], Bindings.t()) :: [module()]
  def __startable__!(handlers, logs), do: Bindings.startable!(handlers, logs)

  ## Storage resolution

  # One resolution, two ways to reach it with no `repo:`. A `nil` supervisor is
  # the operator-facing case — a migration or a console, where nothing is
  # running and the missing option is the whole of the fix. Under a supervisor
  # the default it seeded into the registry is tried first, and only its absence
  # too is a mistake, which is a longer message because it has a second fix.
  defp resolve_storage!(opts, supervisor) do
    storage =
      case storage(opts) do
        %{repo: nil} when is_nil(supervisor) ->
          raise ArgumentError, "a `repo:` is required, e.g. `repo: MyApp.Repo`"

        %{repo: nil} ->
          # Checking the registry is alive first: Registry.meta/2 raises its own
          # "unknown registry" ArgumentError otherwise.
          with pid when is_pid(pid) <- Process.whereis(registry(supervisor)),
               {:ok, %{repo: repo} = default} when not is_nil(repo) <-
                 Registry.meta(registry(supervisor), :storage) do
            default
          else
            _ -> raise_no_repo(supervisor)
          end

        storage ->
          storage
      end

    # Here rather than only on the operator-facing door, so an option an adapter
    # cannot honour — `prefix:` on SQLite — fails at subscribe rather than
    # silently reading the unprefixed tables. Safe by this point: every branch
    # above has established a non-nil repo, which `c:DMEx.Storage.validate!/1`
    # reads.
    :ok = DMEx.Storage.validate!(storage)

    storage
  end

  # The options as one value. `:repo` may be nil here — a supervisor started
  # with no default is legal, and it is `init/1` that seeds whatever it got as
  # the registry metadata a later `subscribe/2` falls back to.
  defp storage(opts) do
    {adapter, storage} =
      @storage_defaults
      |> Map.new(fn {key, default} -> {key, Keyword.get(opts, key, default)} end)
      |> Map.pop(:storage)

    Map.put(storage, :adapter, adapter)
  end

  defp raise_no_repo(supervisor) do
    raise ArgumentError, """
    DMEx.Supervisor.subscribe/2 has nowhere to write this subscription's cursor.

    Either pass a repo on the call:

        DMEx.Supervisor.subscribe(MyApp.OrderProjection, repo: MyApp.Repo, logs: [...])

    or start #{inspect(supervisor)} in your supervision tree with a default:

        {DMEx.Supervisor, repo: MyApp.Repo, logs: [...]}
    """
  end
end
