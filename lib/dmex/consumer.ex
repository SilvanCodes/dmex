defmodule DMEx.Consumer do
  @moduledoc """
  Subscribes to producers' streams and keeps this application's read positions.

  A subscription is declared entirely on its handler module — which producer,
  under what name, for which message types — and this is where producer names
  are bound to URLs:

      {DMEx.Consumer,
       repo: MyApp.Repo,
       producer: %{
         orders: "https://orders.internal/stream",
         billing: "https://billing.internal/stream"
       },
       handlers: [
         MyApp.OrderProjection,
         {MyApp.OrderEmail, on_failure: :dead_message, max_batch_size: 100}
       ]}

  or start one later:

      DMEx.Consumer.subscribe(MyApp.OrderProjection, on_failure: :skip)

  Both forms exist for the same reason `DMEx.Publisher.publish/3` and
  `MyApp.Producer.publish/2` both do: the declarative one is what an application
  writes, the imperative one is for code deciding at runtime. Neither repeats
  what the handler already declares — see `DMEx.Consumer.Handler`.

  ## Producer names, not URLs

  A handler says `producer: :orders`; the map above says what `:orders` is. One
  entry per producer, in one place, instead of a URL literal repeated at every
  declaration site.

  The map lives on this child spec rather than in application config on purpose:
  DMEx reads no global configuration, two named consumers can bind the same name
  to different hosts, and a URL only known at runtime can be injected here.

  Static configuration of *this* application is where the URLs themselves come
  from — app config, an environment variable, a literal. There is deliberately
  no discovery service in this path: that would reintroduce a shared, centrally
  run piece of infrastructure everyone depends on, which is exactly what DMEx
  exists to avoid.

  **A name is not verifiable against reality.** Pointing an entry at the wrong
  host yields a subscription that looks perfectly healthy — connected, cursor
  advancing, every row skipped as an unknown type — while reading the wrong log.
  Skips are the normal case in v1, so they are not a signal.

  ## Where the cursor is written

  A subscription needs somewhere to keep `last_seen`, and it is resolved in
  three layers, with no global configuration anywhere:

    1. `repo:` (and optionally `storage:` / `prefix:`) on this child spec sets
       the default for every subscription under it.
    2. Any handler entry may override those for itself — for an application
       whose bookkeeping lives in a second repo.
    3. A bare `subscribe/2` with no `repo:` resolves from the running consumer
       named by `consumer:` (default `DMEx.Consumer`).

  With none of those, `subscribe/2` raises and names both fixes rather than
  guessing. `producer:` resolves exactly the same way and may likewise be
  overridden per call — a consumer still has to be running either way, since it
  owns the registry each subscription registers in.

  ## Options

    * `:repo` — the Ecto repo holding `dmex_cursors`.
    * `:storage` — the `DMEx.Storage` adapter; defaults to
      `DMEx.Storage.Postgres`.
    * `:prefix` — Postgres schema for the DMEx tables.
    * `:producer` — a map of producer name to stream URL.
    * `:handlers` — a list of handler modules, or `{module, keyword}` pairs
      where the keyword accepts everything `subscribe/2` does.
    * `:name` — names this consumer, so an application can run more than one.

  Per subscription: any `DMEx.Config` consumer setting (`:start`, `:on_failure`,
  `:batch_window`, `:max_batch_size`, …), plus the storage and producer keys
  above.

  ## Two handlers cannot share one identity

  `{producer, id}` keys the cursor, so two handlers declaring the same pair
  would clobber each other's position. `init/1` computes every configured
  handler's id before any child starts and raises naming both modules; the
  registry rejects a dynamic `subscribe/2` for an id already live with
  `{:error, {:already_subscribed, id}}`.

  ## v1 delivers whole logs

  There is **no filter option of any kind**. A subscription is the producer's
  entire log, and the types its handler matches on are what narrow it — locally,
  after the bytes have crossed the wire. A row whose type is not among them
  advances the cursor and invokes nothing, which is the normal case rather than
  a fault.
  """

  use Supervisor

  alias DMEx.Config
  alias DMEx.Consumer.Handler
  alias DMEx.Consumer.Stream, as: Subscription
  alias DMEx.Consumer.Supervisor, as: Subscriptions

  @default_name __MODULE__
  @storage_opts [:repo, :storage, :prefix]
  # `:handler` is deliberately absent: it is the positional argument now, so a
  # stray `handler:` in the options is a mistake worth raising on rather than
  # silently ignoring.
  @subscription_opts [:consumer, :producer]

  @producer_name ~r/\A[a-z][a-z0-9_]*\z/

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
    handlers = configured_handlers(opts)

    # Before any child starts, and loudly. It cannot live in `start_configured/2`
    # below: that runs inside a `Task` with `restart: :temporary`, so a raise
    # there would kill the Task silently and the supervisor would never notice.
    # Computing an id is a pure function of the declaration, so nothing has to
    # start for this to be answerable.
    reject_duplicate_ids!(handlers)

    children = [
      # The default storage and the producer map ride along as registry metadata,
      # seeded at start. That is why a bare subscribe/2 can find them with no
      # GenServer call, and why nothing has to write them afterwards.
      {Registry,
       keys: :unique,
       name: registry(name),
       meta: [storage: storage_config!(opts), producer: producer_map!(opts)]},
      {Subscriptions, name: subscriptions(name)},
      {Task, fn -> start_configured(name, handlers) end}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  # Runs once and exits; `Task`'s own child spec is `restart: :temporary`, which
  # is what a one-shot wants.
  defp start_configured(name, handlers) do
    Enum.each(handlers, fn {handler, opts} ->
      {:ok, _pid} = subscribe(handler, Keyword.put(opts, :consumer, name))
    end)
  end

  @doc """
  Starts the subscription `handler` declares.

  Everything about *what* is consumed comes off the module — the producer name,
  the subscription id and the message types. The options here are the ones a
  call site can legitimately vary: pacing (`DMEx.Config` consumer settings),
  where the cursor is written, and which consumer to attach to.

  Returns `{:error, {:already_subscribed, id}}` if that subscription id already
  has a live stream — one cursor per identity is the model, and two streams
  sharing one would clobber it.
  """
  @spec subscribe(module(), keyword()) ::
          {:ok, pid()} | {:error, {:already_subscribed, String.t()}} | {:error, term()}
  def subscribe(handler, opts \\ []) when is_atom(handler) and is_list(opts) do
    consumer = Keyword.get(opts, :consumer, @default_name)

    unless Handler.handler?(handler) do
      raise ArgumentError,
            "#{inspect(handler)} is not a DMEx handler. " <>
              "Add `use DMEx.Consumer.Handler` to it."
    end

    validate_keys!(opts)

    declaration = handler.__dmex__()

    child =
      [
        handler: handler,
        subscription: declaration.id,
        messages: declaration.messages,
        url: resolve_url!(declaration, opts, consumer),
        registry: registry(consumer),
        config: Config.consumer!(opts)
      ]
      |> Keyword.merge(Map.to_list(resolve_storage!(opts, consumer)))

    case DynamicSupervisor.start_child(subscriptions(consumer), {Subscription, child}) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_subscribed, id}} -> {:error, {:already_subscribed, id}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Stops the subscription `handler` declares."
  @spec unsubscribe(module(), keyword()) :: :ok | {:error, :not_found}
  def unsubscribe(handler, opts \\ []) when is_atom(handler) do
    consumer = Keyword.get(opts, :consumer, @default_name)

    case Registry.lookup(registry(consumer), {:subscription, Handler.subscription_id(handler)}) do
      [{pid, _}] -> DynamicSupervisor.terminate_child(subscriptions(consumer), pid)
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Resolves `repo:`, `storage:` and `prefix:` into an adapter and its options.

  The one place those three are read, so `subscribe/2`, `DMEx.Consumer.Cursors`
  and `DMEx.Consumer.DeadMessages` cannot drift on what they mean. `:repo` may
  be absent here — a consumer started with no default is legal, and
  `subscribe/2` is what insists on one.
  """
  @spec storage(keyword()) :: {module(), DMEx.Storage.opts()}
  def storage(opts) do
    {
      Keyword.get(opts, :storage, DMEx.Storage.Postgres),
      %{repo: Keyword.get(opts, :repo), prefix: Keyword.get(opts, :prefix)}
    }
  end

  @doc """
  Like `storage/1`, but insists on a `repo:`.

  What the operator-facing modules use, since there is no running consumer to
  fall back to when one is called from a migration or a console.
  """
  @spec storage!(keyword()) :: {module(), DMEx.Storage.opts()}
  def storage!(opts) do
    case storage(opts) do
      {_storage, %{repo: nil}} ->
        raise ArgumentError, "a `repo:` is required, e.g. `repo: MyApp.Repo`"

      resolved ->
        resolved
    end
  end

  @doc "The registry name for a consumer, where subscriptions register by id."
  @spec registry(atom()) :: atom()
  def registry(name \\ @default_name), do: Module.concat(name, "Registry")

  @doc "The dynamic supervisor name for a consumer."
  @spec subscriptions(atom()) :: atom()
  def subscriptions(name \\ @default_name), do: Module.concat(name, "Subscriptions")

  ## Configured handlers

  defp configured_handlers(opts) do
    opts
    |> Keyword.get(:handlers, [])
    |> Enum.map(fn
      {handler, handler_opts} when is_atom(handler) and is_list(handler_opts) ->
        {handler, handler_opts}

      handler when is_atom(handler) ->
        {handler, []}

      other ->
        raise ArgumentError,
              "a `handlers:` entry must be a handler module or a {module, keyword} pair, " <>
                "got: #{inspect(other)}"
    end)
  end

  # Names both modules, because "duplicate subscription id" without them is a
  # riddle in an application with thirty handlers.
  defp reject_duplicate_ids!(handlers) do
    _ =
      Enum.reduce(handlers, %{}, fn {handler, _opts}, seen ->
        id = Handler.subscription_id(handler)

        case seen do
          %{^id => already} ->
            raise ArgumentError,
                  "#{inspect(already)} and #{inspect(handler)} both declare the subscription " <>
                    "#{inspect(id)}. A subscription id keys the cursor, so two handlers " <>
                    "sharing one would clobber each other's position — give one of them a " <>
                    "different `id:`."

          _ ->
            Map.put(seen, id, handler)
        end
      end)

    :ok
  end

  ## Producer resolution

  defp resolve_url!(declaration, opts, consumer) do
    producers = resolve_producers!(opts, consumer)

    case Map.fetch(producers, declaration.producer) do
      {:ok, url} ->
        url

      :error ->
        raise ArgumentError, """
        #{inspect(declaration.handler)} declares `producer: #{inspect(declaration.producer)}`, \
        which is not in the producer map.

        Known producers: #{known(producers)}

        Add it where the consumer is started:

            {DMEx.Consumer,
             repo: MyApp.Repo,
             producer: %{#{declaration.producer}: "https://.../stream"},
             handlers: [...]}
        """
    end
  end

  defp known(producers) when map_size(producers) == 0, do: "(none)"
  defp known(producers), do: producers |> Map.keys() |> Enum.sort() |> Enum.join(", ")

  defp resolve_producers!(opts, consumer) do
    if Keyword.has_key?(opts, :producer) do
      producer_map!(opts)
    else
      # Same shape as the storage fallback below, and same reason for checking
      # the registry is alive first: Registry.meta/2 raises its own "unknown
      # registry" ArgumentError otherwise.
      with pid when is_pid(pid) <- Process.whereis(registry(consumer)),
           {:ok, producers} <- Registry.meta(registry(consumer), :producer) do
        producers
      else
        _ -> raise_no_producer(consumer)
      end
    end
  end

  # Names are validated here rather than at first connect, because a typo in a
  # map key and a typo in a handler's `producer:` are the same bug and only one
  # of them is visible in the "unknown producer" message.
  defp producer_map!(opts) do
    case Keyword.get(opts, :producer, %{}) do
      map when is_map(map) ->
        Map.new(map, fn {name, url} ->
          name = producer_name!(name)

          {name, producer_url!(name, url)}
        end)

      other ->
        raise ArgumentError,
              "`producer:` must be a map of name => URL, got: #{inspect(other)}"
    end
  end

  defp producer_name!(name) when is_atom(name) and not is_nil(name),
    do: producer_name!(Atom.to_string(name))

  defp producer_name!(name) when is_binary(name) do
    unless Regex.match?(@producer_name, name) do
      raise ArgumentError,
            "the producer name #{inspect(name)} is not a legal identifier: it must match " <>
              "#{inspect(@producer_name)}, the same charset a handler's `producer:` and " <>
              "`id:` use, so \"/\" can join them with no escaping."
    end

    name
  end

  defp producer_name!(other) do
    raise ArgumentError, "a producer name must be an atom or a string, got: #{inspect(other)}"
  end

  defp producer_url!(name, url) when is_binary(url) and url != "" do
    case URI.parse(url) do
      %URI{scheme: scheme, host: host} when scheme in ["http", "https"] and is_binary(host) ->
        if host == "" do
          raise_producer_url(name, url, "it has no host")
        else
          url
        end

      %URI{scheme: nil} ->
        raise_producer_url(name, url, "it has no scheme")

      %URI{scheme: scheme} ->
        raise_producer_url(name, url, "#{inspect(scheme)} is not http or https")
    end
  end

  defp producer_url!(name, url), do: raise_producer_url(name, url, "it is not a non-empty string")

  defp raise_producer_url(name, url, why) do
    raise ArgumentError,
          "the producer URL for #{inspect(name)} is not usable — #{why}: #{inspect(url)}. " <>
            "It must be an absolute http:// or https:// URL, e.g. " <>
            "\"https://orders.internal/stream\"."
  end

  ## Storage resolution

  defp resolve_storage!(opts, consumer) do
    if Keyword.has_key?(opts, :repo) do
      storage_config!(opts)
    else
      # Checking the registry is alive first: Registry.meta/2 raises its own
      # "unknown registry" ArgumentError otherwise, which would replace the one
      # message here that actually names the two fixes.
      with pid when is_pid(pid) <- Process.whereis(registry(consumer)),
           {:ok, %{storage_opts: %{repo: repo}} = config} when not is_nil(repo) <-
             Registry.meta(registry(consumer), :storage) do
        config
      else
        _ -> raise_no_repo(consumer)
      end
    end
  end

  defp storage_config!(opts) do
    {storage, storage_opts} = storage(opts)

    %{storage: storage, storage_opts: storage_opts}
  end

  defp raise_no_repo(consumer) do
    raise ArgumentError, """
    DMEx.Consumer.subscribe/2 has nowhere to write this subscription's cursor.

    Either pass a repo on the call:

        DMEx.Consumer.subscribe(MyApp.OrderProjection, repo: MyApp.Repo, producer: %{...})

    or start #{inspect(consumer)} in your supervision tree with a default:

        {DMEx.Consumer, repo: MyApp.Repo, producer: %{...}, handlers: [...]}
    """
  end

  defp raise_no_producer(consumer) do
    raise ArgumentError, """
    DMEx.Consumer.subscribe/2 has no producer map to resolve this handler's `producer:` against.

    Either pass one on the call:

        DMEx.Consumer.subscribe(MyApp.OrderProjection,
          repo: MyApp.Repo,
          producer: %{orders: "https://orders.internal/stream"}
        )

    or start #{inspect(consumer)} in your supervision tree with one:

        {DMEx.Consumer, repo: MyApp.Repo, producer: %{...}, handlers: [...]}
    """
  end

  defp validate_keys!(opts) do
    known = @subscription_opts ++ @storage_opts ++ Config.consumer_keys()

    case Enum.reject(Keyword.keys(opts), &(&1 in known)) do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "unknown subscription option(s) #{inspect(unknown)}. " <>
                "Known options: #{inspect(Enum.sort(known))}. " <>
                "Note there is no filter option: a v1 subscription is the " <>
                "producer's whole log, narrowed locally by the types the handler " <>
                "matches on. The producer, the id and the message types belong to " <>
                "the handler module, not here."
    end
  end
end
