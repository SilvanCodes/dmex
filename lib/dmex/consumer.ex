defmodule DMEx.Consumer do
  @moduledoc """
  Subscribes to producers' streams and keeps this application's read positions.

  Add it to your supervision tree with the subscriptions you want:

      {DMEx.Consumer,
       repo: MyApp.Repo,
       subscriptions: [
         [url: "https://orders.internal/stream",
          message_modules: [MyApp.Messages.OrderPlaced],
          handler: MyApp.OrderHandler]
       ]}

  or start one later:

      DMEx.Consumer.subscribe("https://orders.internal/stream",
        message_modules: [MyApp.Messages.OrderPlaced],
        handler: MyApp.OrderHandler
      )

  Both forms exist for the same reason `DMEx.Publisher.publish/3` and
  `MyApp.Producer.publish/2` both do: the declarative one is what an application
  writes, the imperative one is for code deciding at runtime.

  ## Where the producer URL comes from

  Static configuration of *this* application — app config, an environment
  variable, a literal. There is deliberately no discovery service in this path:
  that would reintroduce a shared, centrally-run piece of infrastructure
  everyone depends on, which is exactly what DMEx exists to avoid.

  ## Where the cursor is written

  A subscription needs somewhere to keep `last_seen`, and it is resolved in
  three layers, with no global configuration anywhere:

    1. `repo:` (and optionally `storage:` / `prefix:`) on this child spec sets
       the default for every subscription under it.
    2. Any subscription entry may override those for itself — for an
       application whose bookkeeping lives in a second repo.
    3. A bare `subscribe/2` with no `repo:` resolves from the running consumer
       named by `consumer:` (default `DMEx.Consumer`).

  With none of those, `subscribe/2` raises and names both fixes rather than
  guessing.

  ## Options

    * `:repo` — the Ecto repo holding `dmex_cursors`.
    * `:storage` — the `DMEx.Storage` adapter; defaults to
      `DMEx.Storage.Postgres`.
    * `:prefix` — Postgres schema for the DMEx tables.
    * `:subscriptions` — a list of keyword lists, each accepting everything
      `subscribe/2` does plus `url:`.
    * `:name` — names this consumer, so an application can run more than one.

  Per subscription: `:url`, `:handler`, `:message_modules`, plus any
  `DMEx.Config` consumer setting (`:start`, `:on_failure`, `:batch_window`,
  `:max_batch_size`, …).

  ## v1 delivers whole logs

  There is **no filter option of any kind**. A subscription is the producer's
  entire log, and `message_modules` is what narrows it — locally, after the
  bytes have crossed the wire. A row whose type is not in that list advances the
  cursor and invokes nothing, which is the normal case rather than a fault.
  """

  use Supervisor

  alias DMEx.Config
  alias DMEx.Consumer.Stream, as: Subscription
  alias DMEx.Consumer.Supervisor, as: Subscriptions

  @default_name __MODULE__
  @storage_opts [:repo, :storage, :prefix]
  @subscription_opts [:url, :handler, :message_modules, :consumer]

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
    children = [
      # The default storage rides along as registry metadata, seeded at start.
      # That is why a bare subscribe/2 can find it with no GenServer call, and
      # why nothing has to write it afterwards.
      {Registry, keys: :unique, name: registry(name), meta: [storage: storage_config!(opts)]},
      {Subscriptions, name: subscriptions(name)},
      {Task, fn -> start_configured(name, opts) end}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  # Runs once and exits; `Task`'s own child spec is `restart: :temporary`, which
  # is what a one-shot wants.
  defp start_configured(name, opts) do
    Enum.each(Keyword.get(opts, :subscriptions, []), fn subscription ->
      {url, subscription} = Keyword.pop!(subscription, :url)

      {:ok, _pid} = subscribe(url, Keyword.put(subscription, :consumer, name))
    end)
  end

  @doc """
  Starts a subscription to `url`.

  Returns `{:error, {:already_subscribed, handler}}` if that handler already has
  a live subscription — one cursor per handler is the model, and two streams
  sharing one would clobber it.
  """
  @spec subscribe(String.t(), keyword()) ::
          {:ok, pid()} | {:error, {:already_subscribed, module()}} | {:error, term()}
  def subscribe(url, opts) when is_binary(url) and is_list(opts) do
    consumer = Keyword.get(opts, :consumer, @default_name)

    validate_keys!(opts)

    child =
      opts
      |> Keyword.take(@subscription_opts)
      |> Keyword.merge(
        url: url,
        registry: registry(consumer),
        config: Config.consumer!(opts)
      )
      |> Keyword.merge(Map.to_list(resolve_storage!(opts, consumer)))

    case DynamicSupervisor.start_child(subscriptions(consumer), {Subscription, child}) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_subscribed, handler}} -> {:error, {:already_subscribed, handler}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Stops the subscription owned by `handler`."
  @spec unsubscribe(module(), keyword()) :: :ok | {:error, :not_found}
  def unsubscribe(handler, opts \\ []) when is_atom(handler) do
    consumer = Keyword.get(opts, :consumer, @default_name)

    case Registry.lookup(registry(consumer), {:handler, handler}) do
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

  @doc "The registry name for a consumer, where subscriptions register by handler."
  @spec registry(atom()) :: atom()
  def registry(name \\ @default_name), do: Module.concat(name, "Registry")

  @doc "The dynamic supervisor name for a consumer."
  @spec subscriptions(atom()) :: atom()
  def subscriptions(name \\ @default_name), do: Module.concat(name, "Subscriptions")

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

        DMEx.Consumer.subscribe(url, repo: MyApp.Repo, handler: ..., message_modules: [...])

    or start #{inspect(consumer)} in your supervision tree with a default:

        {DMEx.Consumer, repo: MyApp.Repo, subscriptions: [...]}
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
                "producer's whole log, narrowed locally by :message_modules."
    end
  end
end
