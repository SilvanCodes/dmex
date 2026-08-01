defmodule DMEx.Supervisor do
  @moduledoc """
  DMEx's entry point: this application's log, and every handler it runs.

      {DMEx.Supervisor,
       log: MyApp.Log,
       repo: MyApp.Repo,
       logs: [
         orders: "https://orders.internal/stream",
         billing: "https://billing.internal/stream"
       ]}

  Handlers are **not** listed. Every `use DMEx.Handler` module compiled into an
  application that depends on `:dmex` is discovered and started, and everything
  about a subscription — which log, under what id, which message types, how it
  handles failure — is declared on the handler module. All this needs to say is
  what each log name means.

  A discovered handler whose `log:` is not bound here stops the boot, naming it:
  a typo'd log name is otherwise a subscription that silently never exists.

  Code deciding at runtime can still start one by hand:

      DMEx.Supervisor.subscribe(MyApp.OrderProjection, on_failure: :skip)

  ## Options

    * `:log` — this application's own log module, started as the first child.
      Omit it in an application that only reads.
    * `:logs` — a keyword list of log name to stream URL.
    * `:repo` — the Ecto repo holding `dmex_cursors`.
    * `:storage` — the `DMEx.Storage` adapter; defaults to `DMEx.Storage.Postgres`.
    * `:prefix` — Postgres schema for the DMEx tables.
    * `:otp_app` — an extra application to scan for handlers. Needed only when
      the host application does not itself depend on `:dmex`; taken from `:log`
      otherwise.
    * `:name` — names this supervisor, so an application can run more than one.

  A URL is not verifiable against reality. Pointing an entry at the wrong host
  yields a subscription that looks perfectly healthy — connected, cursor
  advancing, every row skipped as an unknown type — while reading the wrong log.
  A selective subscription usually gets a `404` and says so; one declared
  `selective: false` gets a stream of skips, which is indistinguishable from a
  correct one on a quiet log.

  ## Where the cursor is written

  `repo:` (and optionally `storage:` / `prefix:`) here is the default for every
  subscription under it. A `subscribe/2` call may override it — for an
  application whose bookkeeping lives in a second repo — or resolve it from the
  running supervisor named by `supervisor:`. With neither, it raises and names
  both fixes. `logs:` resolves the same way.

  ## Two handlers cannot share one identity

  `{log, id}` keys the cursor, so two handlers declaring the same pair would
  clobber each other's position. That is checked before any child starts; the
  registry rejects a dynamic `subscribe/2` for an id already live with
  `{:error, {:already_subscribed, id}}`.
  """

  use Supervisor

  alias DMEx.Config
  alias DMEx.Handler
  alias DMEx.Handler.Stream, as: Subscription
  alias DMEx.Handler.Supervisor, as: Subscriptions

  @default_name __MODULE__
  @storage_opts [:repo, :storage, :prefix]
  # `:handler` is deliberately absent: it is the positional argument, so a stray
  # `handler:` in the options is a mistake worth raising on.
  @subscription_opts [:supervisor, :logs]

  @log_name ~r/\A[a-z][a-z0-9_]*\z/

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
    log = log!(opts)
    logs = logs!(opts)

    # Before any child starts, and loudly. It cannot live in `start_discovered/2`
    # below: that runs inside a `Task` with `restart: :temporary`, so a raise
    # there would kill the Task silently and the supervisor would never notice.
    handlers = __startable__!(Handler.discover(otp_app(opts, log)), logs)

    children =
      List.wrap(log) ++
        [
          # The default storage and the log map ride along as registry metadata,
          # seeded at start. That is why a bare subscribe/2 can find them with no
          # GenServer call, and why nothing has to write them afterwards.
          {Registry,
           keys: :unique, name: registry(name), meta: [storage: storage_config!(opts), logs: logs]},
          {Subscriptions, name: subscriptions(name)},
          {Task, fn -> start_discovered(name, handlers) end}
        ]

    # `:rest_for_one` because the registry owns what the subscriptions register
    # in. The log subtree sits in front of it and supervises its own children, so
    # its death is rare and taking the subscriptions with it costs a reconnect.
    Supervisor.init(children, strategy: :rest_for_one)
  end

  # Runs once and exits; `Task`'s own child spec is `restart: :temporary`, which
  # is what a one-shot wants.
  defp start_discovered(name, handlers) do
    Enum.each(handlers, fn handler ->
      {:ok, _pid} = subscribe(handler, supervisor: name)
    end)
  end

  defp log!(opts) do
    case Keyword.get(opts, :log) do
      nil ->
        nil

      log when is_atom(log) ->
        # `append/2` rather than `__dmex__/0`: a handler answers that one too.
        if Code.ensure_loaded?(log) and function_exported?(log, :append, 2) do
          log
        else
          raise_not_a_log(log)
        end

      other ->
        raise_not_a_log(other)
    end
  end

  defp raise_not_a_log(other) do
    raise ArgumentError,
          "`log:` must be a log module built with `use DMEx.Log`, got: #{inspect(other)}"
  end

  # The application to scan for handlers. Every loaded application depending on
  # `:dmex` is scanned anyway — this only adds one that does not, which is what
  # an umbrella parent or DMEx's own test suite needs.
  defp otp_app(opts, log) do
    cond do
      app = Keyword.get(opts, :otp_app) -> app
      log -> log.__dmex__().otp_app
      true -> nil
    end
  end

  @doc """
  Starts the subscription `handler` declares.

  Everything about *what* is consumed comes off the module, as do its defaults
  for how it runs. The options here override those for this one subscription,
  and say where the cursor is written and which supervisor to attach to.

  Returns `{:error, {:already_subscribed, id}}` if that subscription id already
  has a live stream — one cursor per identity is the model, and two streams
  sharing one would clobber it.
  """
  @spec subscribe(module(), keyword()) ::
          {:ok, pid()} | {:error, {:already_subscribed, String.t()}} | {:error, term()}
  def subscribe(handler, opts \\ []) when is_atom(handler) and is_list(opts) do
    supervisor = Keyword.get(opts, :supervisor, @default_name)

    unless Handler.handler?(handler), do: raise_not_a_handler(handler)

    validate_keys!(opts)

    declaration = handler.__dmex__()

    child =
      [
        handler: handler,
        subscription: declaration.id,
        fingerprint: declaration.fingerprint,
        url: resolve_url!(declaration, opts, supervisor),
        registry: registry(supervisor),
        config: Config.handler!(Keyword.merge(declaration.config, opts))
      ]
      |> Keyword.merge(Map.to_list(resolve_storage!(opts, supervisor)))

    case DynamicSupervisor.start_child(subscriptions(supervisor), {Subscription, child}) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_subscribed, id}} -> {:error, {:already_subscribed, id}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Stops the subscription `handler` declares."
  @spec unsubscribe(module(), keyword()) :: :ok | {:error, :not_found}
  def unsubscribe(handler, opts \\ []) when is_atom(handler) do
    supervisor = Keyword.get(opts, :supervisor, @default_name)

    case Registry.lookup(registry(supervisor), {:subscription, Handler.subscription_id(handler)}) do
      [{pid, _}] -> DynamicSupervisor.terminate_child(subscriptions(supervisor), pid)
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Resolves `repo:`, `storage:` and `prefix:` into an adapter and its options.

  The one place those three are read, so `subscribe/2`, `DMEx.Handler.Cursors`
  and `DMEx.Handler.DeadMessages` cannot drift on what they mean. `:repo` may be
  absent here — a supervisor started with no default is legal, and `subscribe/2`
  is what insists on one.
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

  What the operator-facing modules use, since there is nothing running to fall
  back to when one is called from a migration or a console.
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

  @doc "The registry name for a supervisor, where subscriptions register by id."
  @spec registry(atom()) :: atom()
  def registry(name \\ @default_name), do: Module.concat(name, "Registry")

  @doc "The dynamic supervisor name holding a supervisor's subscriptions."
  @spec subscriptions(atom()) :: atom()
  def subscriptions(name \\ @default_name), do: Module.concat(name, "Subscriptions")

  @doc """
  The discovered handlers that may be started against `logs`, or a raise.

  Both failures are pure functions of the declarations, so nothing has to be
  running for them to be answerable — which is why `init/1` asks before it
  starts anything.
  """
  @spec __startable__!([module()], %{String.t() => String.t()}) :: [module()]
  def __startable__!(handlers, logs) do
    reject_duplicate_ids!(handlers)
    reject_unbound_logs!(handlers, logs)

    handlers
  end

  # Names both modules, because "duplicate subscription id" without them is a
  # riddle in an application with thirty handlers.
  defp reject_duplicate_ids!(handlers) do
    _ =
      Enum.reduce(handlers, %{}, fn handler, seen ->
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

  defp reject_unbound_logs!(handlers, logs) do
    Enum.each(handlers, fn handler ->
      declaration = handler.__dmex__()

      unless Map.has_key?(logs, declaration.log), do: raise_unbound(declaration, logs)
    end)
  end

  ## Log resolution

  defp resolve_url!(declaration, opts, supervisor) do
    resolve_url!(declaration, resolve_logs!(opts, supervisor))
  end

  defp resolve_url!(declaration, logs) do
    case Map.fetch(logs, declaration.log) do
      {:ok, url} -> url
      :error -> raise_unbound(declaration, logs)
    end
  end

  defp raise_unbound(declaration, logs) do
    raise ArgumentError, """
    #{inspect(declaration.handler)} declares `log: #{inspect(declaration.log)}`, \
    which is not in `logs:`.

    Known logs: #{known(logs)}

    Add it where DMEx is started:

        {DMEx.Supervisor,
         repo: MyApp.Repo,
         logs: [#{declaration.log}: "https://.../stream"]}
    """
  end

  defp known(logs) when map_size(logs) == 0, do: "(none)"
  defp known(logs), do: logs |> Map.keys() |> Enum.sort() |> Enum.join(", ")

  defp resolve_logs!(opts, supervisor) do
    if Keyword.has_key?(opts, :logs) do
      logs!(opts)
    else
      # Checking the registry is alive first: Registry.meta/2 raises its own
      # "unknown registry" ArgumentError otherwise.
      with pid when is_pid(pid) <- Process.whereis(registry(supervisor)),
           {:ok, logs} <- Registry.meta(registry(supervisor), :logs) do
        logs
      else
        _ -> raise_no_logs(supervisor)
      end
    end
  end

  # Names are validated where they are declared rather than at first connect,
  # because a typo in a key and a typo in a handler's `log:` are the same bug and
  # only one of them is visible in the "unknown log" message.
  defp logs!(opts) do
    case Keyword.get(opts, :logs, []) do
      logs when is_list(logs) or is_map(logs) ->
        Map.new(logs, fn
          {name, url} ->
            name = log_name!(name)

            {name, log_url!(name, url)}

          other ->
            raise ArgumentError, "a `logs:` entry must be `name: url`, got: #{inspect(other)}"
        end)

      other ->
        raise ArgumentError,
              "`logs:` must be a keyword list of name to URL, got: #{inspect(other)}"
    end
  end

  defp log_name!(name) when is_atom(name) and not is_nil(name),
    do: log_name!(Atom.to_string(name))

  defp log_name!(name) when is_binary(name) do
    unless Regex.match?(@log_name, name) do
      raise ArgumentError,
            "the log name #{inspect(name)} is not a legal identifier: it must match " <>
              "#{inspect(@log_name)}, the same charset a handler's `log:` and `id:` use, " <>
              "so \"/\" can join them with no escaping."
    end

    name
  end

  defp log_name!(other) do
    raise ArgumentError, "a log name must be an atom or a string, got: #{inspect(other)}"
  end

  defp log_url!(name, url) when is_binary(url) and url != "" do
    case URI.parse(url) do
      %URI{scheme: scheme, host: host} when scheme in ["http", "https"] and is_binary(host) ->
        if host == "" do
          raise_log_url(name, url, "it has no host")
        else
          url
        end

      %URI{scheme: nil} ->
        raise_log_url(name, url, "it has no scheme")

      %URI{scheme: scheme} ->
        raise_log_url(name, url, "#{inspect(scheme)} is not http or https")
    end
  end

  defp log_url!(name, url), do: raise_log_url(name, url, "it is not a non-empty string")

  defp raise_log_url(name, url, why) do
    raise ArgumentError,
          "the URL for #{inspect(name)} is not usable — #{why}: #{inspect(url)}. " <>
            "It must be an absolute http:// or https:// URL, e.g. " <>
            "\"https://orders.internal/stream\"."
  end

  ## Storage resolution

  defp resolve_storage!(opts, supervisor) do
    if Keyword.has_key?(opts, :repo) do
      storage_config!(opts)
    else
      with pid when is_pid(pid) <- Process.whereis(registry(supervisor)),
           {:ok, %{storage_opts: %{repo: repo}} = config} when not is_nil(repo) <-
             Registry.meta(registry(supervisor), :storage) do
        config
      else
        _ -> raise_no_repo(supervisor)
      end
    end
  end

  defp storage_config!(opts) do
    {storage, storage_opts} = storage(opts)

    %{storage: storage, storage_opts: storage_opts}
  end

  defp raise_not_a_handler(module) do
    raise ArgumentError,
          "#{inspect(module)} is not a DMEx handler. Add `use DMEx.Handler` to it."
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

  defp raise_no_logs(supervisor) do
    raise ArgumentError, """
    DMEx.Supervisor.subscribe/2 has no `logs:` to resolve this handler's `log:` against.

    Either pass them on the call:

        DMEx.Supervisor.subscribe(MyApp.OrderProjection,
          repo: MyApp.Repo,
          logs: [orders: "https://orders.internal/stream"]
        )

    or start #{inspect(supervisor)} in your supervision tree with them:

        {DMEx.Supervisor, repo: MyApp.Repo, logs: [...]}
    """
  end

  defp validate_keys!(opts) do
    known = @subscription_opts ++ @storage_opts ++ Config.handler_keys()

    case Enum.reject(Keyword.keys(opts), &(&1 in known)) do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "unknown subscription option(s) #{inspect(unknown)}. " <>
                "Known options: #{inspect(Enum.sort(known))}. " <>
                "Note there is no filter or selection option here: a subscription selects " <>
                "the message types its handler's clauses match on, encoded at compile time " <>
                "and sent in the request path. The log, the id, the message types and " <>
                "`selective:` all belong to the handler module, not here."
    end
  end
end
