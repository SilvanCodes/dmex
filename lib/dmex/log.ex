defmodule DMEx.Log do
  @moduledoc """
  Declares an application's message log and everything append-time about it.

      defmodule MyApp.Log do
        use DMEx.Log, repo: MyApp.Repo
      end

  A log module is a handle, not a process pool. It carries which storage adapter
  and repo an append goes through, and thereby which `dmex_messages` table. Name
  it in `DMEx.Supervisor`'s `logs:` — an application may own several — and mount
  its endpoint:

      children = [
        MyApp.Repo,
        {DMEx.Supervisor, repo: MyApp.Repo, logs: [orders: MyApp.Log]}
      ]

      # in a Plug.Router
      forward "/stream", to: DMEx.Log.Plug, log_module: MyApp.Log

      MyApp.Log.append(%MyApp.Messages.OrderPlaced{order_id: 7})
      #=> {:ok, 1042}

  Generic code holding a log module in a variable calls through the variable —
  `log.append(message)` — so there is no second form to keep in step.

  **Appending is just writing a row.** No RPC, no broker call, no network hop:
  the insert plus the notification that wakes live streams is the entire story.

    * The wire type comes from the message's **own module**
      (`c:DMEx.Message.type/0`), never from a call argument, so it cannot
      disagree with the struct it describes.
    * Nothing records which log appended the message. Whether a `type` says
      where it came from is the producer's choice of a namespaced one — see
      `DMEx.Message`, which is also where the collision that follows from
      declining is described.

  ## Options

    * `:repo` — the Ecto repo owning `dmex_messages`. Defaults to the
      application's `:ecto_repos` when that names exactly one, so a single-repo
      application omits it; required otherwise.
    * `:storage` — the `DMEx.Storage` adapter. Defaults to
      `DMEx.Storage.Postgres`.
    * `:notifier` — the `DMEx.Notifier`. Defaults to whatever the storage
      adapter names, which is `DMEx.Notifier.Postgres` for the Postgres one.
    * `:prefix` — a Postgres schema the DMEx tables live in, which is also what
      scopes the lock and the channel (see below). `DMEx.Storage.SQLite` raises
      on it, since SQLite has no schemas.
    * any `DMEx.Config` log setting.

  An unrecognised option raises at compile time rather than being ignored.

  Which applications' modules are scanned for the message types this log can
  append is `otp_app:` on `DMEx.Supervisor` — see `DMEx.Message.types/0`.

  ## What scopes the lock and the channel

  Both name one `dmex_messages` table, since that is what each is about: the
  lock buys id order for the rows in one table, and a dispatcher wants waking for
  appends to the one it reads. A table is identified by its database and its
  schema — the table names are fixed — and `LISTEN`/`NOTIFY` channels and
  advisory locks are already scoped to the database. So the schema is the whole
  of the remaining key: `:prefix`, or `public` when unset, names the
  `dmex_<schema>` channel and derives the lock. It has to fit in 58 bytes,
  Postgres's 63-byte identifier limit less the `dmex_` prefix.

  Keying on the log module instead would be unsound rather than merely
  different: two logs on one table would take different locks, fail to exclude
  each other, interleave id assignment with commit order, and every reader's
  `id > cursor` scan would step over whichever rows lost the race — permanently
  and silently. Deriving from the table means sharing follows the table and
  cannot be got wrong.

  What it costs is that logs sharing a table share a channel, so each
  dispatcher wakes for the others' appends and spends a read finding nothing it
  routes. Separate schemas with `:prefix` keep co-tenants apart.

  ## Why appends are serialized

  `append/2` requires that **id assignment order equals commit order**: every
  read path is an `id > cursor ORDER BY id` scan, which would advance straight
  over a lower id that had not committed yet and lose it permanently and
  silently.

  *How* that is bought is the storage adapter's, not this module's, and the two
  shipped ones differ. `DMEx.Storage` states both answers and what they mean for
  a caller appending inside its own transaction.

  ## Telemetry

  `append/2` emits a `[:dmex, :append, ...]` span — see `DMEx.Telemetry`.
  """

  alias DMEx.Config
  alias DMEx.Log.Streams
  alias DMEx.Notifier
  alias DMEx.Storage

  @log_opts [:repo, :storage, :notifier, :prefix]
  @scope_max_bytes 58

  # How long a start waits for a name held by a previous incarnation. Only ever
  # spent when the holder is not dying, i.e. when it is a genuine duplicate.
  @start_timeout 5_000

  # A fixed namespace for every DMEx advisory lock, so DMEx's lock space is
  # disjoint from any other library sharing the database — the objid within it
  # is the schema. Two ints rather than one bigint follows Oban's convention for
  # the same reason.
  @classid rem(:erlang.crc32("dmex"), 2_147_483_648)

  defmacro __using__(opts) do
    # `Application.compile_env/3` has to be expanded *here*, in the module doing
    # the `use`, for the compiler to record the read and recompile this log when
    # `:ecto_repos` changes. Reading it inside `__config__/3` would be an
    # untracked `get_env` against DMEx's own application instead.
    app = if Code.ensure_loaded?(Mix.Project), do: Mix.Project.config()[:app]
    ecto_repos = quote(do: Application.compile_env(unquote(app), :ecto_repos, []))

    quote bind_quoted: [opts: opts, ecto_repos: ecto_repos] do
      @dmex DMEx.Log.__config__(__MODULE__, opts, ecto_repos)

      @doc """
      This log's resolved configuration.

      Built at compile time, so the append path costs one function call rather
      than a keyword traversal.
      """
      @spec __dmex__() :: DMEx.Log.config()
      def __dmex__, do: @dmex

      @doc """
      Appends `message` to this log and returns its assigned id.

      ## Options

        * `:metadata` — a map stored alongside the message and delivered to the
          consumer's handler verbatim. Defaults to `%{}`.

      Raises `ArgumentError` if `message` is not a struct built with
      `use DMEx.Message`.
      """
      @spec append(struct(), keyword()) :: {:ok, pos_integer()} | {:error, term()}
      def append(message, opts \\ []), do: DMEx.Log.__append__(__MODULE__, message, opts)

      @doc false
      def child_spec(_opts), do: DMEx.Log.child_spec(__MODULE__)

      @doc """
      Starts this log's supervision subtree.

      Usually called for you by putting `MyApp.Log` in a supervision tree.
      """
      @spec start_link() :: Supervisor.on_start()
      def start_link, do: DMEx.Log.start_link(__MODULE__)
    end
  end

  @typedoc """
  A log's compile-time resolved configuration.

  Four keys, and `:repo`, `:prefix`, `:lock` and `:channel` are not among them:
  each belongs to exactly one of the two adapter handles, which carry their own
  adapter module — see `t:DMEx.Storage.opts/0`. A caller wanting the lock reads
  `config.storage.lock`, and there is no second copy to disagree with it.
  """
  @type config :: %{
          log: module(),
          config: Config.log(),
          storage: Storage.opts(),
          notifier: Notifier.opts()
        }

  @doc false
  @spec __config__(module(), keyword(), [module()]) :: config()
  def __config__(module, opts, ecto_repos \\ []) do
    DMEx.validate_keys!(
      opts,
      @log_opts ++ Config.log_keys(),
      "`use DMEx.Log` in #{inspect(module)}",
      " A log module is `use DMEx.Log, repo: MyApp.Repo`."
    )

    repo = repo!(module, Keyword.get_lazy(opts, :repo, fn -> default_repo!(module, ecto_repos) end))
    prefix = Keyword.get(opts, :prefix)
    storage = Keyword.get(opts, :storage, DMEx.Storage.Postgres)

    scope = scope!(module, prefix)
    channel = "dmex_" <> scope

    %{
      log: module,
      config: Config.log!(opts),
      storage: %{
        adapter: storage,
        repo: repo,
        log: module,
        lock: {@classid, :erlang.phash2(scope, 2_147_483_648)},
        prefix: prefix
      },
      notifier: %{
        adapter: notifier!(module, storage, opts),
        repo: repo,
        # The one derivation of the notifier's process name. Both adapters read
        # it rather than rebuilding it from `:log`, which is why `:log` is not
        # in this map: naming their processes was the only thing they used it
        # for. See `t:DMEx.Notifier.opts/0`.
        name: Module.concat(module, "Notifications"),
        channel: channel
      }
    }
  end

  @doc false
  @spec child_spec(module()) :: Supervisor.child_spec()
  def child_spec(log) do
    %{
      id: log,
      start: {__MODULE__, :start_link, [log]},
      type: :supervisor
    }
  end

  @doc false
  @spec start_link(module()) :: Supervisor.on_start()
  def start_link(log) do
    %{storage: storage, notifier: notifier, config: config} = log.__dmex__()

    # Here rather than at compile time, because an adapter may need to read the
    # repo's configuration — and the repo is started by now.
    :ok = Storage.validate!(storage)

    # Whatever the notifier needs sits between the registry and the dispatcher,
    # and this module has no idea what it is. Postgres puts a LISTEN connection
    # here; `DMEx.Notifier.Beam` puts a `:pg` scope.
    children =
      [Streams.child_spec(log)] ++
        Notifier.child_specs(notifier) ++
        [{DMEx.Log.Dispatcher, log: log}] ++
        watcher_children(log, config)

    # `:one_for_one` because no child here holds state derived from a sibling
    # that would outlive that sibling's restart, and the one cross-child
    # dependency is already covered:
    #
    #   * The dispatcher subscribes to the notifier, monitors it, and re-listens
    #     on its next poll — so a notifier restarting under it costs one poll
    #     interval of latency and nothing else. That is the same path a notifier
    #     that has not started yet takes.
    #   * A dying registry takes its streams with it: `Registry` links to every
    #     process it holds an entry for, so it cannot restart into a world of
    #     live connections registered nowhere. The clients reconnect. See
    #     `DMEx.Log.Streams`.
    #
    # `:rest_for_one` would therefore restart working children to rebuild
    # nothing, and it is `DMEx.Supervisor` — where the registry *does* own what
    # its siblings put in it — that needs the stronger strategy.
    #
    # Restart is also where a registered name can still be held by the previous
    # incarnation, which is what `__start__/1` waits out.
    __start__(fn ->
      Supervisor.start_link(children,
        strategy: :one_for_one,
        name: Module.concat(log, "Supervisor")
      )
    end)
  end

  @doc false
  # Restart, not first boot, is what needs this. A brutally killed subtree
  # dispatches exit signals to its children and to its own supervisor
  # concurrently, so the restart can begin while the processes it is about to
  # re-register are still queued behind their own exit signal — a registered
  # name outlives the signal that announced its owner's death. The start then
  # fails `{:already_started, pid}` and the supervisor retries just as fast, so
  # the restart intensity is spent before anything could have changed and the
  # whole tree above comes down with it.
  #
  # `{:already_started, pid}` names the process that is in the way, so this
  # waits on that process dying rather than on a duration. The deadline is only
  # for the case where the name is held by a genuine duplicate, which never dies.
  @spec __start__((-> Supervisor.on_start())) :: Supervisor.on_start()
  def __start__(start) when is_function(start, 0),
    do: await_start(start, System.monotonic_time(:millisecond) + @start_timeout)

  defp await_start(start, deadline) do
    case start.() do
      {:error, reason} = error -> retry_start(start, deadline, error, lingering(reason))
      started -> started
    end
  end

  defp retry_start(start, deadline, error, pid) when is_pid(pid) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining > 0 and down?(pid, remaining) do
      await_start(start, deadline)
    else
      error
    end
  end

  defp retry_start(_start, _deadline, error, nil), do: error

  # The name may be held by the child itself or by anything under it: a
  # `Registry` is a subtree, and its partition is the one that lingers.
  defp lingering({:already_started, pid}) when is_pid(pid), do: pid
  defp lingering({:shutdown, {:failed_to_start_child, _id, reason}}), do: lingering(reason)
  defp lingering(_reason), do: nil

  defp down?(pid, timeout) do
    ref = Process.monitor(pid)

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} -> true
    after
      timeout ->
        Process.demonitor(ref, [:flush])
        false
    end
  end

  # With no stall timeout there is no deadline to enforce, so the watcher is not
  # started at all rather than started and made a no-op — a process that exists
  # but never acts is the harder thing to reason about.
  defp watcher_children(_log, %{stall_timeout: :infinity}), do: []
  defp watcher_children(log, _config), do: [{DMEx.Log.Watcher, log: log}]

  ## Appending

  # The body of every `MyApp.Log.append/2`, kept here so `use DMEx.Log` injects
  # a one-line call and nothing else.
  @doc false
  @spec __append__(module(), struct(), keyword()) :: {:ok, pos_integer()} | {:error, term()}
  def __append__(log, message, opts) when is_atom(log) do
    validate_append_opts!(opts)

    %{storage: storage, notifier: notifier} = log.__dmex__()

    type = type!(message)
    data = JSON.encode!(message)
    metadata = opts |> Keyword.get(:metadata, %{}) |> JSON.encode!()

    :telemetry.span(
      [:dmex, :append],
      %{log: log, type: type},
      fn ->
        result = Storage.append(storage, type, data, metadata)

        # Here rather than inside the adapter's transaction, because which
        # mechanism announces an append is not the storage engine's business —
        # see `DMEx.Notifier`. The cost is that a caller appending inside its own
        # transaction announces a row that is not committed yet; the stream then
        # reads nothing and the dispatcher's poll picks it up.
        with {:ok, _id} <- result do
          Notifier.notify(notifier, type)
        end

        {result, %{log: log, type: type}}
      end
    )
  end

  # `DMEx.Message.message?/1` rather than `function_exported?(module, :type, 0)`:
  # exporting `type/0` is a shape a struct can have by coincidence, and one that
  # gets past this dies further in, inside `JSON.encode!/1`.
  defp type!(%module{} = message) do
    if DMEx.Message.message?(module) do
      module.type()
    else
      raise ArgumentError,
            "#{inspect(module)} is not a DMEx message module, so #{inspect(message)} " <>
              "cannot be appended. Add `use DMEx.Message` to #{inspect(module)}."
    end
  end

  # A non-struct gets the same `ArgumentError` the generated `append/2` promises,
  # rather than a `FunctionClauseError` from a guard.
  defp type!(other) do
    raise ArgumentError,
          "#{inspect(other)} is not a struct built with `use DMEx.Message`, " <>
            "so it cannot be appended."
  end

  defp validate_append_opts!(opts) do
    case Enum.reject(Keyword.keys(opts), &(&1 == :metadata)) do
      [] -> :ok
      unknown -> raise ArgumentError, "unknown append option(s): #{inspect(unknown)}"
    end
  end

  defp repo!(_module, repo) when is_atom(repo) and not is_nil(repo), do: repo

  defp repo!(module, other) do
    raise ArgumentError,
          "`:repo` in #{inspect(module)} must be an Ecto repo module, got: #{inspect(other)}. " <>
            "A log module is `use DMEx.Log, repo: MyApp.Repo`."
  end

  # The application's own `:ecto_repos` — what `mix ecto.migrate` reads — so the
  # single-repo application, which is nearly all of them, says nothing here. One
  # entry is an answer; none or several is not, and guessing either would bind
  # the log to a database silently.
  defp default_repo!(_module, [repo]), do: repo

  ## Compile-time validation

  defp default_repo!(module, repos) do
    raise ArgumentError,
          "`use DMEx.Log` in #{inspect(module)} requires :repo: the application's " <>
            "`:ecto_repos` is #{inspect(repos)}, which names no single repo to default to. " <>
            "A log module is `use DMEx.Log, repo: MyApp.Repo`."
  end

  # An adapter names the mechanism that fits it (`c:DMEx.Storage.default_notifier/0`),
  # and a log overrides that when its deployment says otherwise — a Postgres one
  # behind a transaction-pooling pooler cannot LISTEN. An adapter that declines to
  # choose forces the log to.
  defp notifier!(module, storage, opts) do
    with nil <- Keyword.get(opts, :notifier) do
      # `ensure_compiled/1` rather than `ensure_loaded?/1`: this runs while the
      # host's log module is compiling, and the adapter may be in the same
      # compilation — only this form waits for it.
      if match?({:module, _}, Code.ensure_compiled(storage)) and
           function_exported?(storage, :default_notifier, 0) do
        storage.default_notifier()
      else
        raise ArgumentError,
              "#{inspect(storage)} names no default notifier, so #{inspect(module)} must " <>
                "pass one, e.g. `use DMEx.Log, notifier: DMEx.Notifier.Beam`"
      end
    end
  end

  # An unset `:prefix` is the default schema, so it and an explicit "public"
  # name one table and have to land on one key. The charset is the storage
  # adapter's to police, and it does — Postgrex quotes the channel name, so
  # anything legal as a schema is legal here, and length is the only limit this
  # side adds.
  defp scope!(_module, nil), do: "public"

  defp scope!(module, prefix) when is_binary(prefix) do
    if byte_size(prefix) > @scope_max_bytes do
      raise ArgumentError,
            "`prefix: #{inspect(prefix)}` on #{inspect(module)} is #{byte_size(prefix)} " <>
              "bytes; the limit is #{@scope_max_bytes}, which is Postgres's 63-byte " <>
              "identifier limit less the \"dmex_\" channel prefix."
    end

    prefix
  end

  defp scope!(module, other) do
    raise ArgumentError,
          "`prefix:` on #{inspect(module)} must be a schema name as a string or nil, " <>
            "got: #{inspect(other)}"
  end
end
