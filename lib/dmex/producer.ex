defmodule DMEx.Producer do
  @moduledoc """
  Declares an application's message log and everything append-time about it.

      defmodule MyApp.Producer do
        use DMEx.Producer, repo: MyApp.Repo
      end

  A producer module is a handle, not a process pool. It carries which storage
  adapter and repo an append goes through, and the app key that scopes both the
  append lock and the notification channel. Add it to your supervision tree and
  mount its endpoint:

      children = [MyApp.Repo, MyApp.Producer]

      # in a Phoenix router, a Plug.Router, or a bare Plug pipeline
      forward "/stream", to: DMEx.Producer.Plug, init_opts: [producer: MyApp.Producer]

  Publishing is then an ordinary database write:

      MyApp.Producer.publish(%MyApp.Messages.OrderPlaced{order_id: 7})
      #=> {:ok, 1042}

  `MyApp.Producer.publish/2` is a delegate to `DMEx.Publisher.publish/3`, which
  is the same call for generic code holding a producer module in a variable.
  Neither records who published the message: `message_type` is the message
  module's fully-qualified name, so `MyApp.Messages.OrderPlaced` already says
  which application it came from, on every message, structurally.

  ## Options

    * `:repo` — required; the Ecto repo owning `dmex_messages`.
    * `:storage` — the `DMEx.Storage` adapter. Defaults to
      `DMEx.Storage.Postgres`.
    * `:app` — the app key (see below). Defaults to a name derived from this
      module.
    * `:prefix` — a Postgres schema the DMEx tables live in.
    * `:otp_app` — the application whose modules are scanned for message types,
      which is what a consumer's selection filter is resolved against. Defaults
      to the Mix application being compiled; see `DMEx.Producer.Messages`.

  Any `DMEx.Config` producer setting may also be given here — `batch_window:`,
  `max_batch_size:`, `heartbeat_ms:`, `poll_interval:` and the clamp bounds. An
  unrecognised option raises at compile time rather than being ignored.

  ## The app key

  One string does two jobs, and both are scoped to the *application*, not to
  this module: it names the `dmex_<app>` notification channel, and it derives
  the advisory lock that serializes appends.

  It defaults to this module's own name, underscored and flattened —
  `MyApp.Producer` becomes `my_app_producer` — because that needs no Mix at
  compile time, works inside umbrellas, and stays stable in a release. Pass
  `app: :orders` to choose your own; it must match `~r/[a-z][a-z0-9_]*/` and fit
  in 58 bytes, which is Postgres's 63-byte identifier limit less the `dmex_`
  prefix.

  Two producer applications sharing one database must not share one
  `dmex_messages` table — the single-log model assumes one writer per table.
  Give them separate schemas with `:prefix` if they must be co-tenants.

  ## Why appends are serialized

  Postgres allocates sequence values at INSERT time but makes rows visible at
  COMMIT time, so two concurrent publishes can commit out of allocation order.
  A reader scanning `id > cursor ORDER BY id` would then advance straight over
  the row that had not committed yet and lose it permanently — silently, since
  `last_seen` moved right over the gap.

  So `publish/2` takes a transaction-scoped advisory lock keyed on the app key,
  and ids commit in the order they were allocated. This costs publish
  concurrency — appends to one log are effectively serial — in exchange for a
  reader model with no gap-detection logic anywhere in it.

  One consequence worth knowing before it bites: a `publish/2` made *inside* an
  enclosing application transaction extends that lock to the whole transaction's
  lifetime, so a long-running caller serializes every other publish in the app
  for its duration.
  """

  alias DMEx.Config
  alias DMEx.Producer.Messages

  @producer_opts [:repo, :storage, :app, :prefix, :otp_app]
  @app_key_format ~r/\A[a-z][a-z0-9_]*\z/
  @app_key_max_bytes 58

  # A fixed namespace for every DMEx advisory lock, so DMEx's lock space is
  # disjoint from any other library sharing the database. Two ints rather than
  # one bigint follows Oban's convention for the same reason.
  @classid rem(:erlang.crc32("dmex"), 2_147_483_648)

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @dmex DMEx.Producer.__config__(__MODULE__, opts)

      @doc """
      This producer's resolved configuration.

      Built at compile time, so the publish path costs one function call rather
      than a keyword traversal.
      """
      @spec __dmex__() :: DMEx.Producer.config()
      def __dmex__, do: @dmex

      @doc """
      Publishes a message to this producer's log, returning its assigned id.

      Delegates to `DMEx.Publisher.publish/3`. Accepts `metadata: map()`.
      """
      @spec publish(struct(), keyword()) :: {:ok, pos_integer()} | {:error, term()}
      def publish(message, opts \\ []), do: DMEx.Publisher.publish(__MODULE__, message, opts)

      @doc false
      def child_spec(opts), do: DMEx.Producer.child_spec(__MODULE__, opts)

      @doc """
      Starts this producer's registry, notification listener, dispatcher and watcher.

      Usually called for you by putting `MyApp.Producer` in a supervision tree.
      """
      @spec start_link(keyword()) :: Supervisor.on_start()
      def start_link(opts \\ []), do: DMEx.Producer.start_link(__MODULE__, opts)
    end
  end

  @typedoc "A producer's compile-time resolved configuration."
  @type config :: %{
          producer: module(),
          repo: module(),
          storage: module(),
          otp_app: atom(),
          app_key: String.t(),
          channel: String.t(),
          lock: {integer(), integer()},
          prefix: String.t() | nil,
          config: Config.Producer.t(),
          storage_opts: DMEx.Storage.opts()
        }

  @doc false
  @spec __config__(module(), keyword()) :: config()
  def __config__(module, opts) do
    validate_keys!(module, opts)

    repo = fetch_repo!(module, opts)
    app_key = app_key!(module, opts)
    prefix = Keyword.get(opts, :prefix)

    channel = "dmex_" <> app_key
    lock = {@classid, :erlang.phash2(app_key, 2_147_483_648)}

    %{
      producer: module,
      repo: repo,
      storage: Keyword.get(opts, :storage, DMEx.Storage.Postgres),
      otp_app: otp_app!(module, opts),
      app_key: app_key,
      channel: channel,
      lock: lock,
      prefix: prefix,
      config: Config.producer!(opts),
      storage_opts: %{
        repo: repo,
        producer: module,
        lock: lock,
        channel: channel,
        prefix: prefix
      }
    }
  end

  @doc false
  @spec child_spec(module(), keyword()) :: Supervisor.child_spec()
  def child_spec(producer, opts) do
    %{
      id: producer,
      start: {__MODULE__, :start_link, [producer, opts]},
      type: :supervisor
    }
  end

  @doc false
  @spec start_link(module(), keyword()) :: Supervisor.on_start()
  def start_link(producer, opts \\ []) do
    %{storage: storage, storage_opts: storage_opts, config: config} = producer.__dmex__()

    # Before any connection can ask for it. Writing a `:persistent_term` forces
    # a global GC scan, so it happens once here rather than per connect — and
    # the set is immutable for the life of the node anyway, since the modules
    # compiled into a release do not change while it runs.
    Messages.refresh(producer)

    # Whatever the adapter needs sits between the registry and the dispatcher,
    # and this module has no idea what it is. Postgres puts a LISTEN connection
    # here; an engine without notifications puts nothing.
    children =
      [DMEx.Producer.Registry.child_spec(producer)] ++
        storage.child_specs(storage_opts) ++
        [{DMEx.Producer.Dispatcher, producer: producer}] ++
        watcher_children(producer, config)

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: Keyword.get(opts, :name, Module.concat(producer, "Supervisor"))
    )
  end

  # With no stall timeout there is no deadline to enforce, so the watcher is not
  # started at all rather than started and made a no-op — a process that exists
  # but never acts is the harder thing to reason about.
  defp watcher_children(_producer, %{stall_timeout: :infinity}), do: []
  defp watcher_children(producer, _config), do: [{DMEx.Producer.Watcher, producer: producer}]

  ## Compile-time validation

  defp validate_keys!(module, opts) do
    known = @producer_opts ++ Config.producer_keys()

    case Enum.reject(Keyword.keys(opts), &(&1 in known)) do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "unknown option#{if length(unknown) > 1, do: "s"} " <>
                "#{inspect(unknown)} passed to `use DMEx.Producer` in #{inspect(module)}. " <>
                "Known options: #{inspect(Enum.sort(known))}"
    end
  end

  defp fetch_repo!(module, opts) do
    case Keyword.fetch(opts, :repo) do
      {:ok, repo} when is_atom(repo) and not is_nil(repo) ->
        repo

      _ ->
        raise ArgumentError,
              "`use DMEx.Producer` requires a `:repo`, e.g. " <>
                "`use DMEx.Producer, repo: MyApp.Repo` in #{inspect(module)}"
    end
  end

  defp app_key!(module, opts) do
    key =
      case Keyword.get(opts, :app) do
        nil -> derive_app_key(module)
        app when is_atom(app) -> Atom.to_string(app)
        app when is_binary(app) -> app
        other -> raise ArgumentError, ":app must be an atom or a string, got: #{inspect(other)}"
      end

    unless Regex.match?(@app_key_format, key) do
      raise ArgumentError,
            "the app key #{inspect(key)} derived for #{inspect(module)} is not a legal " <>
              "identifier. It must match #{inspect(@app_key_format)} — pass `app: :your_key`."
    end

    if byte_size(key) > @app_key_max_bytes do
      raise ArgumentError,
            "the app key #{inspect(key)} derived for #{inspect(module)} is " <>
              "#{byte_size(key)} bytes; the limit is #{@app_key_max_bytes}, which is " <>
              "Postgres's 63-byte identifier limit less the \"dmex_\" channel prefix. " <>
              "Pass a shorter `app: :your_key`."
    end

    key
  end

  # Which OTP application this producer's message modules live in — the root of
  # the scan `DMEx.Producer.Messages` runs to answer "what can this producer
  # publish". Resolved from Mix at compile time, which is where the answer is
  # known: `:application.get_application/1` reads it off a *loaded* module, and
  # nothing is loaded while it is still being defined.
  defp otp_app!(module, opts) do
    case Keyword.get(opts, :otp_app) do
      nil -> derive_otp_app!(module)
      app when is_atom(app) and not is_nil(app) -> app
      other -> raise ArgumentError, ":otp_app must be an atom, got: #{inspect(other)}"
    end
  end

  defp derive_otp_app!(module) do
    with true <- Code.ensure_loaded?(Mix.Project),
         app when not is_nil(app) <- Mix.Project.config()[:app] do
      app
    else
      _ ->
        raise ArgumentError,
              "DMEx could not work out which OTP application #{inspect(module)} belongs to, " <>
                "which is what it scans for message modules. Pass it explicitly: " <>
                "`use DMEx.Producer, repo: MyApp.Repo, otp_app: :my_app`."
    end
  end

  # MyApp.Producer -> "my_app/producer" -> "my_app_producer"
  defp derive_app_key(module) do
    module
    |> inspect()
    |> Macro.underscore()
    |> String.replace("/", "_")
  end
end
