defmodule DMEx.Log do
  @moduledoc """
  Declares an application's message log and everything append-time about it.

      defmodule MyApp.Log do
        use DMEx.Log, repo: MyApp.Repo
      end

  A log module is a handle, not a process pool. It carries which storage adapter
  and repo an append goes through, and the app key that scopes both the append
  lock and the notification channel. Name it in `DMEx.Supervisor`'s `logs:` — an
  application may own several — and mount its endpoint:

      children = [
        MyApp.Repo,
        {DMEx.Supervisor, repo: MyApp.Repo, logs: [orders: MyApp.Log]}
      ]

      # in a Phoenix router, a Plug.Router, or a bare Plug pipeline
      forward "/stream", to: DMEx.Log.Plug, init_opts: [log: MyApp.Log]

      MyApp.Log.append(%MyApp.Messages.OrderPlaced{order_id: 7})
      #=> {:ok, 1042}

  Generic code holding a log module in a variable calls through the variable —
  `log.append(message)` — so there is no second form to keep in step.

  **Appending is just writing a row.** No RPC, no broker call, no network hop:
  the insert plus the notification that wakes live streams is the entire story.

    * The wire type comes from the message's **own module**
      (`c:DMEx.Message.type/0`), never from a call argument, so it cannot
      disagree with the struct it describes.
    * `metadata` passes through **untouched** unless this log's `metadata:` names
      columns to merge in, in which case those names — and only those — are
      reserved. See `DMEx.Log.Metadata`.
    * Nothing records which log appended the message. `message_type` is fully
      qualified, so it already identifies the appending application.

  ## Options

    * `:repo` — required; the Ecto repo owning `dmex_messages`.
    * `:storage` — the `DMEx.Storage` adapter. Defaults to
      `DMEx.Storage.Postgres`.
    * `:app` — the app key (see below). Defaults to a name derived from this
      module.
    * `:prefix` — a Postgres schema the DMEx tables live in.
    * any `DMEx.Config` log setting, including `metadata: [:id, :inserted_at]` —
      which of this log's own columns are merged into the metadata a consumer's
      handler receives. Empty by default; see `DMEx.Log.Metadata`.

  An unrecognised option raises at compile time rather than being ignored.

  Which application's modules are scanned for the message types this log can
  append is `otp_app:` on `DMEx.Supervisor` — see `DMEx.Log.Messages`.

  ## The app key

  One string does two jobs, both scoped to the *application* rather than to this
  module: it names the `dmex_<app>` notification channel and derives the
  advisory lock that serializes appends.

  It defaults to this module's own name, underscored and flattened — `MyApp.Log`
  becomes `my_app_log` — which needs no Mix at compile time, works inside
  umbrellas, and stays stable in a release. Pass `app: :orders` to choose your
  own; it must match `~r/\A[a-z][a-z0-9_]*\z/` and fit in 58 bytes, Postgres's
  63-byte identifier limit less the `dmex_` prefix.

  Two applications sharing one database must not share one `dmex_messages`
  table — the single-log model assumes one writer per table. Give them separate
  schemas with `:prefix` if they must be co-tenants.

  ## Why appends are serialized

  Postgres allocates sequence values at INSERT time but makes rows visible at
  COMMIT time, so two concurrent appends can commit out of allocation order. A
  reader scanning `id > cursor ORDER BY id` would advance straight over the row
  that had not committed yet and lose it permanently — silently, since
  `last_seen` moved right over the gap.

  So `append/2` takes a transaction-scoped advisory lock keyed on the app key.
  Appends to one log are effectively serial, in exchange for a reader model with
  no gap-detection logic in it.

  One consequence worth knowing before it bites: an `append/2` made *inside* an
  enclosing transaction extends that lock for the transaction's lifetime, so a
  long-running caller serializes every other append in the app.

  ## Telemetry

  `append/2` emits `[:dmex, :append, :start | :stop | :exception]` with
  `%{log: module, message_type: String.t()}` metadata.
  """

  alias DMEx.Config
  alias DMEx.Log.Messages

  @log_opts [:repo, :storage, :app, :prefix]
  @app_key_format ~r/\A[a-z][a-z0-9_]*\z/
  @app_key_max_bytes 58

  # A fixed namespace for every DMEx advisory lock, so DMEx's lock space is
  # disjoint from any other library sharing the database. Two ints rather than
  # one bigint follows Oban's convention for the same reason.
  @classid rem(:erlang.crc32("dmex"), 2_147_483_648)

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @dmex DMEx.Log.__config__(__MODULE__, opts)

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
      def child_spec(opts), do: DMEx.Log.child_spec(__MODULE__, opts)

      @doc """
      Starts this log's supervision subtree.

      Usually called for you by putting `MyApp.Log` in a supervision tree.
      """
      @spec start_link(keyword()) :: Supervisor.on_start()
      def start_link(opts \\ []), do: DMEx.Log.start_link(__MODULE__, opts)
    end
  end

  @typedoc "A log's compile-time resolved configuration."
  @type config :: %{
          log: module(),
          repo: module(),
          storage: module(),
          app_key: String.t(),
          channel: String.t(),
          lock: {integer(), integer()},
          prefix: String.t() | nil,
          config: Config.log(),
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
      log: module,
      repo: repo,
      storage: Keyword.get(opts, :storage, DMEx.Storage.Postgres),
      app_key: app_key,
      channel: channel,
      lock: lock,
      prefix: prefix,
      config: Config.log!(opts),
      storage_opts: %{
        repo: repo,
        log: module,
        lock: lock,
        channel: channel,
        prefix: prefix
      }
    }
  end

  @doc false
  @spec child_spec(module(), keyword()) :: Supervisor.child_spec()
  def child_spec(log, opts) do
    %{
      id: log,
      start: {__MODULE__, :start_link, [log, opts]},
      type: :supervisor
    }
  end

  @doc false
  @spec start_link(module(), keyword()) :: Supervisor.on_start()
  def start_link(log, opts \\ []) do
    %{storage: storage, storage_opts: storage_opts, config: config} = log.__dmex__()

    # Before any connection can ask for it. Writing a `:persistent_term` forces
    # a global GC scan, so it happens once here rather than per connect.
    Messages.refresh(log, Keyword.get(opts, :otp_app))

    # Whatever the adapter needs sits between the registry and the dispatcher,
    # and this module has no idea what it is. Postgres puts a LISTEN connection
    # here; an engine without notifications puts nothing.
    children =
      [DMEx.Log.Registry.child_spec(log)] ++
        storage.child_specs(storage_opts) ++
        [{DMEx.Log.Dispatcher, log: log}] ++
        watcher_children(log, config)

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: Keyword.get(opts, :name, Module.concat(log, "Supervisor"))
    )
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
  def __append__(log, message, opts) when is_atom(log) and is_struct(message) do
    validate_append_opts!(opts)

    %{storage: storage, storage_opts: storage_opts} = log.__dmex__()

    type = message_type!(message)
    data = JSON.encode!(message)
    metadata = opts |> Keyword.get(:metadata, %{}) |> JSON.encode!()

    :telemetry.span(
      [:dmex, :append],
      %{log: log, message_type: type},
      fn ->
        result = storage.append(storage_opts, type, data, metadata)

        {result, %{log: log, message_type: type}}
      end
    )
  end

  defp message_type!(%module{} = message) do
    if function_exported?(module, :type, 0) do
      module.type()
    else
      raise ArgumentError,
            "#{inspect(module)} is not a DMEx message module, so #{inspect(message)} " <>
              "cannot be appended. Add `use DMEx.Message` to #{inspect(module)}."
    end
  end

  defp validate_append_opts!(opts) do
    case Enum.reject(Keyword.keys(opts), &(&1 == :metadata)) do
      [] -> :ok
      unknown -> raise ArgumentError, "unknown append option(s): #{inspect(unknown)}"
    end
  end

  ## Compile-time validation

  defp validate_keys!(module, opts) do
    known = @log_opts ++ Config.log_keys()

    case Enum.reject(Keyword.keys(opts), &(&1 in known)) do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "unknown option#{if length(unknown) > 1, do: "s"} " <>
                "#{inspect(unknown)} passed to `use DMEx.Log` in #{inspect(module)}. " <>
                "Known options: #{inspect(Enum.sort(known))}"
    end
  end

  defp fetch_repo!(module, opts) do
    case Keyword.fetch(opts, :repo) do
      {:ok, repo} when is_atom(repo) and not is_nil(repo) ->
        repo

      _ ->
        raise ArgumentError,
              "`use DMEx.Log` requires a `:repo`, e.g. " <>
                "`use DMEx.Log, repo: MyApp.Repo` in #{inspect(module)}"
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

  # MyApp.Log -> "my_app/log" -> "my_app_log"
  defp derive_app_key(module) do
    module
    |> inspect()
    |> Macro.underscore()
    |> String.replace("/", "_")
  end
end
