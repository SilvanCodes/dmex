defmodule DMEx.Notifier do
  @moduledoc """
  How a live stream learns that something was appended.

  Separate from `DMEx.Storage` because the two answer different questions: Postgres
  has `LISTEN`/`NOTIFY`, SQLite has nothing, and a Postgres deployment behind a
  transaction-pooling PgBouncer *has* Postgres and still cannot use it. Welding
  notification into the storage adapter would make every one of those a different
  adapter.

  So a log names its storage and its notifier independently:

      use DMEx.Log, repo: MyApp.Repo, notifier: DMEx.Notifier.Beam

  Left out, the storage adapter picks — `c:DMEx.Storage.default_notifier/0`.

  **Nothing here is load-bearing for correctness.** A notification is a hint
  carrying a message type and no data, and `DMEx.Log.Dispatcher`'s poll is what
  makes dispatch robust: a notifier that is down or slow costs latency and never
  a stuck consumer.
  """

  @typedoc """
  A notifier and everything it needs, built once by the log and passed to every
  call.

  `:adapter` is the module implementing this behaviour, so this map is the whole
  handle rather than half of a `{module, opts}` pair. `:name` is the registered
  name `c:child_specs/1` starts under and `c:listen/1` looks up, derived once by
  the log and unique per log. `:channel` is the `dmex_<schema>` topic, which
  co-tenants on one table share — see `DMEx.Log`. `:repo` is for an engine that
  notifies *through* the database; one that does not simply ignores it.
  """
  @type opts :: %{
          required(:adapter) => module(),
          required(:name) => atom(),
          required(:channel) => String.t(),
          optional(:repo) => module(),
          optional(atom()) => term()
        }

  @doc """
  Returns the processes this notifier needs running alongside a log.

  Started by the log's supervisor, before `DMEx.Log.Dispatcher`, and registered
  under `opts.name`. `DMEx.Notifier.Postgres` puts its dedicated
  `Postgrex.Notifications` connection here; `DMEx.Notifier.Beam` a `:pg` scope.
  """
  @callback child_specs(opts()) :: [Supervisor.child_spec() | {module(), term()} | module()]

  @doc """
  Subscribes the calling process to this log's append notifications.

  Delivery is `{:notification, pid, reference(), channel :: String.t(), type ::
  String.t()}` — the shape `Postgrex.Notifications` sends, of which
  `DMEx.Log.Dispatcher` reads the last element and nothing else.

  It is Postgrex's shape because Postgrex sends **directly to the listening
  process**. Normalising it away would take a rewriting proxy process per log,
  paid for with a process and an extra hop on the wakeup path, to save four
  ignored elements. So the shape is this behaviour's contract and an adapter
  sending its own messages fills the four in — see `DMEx.Notifier.Beam.notify/2`.

  There is no `unlisten` by decision: one dispatcher subscribes per log and lives
  as long as the log's supervision tree, so a subscription ends with its
  subscriber — which `:pg` and `Postgrex.Notifications` both clean up. A second
  subscriber per log would need one.

  Returns the pid to monitor.

  Best-effort by contract: a notifier must not raise or exit here.
  """
  @callback listen(opts()) :: {:ok, pid()} | {:error, term()}

  @doc """
  Announces one append, by message type.

  Called by a log module's `append/2` **after** the row is written, never inside
  the append's transaction: a notifier that fails must not turn a committed
  append into an error, so this returns `:ok` regardless. The lost notification
  costs one poll interval.

  The payload is the `type` and nothing else — what the *dispatcher* routes on,
  never a carrier of work; `:channel` is the only dimension a notifier selects by.

  Cost is per append and differs by an order of magnitude between adapters:
  `DMEx.Notifier.Beam` is a local send, `DMEx.Notifier.Postgres` a pool checkout
  and a database round trip.
  """
  @callback notify(type :: String.t(), opts()) :: :ok

  ## Calling a notifier

  # As in `DMEx.Storage`: the map first, the adapter it names last.

  @doc "The processes this notifier needs running. See `c:child_specs/1`."
  @spec child_specs(opts()) :: [Supervisor.child_spec() | {module(), term()} | module()]
  def child_specs(%{adapter: adapter} = notifier), do: adapter.child_specs(notifier)

  @doc "Subscribes the calling process to this log's appends. See `c:listen/1`."
  @spec listen(opts()) :: {:ok, pid()} | {:error, term()}
  def listen(%{adapter: adapter} = notifier), do: adapter.listen(notifier)

  @doc "Announces an append. See `c:notify/2`."
  @spec notify(opts(), String.t()) :: :ok
  def notify(%{adapter: adapter} = notifier, type), do: adapter.notify(type, notifier)
end
