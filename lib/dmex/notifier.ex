defmodule DMEx.Notifier do
  @moduledoc """
  How a live stream learns that something was appended.

  Separate from `DMEx.Storage` because the two answer different questions and a
  database that is good at one need not be good at the other. Postgres has
  `LISTEN`/`NOTIFY`; SQLite has nothing; a Postgres deployment behind a
  transaction-pooling PgBouncer *has* Postgres and still cannot use it. Welding
  notification into the storage adapter would make every one of those a
  different adapter.

  So a log names its storage and its notifier independently:

      use DMEx.Log, repo: MyApp.Repo, notifier: DMEx.Notifier.Beam

  Left out, the storage adapter picks — `c:DMEx.Storage.default_notifier/0`.

  **Nothing here is load-bearing for correctness.** A notification is a hint
  that carries a message type and no data; `DMEx.Log.Dispatcher` polls in
  parallel and that poll is what makes dispatch robust. A notifier that is down,
  slow, or `:unsupported` costs latency and never a stuck consumer.
  """

  @typedoc """
  Everything a notifier needs, built once by the log and passed to every call.

  `:name` is the registered name for whatever `c:child_specs/1` starts, which is
  what `c:listen/1` then looks up — derived once by the log so no adapter
  re-derives it, and unique per log because it is built from the log module.
  `:channel` is the log's own `dmex_<app>` topic. `:repo` is there for an engine
  that notifies *through* the database; one that does not simply ignores it.
  """
  @type opts :: %{
          required(:name) => atom(),
          required(:channel) => String.t(),
          optional(:repo) => module(),
          optional(atom()) => term()
        }

  @doc """
  Returns the processes this notifier needs running alongside a log.

  Started by the log's supervisor, before `DMEx.Log.Dispatcher`, and registered
  under `opts.name`. Postgres puts a `Postgrex.Notifications` connection here,
  since a `LISTEN` connection must stay checked out for its whole life and
  cannot be pooled; `DMEx.Notifier.Beam` puts a `:pg` scope.
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

  There is no `unlisten`, and that is a decision rather than an omission: one
  dispatcher subscribes per log and lives as long as the log's supervision tree,
  so a subscription only ever ends with its subscriber — which `:pg` and
  `Postgrex.Notifications` both already clean up. A second subscriber per log
  would need one.

  Returns the pid to monitor, so the dispatcher can re-subscribe if it dies.
  `:unsupported` is the honest answer for a mechanism that does not exist here;
  the dispatcher then runs polling-only *without* warning, because that is the
  design rather than a degradation.

  Best-effort by contract: a notifier must not raise or exit here.
  """
  @callback listen(opts()) :: {:ok, pid()} | :unsupported | {:error, term()}

  @doc """
  Announces one append, by message type.

  Called by a log module's `append/2` **after** the row is written, never inside the
  append's transaction: a notifier that fails must not turn a committed append
  into an error, so this returns `:ok` regardless. The lost notification costs
  one poll interval.

  The payload is the `type` and nothing else — what the *dispatcher*
  routes on, never a carrier of work. No notifier routes on it: `:channel` is
  the only dimension a notifier selects by, and every stream reads for itself.

  **Cost is per append and differs by an order of magnitude between adapters.**
  `append/2` calls this once per message, so this is the append path's own cost.
  `DMEx.Notifier.Beam` is an ETS lookup plus one local send per subscriber;
  `DMEx.Notifier.Postgres` is a pool checkout and a database round trip. Neither
  is wrong — the round trip is what buys reach past this cluster — but on a hot
  append path the difference is the thing to choose on.
  """
  @callback notify(opts(), type :: String.t()) :: :ok
end
