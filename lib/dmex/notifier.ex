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
  that carries a routing key and no data; `DMEx.Log.Dispatcher` polls in
  parallel and that poll is what makes dispatch robust. A notifier that is down,
  slow, or `:unsupported` costs latency and never a stuck consumer.
  """

  @typedoc """
  Everything a notifier needs, built once by the log and passed to every call.

  `:log` names the notifier's processes and `:channel` is the log's own
  `dmex_<app>` topic. `:repo` is there for an engine that notifies *through* the
  database; one that does not simply ignores it.
  """
  @type opts :: %{
          required(:log) => module(),
          required(:channel) => String.t(),
          optional(:repo) => module(),
          optional(atom()) => term()
        }

  @doc """
  Returns the processes this notifier needs running alongside a log.

  Started by the log's supervisor, before `DMEx.Log.Dispatcher`. Postgres puts a
  `Postgrex.Notifications` connection here, since a `LISTEN` connection must
  stay checked out for its whole life and cannot be pooled; `DMEx.Notifier.Beam`
  puts a `:pg` scope.
  """
  @callback child_specs(opts()) :: [Supervisor.child_spec() | {module(), term()} | module()]

  @doc """
  Subscribes the calling process to this log's append notifications.

  Delivery is `{:notification, pid, reference(), channel :: String.t(), message_type ::
  String.t()}` — the shape `Postgrex.Notifications` sends, now this behaviour's
  contract rather than a driver detail leaking through.

  Returns the pid to monitor, so the dispatcher can re-subscribe if it dies.
  `:unsupported` is the honest answer for a mechanism that does not exist here;
  the dispatcher then runs polling-only *without* warning, because that is the
  design rather than a degradation.

  Best-effort by contract: a notifier must not raise or exit here.
  """
  @callback listen(opts()) :: {:ok, pid()} | :unsupported | {:error, term()}

  @doc """
  Announces one append, by message type.

  Called by `DMEx.Log.append/2` **after** the row is written, never inside the
  append's transaction: a notifier that fails must not turn a committed append
  into an error, so this returns `:ok` regardless. The lost notification costs
  one poll interval.

  The payload is the `message_type` and nothing else — a routing key, never a
  carrier of work. Every stream reads for itself.
  """
  @callback notify(opts(), message_type :: String.t()) :: :ok
end
