defmodule DMEx.Notifier.Postgres do
  @moduledoc """
  `LISTEN`/`NOTIFY` over a dedicated `Postgrex.Notifications` connection.

  The default for `DMEx.Storage.Postgres`. One supervised connection per log,
  outside the repo's pool — `c:DMEx.Notifier.child_specs/1` covers why.

  `pg_notify/2` rather than `NOTIFY <channel>`, so neither a derived channel
  name nor a message type needs identifier quoting or can be an injection site.
  The payload is the message type, which `DMEx.Message` keeps to dot-separated
  identifier segments — well inside Postgres' 8000-byte notification cap in
  practice, and an over-long one costs a swallowed error and one poll interval.

  **Not usable behind a transaction-pooling connection pooler.** `LISTEN` is
  session state and PgBouncer in transaction mode hands the session back after
  every statement. Use `DMEx.Notifier.Beam` there.
  """

  @behaviour DMEx.Notifier

  alias Ecto.Adapters.SQL

  @impl DMEx.Notifier
  def child_specs(opts) do
    name = name(opts)

    conn_opts =
      Map.fetch!(opts, :repo).config()
      |> Enum.reject(&ecto_only?/1)
      |> Keyword.merge(name: name, auto_reconnect: true)

    [Supervisor.child_spec({Postgrex.Notifications, conn_opts}, id: name)]
  end

  # Neither direction can be derived — Postgrex exports no list of its own
  # connect options, and Ecto none of its repo ones — so one of the two lists
  # has to be written out here and will drift. Which one is a choice between
  # failure modes, and they are not comparable.
  #
  # Keeping only what Postgrex takes drifts *quietly*: a `:ssl_opts` variant a
  # user sets reaches the repo's pool and not this connection, so the notifier
  # works in dev against a plaintext database and in production silently falls
  # back to polling — `DMEx.Log.Dispatcher` covers it at one poll interval and
  # one warning, which is exactly quiet enough to survive a deploy.
  #
  # Rejecting what Ecto owns drifts *loudly*: an unrecognised key reaches
  # `Postgrex.Notifications`, the connection refuses to start, and the log's
  # tree fails at boot with the option named. It also drifts less often — this
  # list is one library's documented repo options, closed and slow-moving, where
  # Postgrex's connect options grow with every authentication and TLS feature.
  # So a new driver option works here the day it exists, and a new Ecto option
  # is a boot failure rather than a production degradation.
  #
  # `:scheme` and `:url` are URL-parsing residue rather than settings — Ecto has
  # already exploded them into host, port, credentials and database.
  defp ecto_only?({key, _value}) do
    key in [
      :adapter,
      :caller,
      :default_dynamic_repo,
      :log,
      :loggers,
      :name,
      :otp_app,
      :ownership_timeout,
      :pool,
      :pool_size,
      :priv,
      :queue_interval,
      :queue_target,
      :read_only,
      :repo,
      :scheme,
      :show_sensitive_data_on_connection_error,
      :stacktrace,
      :start_apps_before_migration,
      :telemetry_prefix,
      :url
    ] or String.starts_with?(Atom.to_string(key), "migration_")
  end

  @impl DMEx.Notifier
  def listen(opts) do
    # `{:eventually, ref}` is a success, not a degradation: `auto_reconnect:
    # true` means Postgrex records the subscription and re-issues every `LISTEN`
    # when the connection comes back. Answering it as anything else would take
    # the dispatcher down every time a log boots ahead of its database.
    #
    # The ref is dropped because there is nothing to hand it to: the behaviour
    # has no `unlisten` and deliberately so — see `c:DMEx.Notifier.listen/1`.
    with pid when is_pid(pid) <- Process.whereis(name(opts)),
         {tag, _ref} when tag in [:ok, :eventually] <-
           safe_listen(pid, Map.fetch!(opts, :channel)) do
      {:ok, pid}
    else
      nil -> {:error, :not_running}
      other -> other
    end
  end

  @impl DMEx.Notifier
  def notify(type, %{repo: repo, channel: channel}) when is_binary(type) do
    # The append has already committed, so a failed hint is a latency cost and
    # nothing more — see `c:DMEx.Notifier.notify/2`.
    _ = SQL.query(repo, "SELECT pg_notify($1, $2)", [channel, type])

    :ok
  end

  # Postgrex.Notifications.listen/2 is a gen_statem call, so it *exits* when the
  # notifier is not running rather than returning an error tuple — and the
  # behaviour promises this is best-effort.
  defp safe_listen(pid, channel) do
    Postgrex.Notifications.listen(pid, channel)
  catch
    :exit, reason -> {:error, reason}
  end

  # Derived by the log, not here — see `t:DMEx.Notifier.opts/0`.
  defp name(opts), do: Map.fetch!(opts, :name)
end
