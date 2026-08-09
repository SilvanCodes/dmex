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

    # Hand-maintained, because Postgrex exports no list of its own connect
    # options — there is no `Postgrex.start_option/0` to derive this from, so
    # nothing links it to the driver at compile time and it can only drift.
    # Taking rather than dropping is still the right direction: `repo.config()`
    # carries Ecto's settings too (`:pool`, `:telemetry_prefix`), and handing
    # those to Postgrex is a connection that fails to start.
    #
    # The failure mode of a missing key is quiet, which is why this is worth a
    # comment: a 13th key a user sets — an `:ssl_opts` variant, say — reaches the
    # repo's pool and not this connection, so the notifier works in dev against a
    # plaintext database and silently falls back to polling in production, where
    # `DMEx.Log.Dispatcher` covers it at one poll interval of latency and logs a
    # warning once. Add the key here when that happens.
    conn_opts =
      Map.fetch!(opts, :repo).config()
      |> Keyword.take([
        :hostname,
        :port,
        :username,
        :password,
        :database,
        :socket,
        :socket_dir,
        :ssl,
        :ssl_opts,
        :parameters,
        :connect_timeout,
        :socket_options
      ])
      |> Keyword.merge(name: name, auto_reconnect: true)

    [Supervisor.child_spec({Postgrex.Notifications, conn_opts}, id: name)]
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
