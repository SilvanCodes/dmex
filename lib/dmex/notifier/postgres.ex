defmodule DMEx.Notifier.Postgres do
  @moduledoc """
  `LISTEN`/`NOTIFY` over a dedicated `Postgrex.Notifications` connection.

  The default for `DMEx.Storage.Postgres`. A `LISTEN` connection must stay
  checked out for its whole life, so it cannot come from the repo's pool and
  gets a supervised connection of its own per log.

  `pg_notify/2` rather than `NOTIFY <channel>`, so neither a derived channel
  name nor a message type needs identifier quoting or can be an injection site.
  The payload is the message type, which `DMEx.Message`'s `[A-Za-z0-9_.]+` rule
  keeps well inside Postgres' 8000-byte notification cap in practice — and an
  over-long one costs a swallowed error and one poll interval, nothing more.

  **Not usable behind a transaction-pooling connection pooler.** `LISTEN` is
  session state and PgBouncer in transaction mode hands the session back after
  every statement. Use `DMEx.Notifier.Beam` there.
  """

  @behaviour DMEx.Notifier

  alias Ecto.Adapters.SQL

  @impl DMEx.Notifier
  def child_specs(opts) do
    name = name(opts)
    repo = Map.fetch!(opts, :repo)

    conn_opts =
      repo.config()
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
  def notify(opts, type) when is_binary(type) do
    repo = Map.fetch!(opts, :repo)
    channel = Map.fetch!(opts, :channel)

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

  defp name(opts), do: opts |> Map.fetch!(:log) |> Module.concat("Notifications")
end
