defmodule DMEx.Storage.Postgres do
  @moduledoc """
  The Postgres implementation of `DMEx.Storage`.

  Raw SQL through `Ecto.Adapters.SQL.query/4` rather than Ecto schemas. Two
  reasons, and the first is not negotiable: `data::text` cannot be expressed as
  a schema load, and the whole point of the read is that `jsonb` comes back as
  the text `DMEx.Frame` splices verbatim. The second is that there is no schema
  to reuse — DMEx owns three tables nothing else queries.

  ## No JSON library is ever required

  Parameters go in as **`$n::text::jsonb`** and `data`/`metadata` come back as
  `column::text`. Postgrex is therefore never asked to encode or decode `jsonb`,
  and DMEx never needs `config :postgrex, :json_library` — which matters,
  because DMEx depends on no JSON library at all and uses Elixir's built-in
  `JSON` module.

  **The double cast is load-bearing, and `$n::jsonb` is the trap.** Postgres
  infers a parameter's type from where it is used, so `$n::jsonb` makes it
  describe that parameter as `jsonb` — at which point Postgrex helpfully encodes
  the Elixir binary *through its own JSON codec*, storing the string
  `"{\\"order_id\\":7}"` rather than the object `{"order_id": 7}`. Going through
  `::text` first pins the parameter to `text`, so the bytes reach Postgres
  untouched and Postgres does the parse.

  That failure is silent — the insert succeeds, the row is valid `jsonb`, and
  only a consumer decoding the payload ever notices — so there is a test
  asserting `jsonb_typeof(data) = 'object'`.
  """

  @behaviour DMEx.Storage

  alias Ecto.Adapters.SQL

  @messages "dmex_messages"
  @cursors "dmex_cursors"
  @dead_messages "dmex_dead_messages"

  @impl DMEx.Storage
  def append(opts, type, data, metadata)
      when is_binary(type) and is_binary(data) and is_binary(metadata) do
    repo = Map.fetch!(opts, :repo)

    result =
      repo.transaction(fn ->
        # First, and released only by COMMIT. See DMEx.Storage's module
        # documentation for why anything shorter reintroduces silent id gaps.
        lock!(repo, opts)

        id = insert!(repo, opts, type, data, metadata)

        # Postgres delivers notifications at COMMIT, so a row is never announced
        # before it is visible. The payload is deliberately empty: a wakeup is a
        # hint that work may exist, never a carrier of work.
        notify!(repo, opts)

        id
      end)

    case result do
      {:ok, id} -> {:ok, id}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def read(opts, cursor, limit)
      when is_integer(cursor) and cursor >= 0 and is_integer(limit) and limit > 0 do
    repo = Map.fetch!(opts, :repo)
    table = table(opts, @messages)

    sql = """
    SELECT id, message_type, data::text, metadata::text, inserted_at,
           (SELECT max(id) FROM #{table}) AS max_id
      FROM #{table}
     WHERE id > $1
     ORDER BY id
     LIMIT $2
    """

    case SQL.query(repo, sql, [cursor, limit]) do
      {:ok, %{rows: []}} -> {:ok, {[], nil}}
      {:ok, %{rows: rows}} -> {:ok, {Enum.map(rows, &to_row/1), max_id_of(rows)}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def max_id(opts) do
    repo = Map.fetch!(opts, :repo)
    sql = "SELECT coalesce(max(id), 0) FROM #{table(opts, @messages)}"

    case SQL.query(repo, sql, []) do
      {:ok, %{rows: [[max_id]]}} -> {:ok, max_id}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def last_seen(opts, handler) when is_atom(handler) do
    repo = Map.fetch!(opts, :repo)
    sql = "SELECT last_seen FROM #{table(opts, @cursors)} WHERE handler = $1"

    case SQL.query(repo, sql, [inspect(handler)]) do
      {:ok, %{rows: [[last_seen]]}} -> {:ok, last_seen}
      {:ok, %{rows: []}} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def cursor(opts, handler) when is_atom(handler) do
    repo = Map.fetch!(opts, :repo)

    sql = """
    SELECT last_seen, producer_url, updated_at
      FROM #{table(opts, @cursors)}
     WHERE handler = $1
    """

    case SQL.query(repo, sql, [inspect(handler)]) do
      {:ok, %{rows: [[last_seen, url, updated_at]]}} ->
        {:ok, %{last_seen: last_seen, producer_url: url, updated_at: updated_at}}

      {:ok, %{rows: []}} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl DMEx.Storage
  def delete_cursor(opts, handler) when is_atom(handler) do
    repo = Map.fetch!(opts, :repo)
    sql = "DELETE FROM #{table(opts, @cursors)} WHERE handler = $1"

    case SQL.query(repo, sql, [inspect(handler)]) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def transaction(opts, fun) when is_function(fun, 0) do
    repo = Map.fetch!(opts, :repo)

    repo.transaction(fun)
  end

  @impl DMEx.Storage
  def advance(opts, handler, id, url) when is_atom(handler) and is_integer(id) do
    repo = Map.fetch!(opts, :repo)

    sql = """
    INSERT INTO #{table(opts, @cursors)} (handler, last_seen, producer_url, updated_at)
         VALUES ($1, $2, $3, now())
    ON CONFLICT (handler)
      DO UPDATE SET last_seen = EXCLUDED.last_seen,
                    producer_url = EXCLUDED.producer_url,
                    updated_at = now()
    """

    {:ok, _} = SQL.query(repo, sql, [inspect(handler), id, url])

    :ok
  end

  @impl DMEx.Storage
  def dead_message(opts, attrs) when is_map(attrs) do
    repo = Map.fetch!(opts, :repo)

    sql = """
    INSERT INTO #{table(opts, @dead_messages)}
           (handler, producer_url, id, message_type, data, metadata, inserted_at)
         VALUES ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, $7::text::timestamptz)
    """

    params = [
      inspect(Map.fetch!(attrs, :handler)),
      Map.fetch!(attrs, :producer_url),
      Map.fetch!(attrs, :id),
      Map.fetch!(attrs, :message_type),
      Map.fetch!(attrs, :data),
      Map.get(attrs, :metadata) || "{}",
      Map.fetch!(attrs, :inserted_at)
    ]

    case SQL.query(repo, sql, params) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def dead_messages(opts, handler) when is_atom(handler) do
    repo = Map.fetch!(opts, :repo)

    # The same projection `read/3` uses, because the columns are the same
    # columns — which is the whole point of the table's shape.
    sql = """
    SELECT id, message_type, data::text, metadata::text, inserted_at
      FROM #{table(opts, @dead_messages)}
     WHERE handler = $1
     ORDER BY id
    """

    case SQL.query(repo, sql, [inspect(handler)]) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, &to_row/1)}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def delete_dead_message(opts, handler, id) when is_atom(handler) and is_integer(id) do
    repo = Map.fetch!(opts, :repo)

    sql = "DELETE FROM #{table(opts, @dead_messages)} WHERE handler = $1 AND id = $2"

    case SQL.query(repo, sql, [inspect(handler), id]) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  ## Notifications

  @impl DMEx.Storage
  def child_specs(opts) do
    name = notifier(opts)
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
        :connect_timeout
      ])
      |> Keyword.merge(name: name, auto_reconnect: true)

    [Supervisor.child_spec({Postgrex.Notifications, conn_opts}, id: name)]
  end

  @impl DMEx.Storage
  def listen(opts) do
    with pid when is_pid(pid) <- Process.whereis(notifier(opts)),
         {:ok, _ref} <- safe_listen(pid, Map.fetch!(opts, :channel)) do
      {:ok, pid}
    else
      nil -> {:error, :not_running}
      other -> other
    end
  end

  # Postgrex.Notifications.listen/2 is a gen_statem call, so it *exits* when the
  # notifier is not running rather than returning an error tuple — and the
  # behaviour promises this is best-effort.
  defp safe_listen(pid, channel) do
    Postgrex.Notifications.listen(pid, channel)
  catch
    :exit, reason -> {:error, reason}
  end

  defp notifier(opts), do: opts |> Map.fetch!(:producer) |> Module.concat("Notifications")

  ## Append steps

  defp lock!(repo, opts) do
    {classid, objid} = Map.fetch!(opts, :lock)

    {:ok, _} =
      SQL.query(repo, "SELECT pg_advisory_xact_lock($1, $2)", [
        classid,
        objid
      ])
  end

  defp insert!(repo, opts, type, data, metadata) do
    sql = """
    INSERT INTO #{table(opts, @messages)} (message_type, data, metadata)
         VALUES ($1, $2::text::jsonb, $3::text::jsonb)
      RETURNING id
    """

    {:ok, %{rows: [[id]]}} = SQL.query(repo, sql, [type, data, metadata])

    id
  end

  # pg_notify/2 rather than `NOTIFY <channel>`, so a derived channel name needs
  # no identifier quoting and cannot be an injection site.
  defp notify!(repo, opts) do
    channel = Map.fetch!(opts, :channel)

    {:ok, _} = SQL.query(repo, "SELECT pg_notify($1, '')", [channel])
  end

  ## Row shaping

  defp to_row([id, message_type, data, metadata, inserted_at | _max_id]) do
    %{
      id: id,
      message_type: message_type,
      data: data,
      metadata: metadata,
      inserted_at: inserted_at
    }
  end

  # Every row carries the same correlated `max_id`, so the first one has it.
  defp max_id_of([first | _]), do: List.last(first)

  ## Naming

  defp table(opts, name) do
    case Map.get(opts, :prefix) do
      nil -> ~s("#{name}")
      prefix -> ~s("#{prefix}"."#{name}")
    end
  end
end
