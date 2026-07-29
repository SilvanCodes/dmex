defmodule DMEx.Storage.Postgres do
  @moduledoc """
  The Postgres implementation of `DMEx.Storage`.

  Raw SQL through `Ecto.Adapters.SQL.query/4` rather than Ecto schemas. Two
  reasons, and the first is not negotiable: `data::text` cannot be expressed as
  a schema load, and the whole point of the read is that `jsonb` comes back as
  the text `DMEx.SSE.Frame` splices verbatim. The second is that there is no schema
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
        # before it is visible. The payload is the `message_type` and nothing
        # else: a routing key, never a carrier of work — no id, no range, no
        # row, because each stream reads from its own cursor to whatever is
        # there now and would consume none of it.
        notify!(repo, opts, type)

        id
      end)

    case result do
      {:ok, id} -> {:ok, id}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def read(opts, cursor, limit, types \\ :all)

  def read(opts, cursor, limit, :all)
      when is_integer(cursor) and cursor >= 0 and is_integer(limit) and limit > 0 do
    query(opts, read_sql(opts, ""), [cursor, limit])
  end

  def read(opts, cursor, limit, types)
      when is_integer(cursor) and cursor >= 0 and is_integer(limit) and limit > 0 and
             is_list(types) do
    # A separate statement rather than `($3 IS NULL OR message_type = ANY($3))`
    # on the one above. The nullable form is a correct query and a ruinous plan:
    # Postgres cannot use either index through it, so the unfiltered read would
    # regress to pay for a predicate it never uses.
    query(opts, read_sql(opts, "AND message_type = ANY($3)"), [cursor, limit, types])
  end

  # `max_id` is the tip of the *whole* log even on a filtered read, and that is
  # load-bearing rather than an oversight: it is what keeps a selective
  # subscription's cursor denominated in the same id space as every other
  # consumer's, so a sparse subset never becomes a space of its own.
  defp read_sql(opts, predicate) do
    table = table(opts, @messages)

    """
    SELECT id, message_type, data::text, metadata::text, inserted_at,
           (SELECT max(id) FROM #{table}) AS max_id
      FROM #{table}
     WHERE id > $1 #{predicate}
     ORDER BY id
     LIMIT $2
    """
  end

  defp query(opts, sql, params) do
    case SQL.query(Map.fetch!(opts, :repo), sql, params) do
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
  def published_since(opts, cursor) when is_integer(cursor) and cursor >= 0 do
    repo = Map.fetch!(opts, :repo)

    # One statement for both halves the poll needs. `array_agg(DISTINCT ...)`
    # over a window bounded by one poll interval of publishes is a range scan on
    # the primary key, and on an idle log — the case that has to stay cheap,
    # since this runs once per interval per app — it touches nothing and both
    # columns come back NULL.
    sql = """
    SELECT max(id), coalesce(array_agg(DISTINCT message_type), '{}')
      FROM #{table(opts, @messages)}
     WHERE id > $1
    """

    case SQL.query(repo, sql, [cursor]) do
      {:ok, %{rows: [[nil, _types]]}} -> {:ok, {nil, []}}
      {:ok, %{rows: [[max_id, types]]}} -> {:ok, {max_id, types}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def last_seen(opts, subscription) when is_binary(subscription) do
    repo = Map.fetch!(opts, :repo)
    sql = "SELECT last_seen FROM #{table(opts, @cursors)} WHERE subscription = $1"

    case SQL.query(repo, sql, [subscription]) do
      {:ok, %{rows: [[last_seen]]}} -> {:ok, last_seen}
      {:ok, %{rows: []}} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def cursor(opts, subscription) when is_binary(subscription) do
    repo = Map.fetch!(opts, :repo)

    sql = """
    SELECT last_seen, updated_at
      FROM #{table(opts, @cursors)}
     WHERE subscription = $1
    """

    case SQL.query(repo, sql, [subscription]) do
      {:ok, %{rows: [[last_seen, updated_at]]}} ->
        {:ok, %{last_seen: last_seen, updated_at: updated_at}}

      {:ok, %{rows: []}} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl DMEx.Storage
  def delete_cursor(opts, subscription) when is_binary(subscription) do
    repo = Map.fetch!(opts, :repo)
    sql = "DELETE FROM #{table(opts, @cursors)} WHERE subscription = $1"

    case SQL.query(repo, sql, [subscription]) do
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
  def advance(opts, subscription, id) when is_binary(subscription) and is_integer(id) do
    repo = Map.fetch!(opts, :repo)

    sql = """
    INSERT INTO #{table(opts, @cursors)} (subscription, last_seen, updated_at)
         VALUES ($1, $2, now())
    ON CONFLICT (subscription)
      DO UPDATE SET last_seen = EXCLUDED.last_seen,
                    updated_at = now()
    """

    {:ok, _} = SQL.query(repo, sql, [subscription, id])

    :ok
  end

  @impl DMEx.Storage
  def dead_message(opts, attrs) when is_map(attrs) do
    repo = Map.fetch!(opts, :repo)

    # `inserted_at` is left to the column default — it records when this row
    # landed here, not when the message was published.
    sql = """
    INSERT INTO #{table(opts, @dead_messages)}
           (subscription, id, message_type, data, metadata)
         VALUES ($1, $2, $3, $4::text::jsonb, $5::text::jsonb)
    """

    params = [
      Map.fetch!(attrs, :subscription),
      Map.fetch!(attrs, :id),
      Map.fetch!(attrs, :message_type),
      Map.fetch!(attrs, :data),
      Map.get(attrs, :metadata) || "{}"
    ]

    case SQL.query(repo, sql, params) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def dead_messages(opts, subscription) when is_binary(subscription) do
    repo = Map.fetch!(opts, :repo)

    # The same projection `read/4` uses, because the columns are the same
    # columns — which is the whole point of the table's shape.
    sql = """
    SELECT id, message_type, data::text, metadata::text, inserted_at
      FROM #{table(opts, @dead_messages)}
     WHERE subscription = $1
     ORDER BY id
    """

    case SQL.query(repo, sql, [subscription]) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, &to_row/1)}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def delete_dead_message(opts, subscription, id)
      when is_binary(subscription) and is_integer(id) do
    repo = Map.fetch!(opts, :repo)

    sql = "DELETE FROM #{table(opts, @dead_messages)} WHERE subscription = $1 AND id = $2"

    case SQL.query(repo, sql, [subscription, id]) do
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

  # pg_notify/2 rather than `NOTIFY <channel>`, so neither a derived channel
  # name nor a message type needs identifier quoting or can be an injection
  # site. Both go in as ordinary parameters.
  #
  # Postgres caps a notification payload at 8000 bytes. A `message_type` is a
  # module name, so this is not a limit anything can reach.
  defp notify!(repo, opts, type) do
    channel = Map.fetch!(opts, :channel)

    {:ok, _} = SQL.query(repo, "SELECT pg_notify($1, $2)", [channel, type])
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
