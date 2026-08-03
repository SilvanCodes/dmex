defmodule DMEx.Storage.Postgres do
  @moduledoc """
  The Postgres implementation of `DMEx.Storage`.

  Raw SQL through `Ecto.Adapters.SQL.query/4` rather than Ecto schemas:
  `data::text` cannot be expressed as a schema load, and the whole point of the
  read is that `jsonb` comes back as the text `DMEx.Transport.SSE.Frame` splices
  verbatim.

  ## No JSON library is ever required

  Parameters go in as **`$n::text::jsonb`** and `data`/`metadata` come back as
  `column::text`, so Postgrex is never asked to encode or decode `jsonb` and
  DMEx never needs `config :postgrex, :json_library`.

  The double cast is load-bearing and `$n::jsonb` is the trap: Postgres would
  describe the parameter as `jsonb`, at which point Postgrex encodes the binary
  through its own JSON codec and stores a *string* rather than an object. Going
  through `::text` pins the parameter and Postgres does the parse. The failure
  is silent, so a test asserts `jsonb_typeof(data) = 'object'`.
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
        # else: a routing key, never a carrier of work.
        notify!(repo, opts, type)

        id
      end)

    case result do
      {:ok, id} -> {:ok, id}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def read(opts, cursor, limit, max_bytes, :all)
      when is_integer(cursor) and cursor >= 0 and is_integer(limit) and limit > 0 and
             is_integer(max_bytes) and max_bytes > 0 do
    query(opts, read_sql(opts, ""), [cursor, limit, max_bytes])
  end

  def read(opts, cursor, limit, max_bytes, types)
      when is_integer(cursor) and cursor >= 0 and is_integer(limit) and limit > 0 and
             is_integer(max_bytes) and max_bytes > 0 and is_list(types) do
    # A separate statement, not a nullable predicate — see `c:DMEx.Storage.read/5`.
    query(opts, read_sql(opts, "AND message_type = ANY($4)"), [cursor, limit, max_bytes, types])
  end

  # Public so `DMEx.Storage.PostgresTest` can EXPLAIN the real statement.
  @doc false
  def __read_sql__(opts, predicate), do: read_sql(opts, predicate)

  # Four nested levels, each load-bearing.
  #
  # **`page`** applies the row limit against the index, so everything above it
  # works on at most `$2` rows. Not stylistic: within one SELECT, Postgres
  # computes window functions *before* LIMIT, so a window at the same level as
  # the LIMIT would run the sum over every matching row in the log. The `::text`
  # casts happen here, once.
  #
  # **`sized`** gives each row its byte count, off the already-cast columns.
  #
  # **`measured`** adds two things over the same ordering. The *exclusive*
  # prefix sum — bytes of the rows strictly before this one; `sum()` over an
  # empty frame is NULL, hence the coalesce, and that NULL-to-zero is exactly
  # the first row. And `previous_id` via `lag/3`, defaulting to the cursor so
  # the first row of a page links back to what the caller asked to resume after.
  #
  # **The outer filter** applies the byte bound — inclusive, plus
  # `prefix_bytes = 0` so one oversized row cannot wedge the subscription. That
  # identifies the first row exactly: `message_type` is `NOT NULL` and at least
  # one character, so every row contributes at least one byte. See
  # `c:DMEx.Storage.read/5`.
  #
  # The ORDER BY is repeated at the top because a filter over a subquery does
  # not contractually preserve its ordering, and ordering here is a delivery
  # guarantee.
  defp read_sql(opts, predicate) do
    """
    SELECT id, previous_id, message_type, data, metadata, inserted_at
      FROM (
        SELECT id, message_type, data, metadata, inserted_at, bytes,
               lag(id, 1, $1) OVER (ORDER BY id) AS previous_id,
               coalesce(
                 sum(bytes) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                 0
               ) AS prefix_bytes
          FROM (
            SELECT id, message_type, data, metadata, inserted_at,
                   octet_length(data) + octet_length(metadata) + octet_length(message_type)
                     AS bytes
              FROM (
                SELECT id, message_type, data::text AS data, metadata::text AS metadata,
                       inserted_at
                  FROM #{table(opts, @messages)}
                 WHERE id > $1 #{predicate}
                 ORDER BY id
                 LIMIT $2
              ) page
          ) sized
      ) measured
     WHERE prefix_bytes + bytes <= $3 OR prefix_bytes = 0
     ORDER BY id
    """
  end

  defp query(opts, sql, params) do
    case SQL.query(Map.fetch!(opts, :repo), sql, params) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, &to_row/1)}
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
  def cursor_at(opts, %DateTime{} = at) do
    repo = Map.fetch!(opts, :repo)

    # An ordered LIMIT 1, not `max(id)`: the planner cannot know `inserted_at`
    # ascends with `id`, so the aggregate visits every row before the instant
    # where this is a backward scan of `(inserted_at, id)` — one descent. The
    # two agree only because `clock_timestamp()` makes the columns monotonic
    # together; see `c:DMEx.Storage.cursor_at/2`.
    #
    # The coalesce is the whole of the empty case: no row before the instant
    # means cursor 0, which is the whole log.
    sql = """
    SELECT coalesce((SELECT id
                       FROM #{table(opts, @messages)}
                      WHERE inserted_at < $1
                      ORDER BY inserted_at DESC, id DESC
                      LIMIT 1), 0)
    """

    case SQL.query(repo, sql, [at]) do
      {:ok, %{rows: [[cursor]]}} -> {:ok, cursor}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def appended_since(opts, cursor) when is_integer(cursor) and cursor >= 0 do
    repo = Map.fetch!(opts, :repo)

    # One statement for both halves the poll needs: a primary-key range scan
    # over one poll interval of appends. On an idle log `max(id)` is NULL, which
    # is the empty case.
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

    # `read/5`'s columns minus `previous_id`, which a dead row has no delivered
    # sequence for.
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

  defp notifier(opts), do: opts |> Map.fetch!(:log) |> Module.concat("Notifications")

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
  # site. Postgres caps a notification payload at 8000 bytes, which a module
  # name cannot reach.
  defp notify!(repo, opts, type) do
    channel = Map.fetch!(opts, :channel)

    {:ok, _} = SQL.query(repo, "SELECT pg_notify($1, $2)", [channel, type])
  end

  ## Row shaping

  defp to_row([id, previous_id, message_type, data, metadata, inserted_at]) do
    %{
      id: id,
      previous_id: previous_id,
      message_type: message_type,
      data: data,
      metadata: metadata,
      inserted_at: inserted_at
    }
  end

  # `dead_messages/2` reads a table with no delivered sequence to chain over —
  # a replayed row goes straight to a handler, never framed — so it gets a nil
  # predecessor rather than a meaningless one.
  defp to_row([id, message_type, data, metadata, inserted_at]) do
    to_row([id, nil, message_type, data, metadata, inserted_at])
  end

  ## Naming

  defp table(opts, name) do
    case Map.get(opts, :prefix) do
      nil -> ~s("#{name}")
      prefix -> ~s("#{prefix}"."#{name}")
    end
  end
end
