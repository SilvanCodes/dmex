defmodule DMEx.Storage.SQLite do
  @moduledoc """
  The SQLite implementation of `DMEx.Storage`.

  Raw SQL through `Ecto.Adapters.SQL.query/4`, like the Postgres adapter, and
  for one of the same reasons — the read has to hand back `data` and `metadata`
  as text. Here that costs nothing at all: the columns *are* text.

  ## `append/4` takes no lock, because the engine is the lock

  Postgres allocates a sequence value at INSERT and makes the row visible at
  COMMIT, so two appends can invert and DMEx has to serialize them itself with
  an advisory lock. SQLite has one write transaction database-wide: a second
  writer cannot execute its INSERT — cannot draw a rowid — until the first has
  committed. Assignment order *is* commit order, and the interleaving the
  Postgres adapter guards against is unrepresentable.

  So an append is one statement, `INSERT ... RETURNING id`, which is its own
  implicit write transaction. Wrapping it in `repo.transaction/1` would
  introduce the problem below rather than solve one. `opts.lock` is unused.

  ## The repo must be `default_transaction_mode: :immediate`

  `BEGIN DEFERRED` — ecto_sqlite3's default — starts a *read* transaction, and
  promoting it to a write returns `SQLITE_BUSY`, or in WAL mode
  `SQLITE_BUSY_SNAPSHOT`, which no busy handler can resolve because the read
  snapshot is already stale. Any caller wrapping `append/2` in a transaction of
  its own would hit it under concurrency. `:immediate` takes the write lock at
  `BEGIN`, leaving only the retryable case. `validate!/1` refuses a repo without
  it at boot, and `transaction/2` passes the mode itself.

  ## Payloads are stored verbatim

  `data` and `metadata` are TEXT holding JSON, so the bytes read back are the
  bytes the publisher wrote — key order and duplicate keys included, where
  Postgres's `jsonb` normalizes both away. Nothing is cast on the way in or out,
  so `c:DMEx.Storage.read/5`'s splice-verbatim contract is free rather than
  bought with a `::text` render per row. A `json_valid` CHECK keeps the one
  guarantee `jsonb` gave. See `DMEx.Storage.SQLite.Migration`.

  ## Timestamps are millisecond text

  `inserted_at` is fixed-width ISO-8601 to the millisecond, so byte order is
  time order, and `datetime!/1` turns it back into the `%DateTime{}`
  `t:DMEx.Storage.row/0` promises.

  ## No `:prefix`

  SQLite has no schemas. `validate!/1` raises rather than ignoring one.
  """

  @behaviour DMEx.Storage

  alias Ecto.Adapters.SQL

  # From the behaviour, not restated: see `DMEx.Storage.__table__/1`.
  @messages DMEx.Storage.__table__(:messages)
  @cursors DMEx.Storage.__table__(:cursors)
  @dead_messages DMEx.Storage.__table__(:dead_messages)

  # SQLite's own limit is 32766 bound parameters (999 before 3.32). This is the
  # conservative one, and the point is a named error rather than an opaque one
  # from the driver: a selection is expanded from a handler's declared message
  # set, so hundreds of types means something went wrong upstream.
  @max_types 900

  # `strftime('%f')` renders `SS.SSS`, so every stored value is fixed-width and
  # comparable as bytes, which is what makes `updated_at` orderable at all —
  # SQLite has no timestamp type. Stated once: `DMEx.Storage.SQLite.Migration` renders
  # all three of its column defaults from `__now__/0` below, so no writer of
  # these tables can disagree about the format.
  @now "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"

  # Public so `DMEx.Storage.SQLite.Migration` stamps its defaults with the same
  # expression `advance_cursor/3` writes with — the counterpart to
  # `DMEx.Storage.Postgres.__table__/2`, shared for the same reason.
  @doc false
  @spec __now__() :: String.t()
  def __now__, do: @now

  @impl DMEx.Storage
  def default_notifier, do: DMEx.Notifier.Beam

  @impl DMEx.Storage
  def validate!(opts) do
    case Map.get(opts, :prefix) do
      nil ->
        :ok

      prefix ->
        raise ArgumentError,
              "SQLite has no schemas, so #{inspect(__MODULE__)} cannot honour " <>
                "`prefix: #{inspect(prefix)}`. Drop it, or give this log a database " <>
                "file of its own — which is what a schema would have bought."
    end

    validate_transaction_mode!(opts)
  end

  # Read from the repo rather than taken on faith: the failure it prevents is a
  # `SQLITE_BUSY_SNAPSHOT` under concurrency, which is rare enough in
  # development to ship.
  defp validate_transaction_mode!(opts) do
    repo = Map.fetch!(opts, :repo)

    case repo.config()[:default_transaction_mode] do
      :immediate ->
        :ok

      other ->
        raise ArgumentError, """
        #{inspect(repo)} is configured with default_transaction_mode: #{inspect(other)}, \
        and #{inspect(__MODULE__)} requires :immediate.

            config :my_app, #{inspect(repo)},
              database: "priv/my_app.db",
              default_transaction_mode: :immediate

        A deferred transaction starts as a reader, and promoting it to a writer \
        returns SQLITE_BUSY_SNAPSHOT, which no busy timeout can resolve.\
        """
    end
  end

  @impl DMEx.Storage
  def append(opts, type, data, metadata) when is_binary(type) and is_binary(data) and is_binary(metadata) do
    sql = """
    INSERT INTO #{@messages} (type, data, metadata)
         VALUES (?1, ?2, ?3)
      RETURNING id
    """

    # One statement, so SQLite wraps it in an implicit write transaction of its
    # own — see the module documentation for why that is the whole locking story.
    case SQL.query(Map.fetch!(opts, :repo), sql, [type, data, metadata]) do
      {:ok, %{rows: [[id]]}} -> {:ok, id}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def read(opts, cursor, max_rows, max_bytes, :all)
      when is_integer(cursor) and cursor >= 0 and is_integer(max_rows) and max_rows > 0 and is_integer(max_bytes) and
             max_bytes > 0 do
    query(opts, read_sql(""), [cursor, max_rows, max_bytes])
  end

  # Ahead of the list clause: an empty selection admits nothing, and building
  # `IN ()` out of it would be a descending range and an unbindable statement.
  # `DMEx.Storage.Postgres` answers the same call with an empty `= ANY($4)`.
  def read(_opts, cursor, max_rows, max_bytes, [])
      when is_integer(cursor) and cursor >= 0 and is_integer(max_rows) and max_rows > 0 and is_integer(max_bytes) and
             max_bytes > 0 do
    {:ok, []}
  end

  def read(opts, cursor, max_rows, max_bytes, types)
      when is_integer(cursor) and cursor >= 0 and is_integer(max_rows) and max_rows > 0 and is_integer(max_bytes) and
             max_bytes > 0 and is_list(types) do
    validate_types!(types)

    # SQLite has no array type, so a selection is N placeholders rather than one
    # `= ANY($4)`. Still a *separate statement* and never a nullable predicate —
    # see `c:DMEx.Storage.read/5`; the reason is the index, not the syntax. The
    # varying arity costs nothing: exqlite prepares every statement anyway.
    placeholders = Enum.map_join(4..(3 + length(types))//1, ",", &"?#{&1}")

    query(opts, read_sql("AND type IN (#{placeholders})"), [
      cursor,
      max_rows,
      max_bytes | types
    ])
  end

  # Public so `DMEx.Storage.SQLiteTest` can EXPLAIN the real statement.
  @doc false
  def __read_sql__(predicate), do: read_sql(predicate)

  defp validate_types!(types) do
    if length(types) > @max_types do
      raise ArgumentError,
            "a selective read of #{length(types)} message types exceeds SQLite's " <>
              "bound-parameter limit; #{inspect(__MODULE__)} allows #{@max_types}"
    end
  end

  # The Postgres adapter's four levels, and each is load-bearing for the same
  # reason here: SQLite also computes window functions before `LIMIT` within one
  # SELECT, and it does not flatten a subquery that contains one — so `page` is
  # what keeps the running sum off every row after the cursor.
  #
  # Two things differ. There is no `::text` cast, because the columns are text.
  # And bytes are `length(CAST(x AS BLOB))` rather than `octet_length` — the
  # latter needs SQLite 3.43, and exqlite can be built against the system
  # library, so the portable spelling is the one that cannot be wrong.
  #
  # `?1` twice rather than a repeated parameter: numbered placeholders bind one
  # value to every occurrence, which keeps the params list identical to the
  # Postgres adapter's.
  defp read_sql(predicate) do
    """
    SELECT id, type, data, metadata, inserted_at
      FROM (
        SELECT id, type, data, metadata, inserted_at, bytes,
               coalesce(
                 sum(bytes) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                 0
               ) AS prefix_bytes
          FROM (
            SELECT id, type, data, metadata, inserted_at,
                   length(CAST(data AS BLOB)) +
                   length(CAST(metadata AS BLOB)) +
                   length(CAST(type AS BLOB)) AS bytes
              FROM (
                SELECT id, type, data, metadata, inserted_at
                  FROM #{@messages}
                 WHERE id > ?1 #{predicate}
                 ORDER BY id
                 LIMIT ?2
              ) page
          ) sized
      ) measured
     WHERE prefix_bytes + bytes <= ?3 OR prefix_bytes = 0
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
    case SQL.query(Map.fetch!(opts, :repo), "SELECT coalesce(max(id), 0) FROM #{@messages}", []) do
      {:ok, %{rows: [[max_id]]}} -> {:ok, max_id}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def appended_since(opts, cursor) when is_integer(cursor) and cursor >= 0 do
    # One statement, because the empty case runs once per poll interval per app
    # and is the common one. `group_concat` takes DISTINCT only in its
    # single-argument form, so the separator is not ours to choose — splitting on
    # it is lossless anyway, since the `type` CHECK forbids a comma
    # inside a type. See `DMEx.Storage.SQLite.Migration`.
    sql = """
    SELECT max(id), coalesce(group_concat(DISTINCT type), '')
      FROM #{@messages}
     WHERE id > ?1
    """

    case SQL.query(Map.fetch!(opts, :repo), sql, [cursor]) do
      {:ok, %{rows: [[nil, _types]]}} -> {:ok, {nil, []}}
      {:ok, %{rows: [[max_id, ""]]}} -> {:ok, {max_id, []}}
      {:ok, %{rows: [[max_id, types]]}} -> {:ok, {max_id, String.split(types, ",")}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def last_seen(opts, subscription) when is_binary(subscription) do
    sql = "SELECT last_seen FROM #{@cursors} WHERE subscription = ?1"

    case SQL.query(Map.fetch!(opts, :repo), sql, [subscription]) do
      {:ok, %{rows: [[last_seen]]}} -> {:ok, last_seen}
      {:ok, %{rows: []}} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def cursor(opts, subscription) when is_binary(subscription) do
    sql = "SELECT last_seen, updated_at FROM #{@cursors} WHERE subscription = ?1"

    case SQL.query(Map.fetch!(opts, :repo), sql, [subscription]) do
      {:ok, %{rows: [[last_seen, updated_at]]}} ->
        {:ok, %{last_seen: last_seen, updated_at: datetime!(updated_at)}}

      {:ok, %{rows: []}} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl DMEx.Storage
  def delete_cursor(opts, subscription) when is_binary(subscription) do
    sql = "DELETE FROM #{@cursors} WHERE subscription = ?1"

    case SQL.query(Map.fetch!(opts, :repo), sql, [subscription]) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def transaction(opts, fun) when is_function(fun, 0) do
    # `:immediate` here as well as in the repo's configuration, so the consumer
    # half is correct even against a repo `validate!/1` never saw. A nested call
    # becomes a SAVEPOINT and ignores the mode, which is right.
    Map.fetch!(opts, :repo).transaction(fun, mode: :immediate)
  end

  @impl DMEx.Storage
  def advance_cursor(opts, subscription, last_seen) when is_binary(subscription) and is_integer(last_seen) do
    sql = """
    INSERT INTO #{@cursors} (subscription, last_seen, updated_at)
         VALUES (?1, ?2, #{@now})
    ON CONFLICT (subscription)
      DO UPDATE SET last_seen = excluded.last_seen,
                    updated_at = #{@now}
    """

    {:ok, _} = SQL.query(Map.fetch!(opts, :repo), sql, [subscription, last_seen])

    :ok
  end

  @impl DMEx.Storage
  def record_dead_message(opts, attrs) when is_map(attrs) do
    # Five plain parameters. The Postgres adapter's `$n::text::jsonb` exists to
    # stop its driver re-encoding an already-encoded binary; text has no such
    # trap to fall into.
    sql = """
    INSERT INTO #{@dead_messages} (subscription, id, type, data, metadata)
         VALUES (?1, ?2, ?3, ?4, ?5)
    """

    params = [
      Map.fetch!(attrs, :subscription),
      Map.fetch!(attrs, :id),
      Map.fetch!(attrs, :type),
      Map.fetch!(attrs, :data),
      Map.get(attrs, :metadata) || "{}"
    ]

    case SQL.query(Map.fetch!(opts, :repo), sql, params) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def list_dead_messages(opts, subscription) when is_binary(subscription) do
    sql = """
    SELECT id, type, data, metadata, inserted_at
      FROM #{@dead_messages}
     WHERE subscription = ?1
     ORDER BY id
    """

    query(opts, sql, [subscription])
  end

  @impl DMEx.Storage
  def delete_dead_message(opts, subscription, id) when is_binary(subscription) and is_integer(id) do
    sql = "DELETE FROM #{@dead_messages} WHERE subscription = ?1 AND id = ?2"

    case SQL.query(Map.fetch!(opts, :repo), sql, [subscription, id]) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  ## Row shaping

  # This head **is** the SELECT list, in order: change the column list without
  # changing the head and the rows come out mis-keyed. Not silently —
  # `test/dmex/storage/conformance_test.exs` value-pins `type`, so the
  # mismatch is a failing suite rather than a wrong wire frame.
  defp to_row([id, type, data, metadata, inserted_at]) do
    %{
      id: id,
      type: type,
      data: data,
      metadata: metadata,
      inserted_at: datetime!(inserted_at)
    }
  end

  ## Timestamps

  # A `%DateTime{}` rather than the stored binary, because `t:DMEx.Storage.row/0`
  # says so.
  defp datetime!(text) when is_binary(text) do
    {:ok, at, 0} = DateTime.from_iso8601(text)

    at
  end
end
