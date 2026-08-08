defmodule DMEx.Storage.Postgres do
  @moduledoc """
  The Postgres implementation of `DMEx.Storage`.

  Raw SQL through `Ecto.Adapters.SQL.query/4` rather than Ecto schemas:
  `data::text` cannot be expressed as a schema load, and the whole point of the
  read is that `jsonb` comes back as the text `DMEx.Transport.SSE.Frame` splices
  verbatim.

  ## The driver never touches `jsonb`

  Parameters go in as **`$n::text::jsonb`** and `data`/`metadata` come back as
  `column::text`, so Postgrex is never asked to encode or decode `jsonb` and
  DMEx never needs `config :postgrex, :json_library`. `c:DMEx.Storage.record_dead_message/2`
  is the one call handed terms rather than column text, and encodes them here —
  into the same text parameters, so the rule above still holds.

  The double cast is load-bearing and `$n::jsonb` is the trap: Postgres would
  describe the parameter as `jsonb`, at which point Postgrex encodes the binary
  through its own JSON codec and stores a *string* rather than an object. Going
  through `::text` pins the parameter and Postgres does the parse. The failure
  is silent, so a test asserts `jsonb_typeof(data) = 'object'`.
  """

  @behaviour DMEx.Storage

  alias Ecto.Adapters.SQL

  # From the behaviour, not restated: see `DMEx.Storage.__table__/1`.
  @messages DMEx.Storage.__table__(:messages)
  @cursors DMEx.Storage.__table__(:cursors)
  @dead_messages DMEx.Storage.__table__(:dead_messages)

  # Postgres's own unquoted-identifier charset, as a source string rather than a
  # compiled `~r//` so it can appear in the error message it enforces.
  @prefix_charset "^[A-Za-z_][A-Za-z0-9_$]*$"

  @impl DMEx.Storage
  def validate!(opts) do
    validate_prefix!(Map.get(opts, :prefix))
    validate_lock!(opts)
  end

  # A schema name reaches `__table__/2`'s quoted interpolation, so it is checked
  # rather than trusted: anything outside the charset would need quoting rules
  # DMEx is not going to invent, and a `"` would break out of the quoting it does.
  defp validate_prefix!(nil), do: :ok

  defp validate_prefix!(prefix) when is_binary(prefix) do
    if Regex.match?(Regex.compile!(@prefix_charset), prefix) do
      :ok
    else
      raise ArgumentError,
            "`prefix: #{inspect(prefix)}` is not a usable Postgres schema name. " <>
              "#{inspect(__MODULE__)} splices it into quoted identifiers, so it must " <>
              "match #{@prefix_charset}."
    end
  end

  defp validate_prefix!(prefix) do
    raise ArgumentError,
          "`prefix:` must be a schema name as a string or nil, got: #{inspect(prefix)}"
  end

  # `:lock` is log-side — a subscription's opts carry none — and `:log` is what
  # tells the two halves apart; see `t:DMEx.Storage.opts/0`. Checked here so a
  # log without one fails where it was written, rather than as a `KeyError` out
  # of `lock!/2` inside the transaction of the first append.
  defp validate_lock!(%{log: _} = opts) do
    case Map.get(opts, :lock) do
      {classid, objid} when is_integer(classid) and is_integer(objid) ->
        :ok

      other ->
        raise ArgumentError,
              "#{inspect(__MODULE__)} serializes appends with a transaction-scoped " <>
                "advisory lock and needs `lock: {classid, objid}` as two integers, " <>
                "got: #{inspect(other)}"
    end
  end

  defp validate_lock!(_opts), do: :ok

  @impl DMEx.Storage
  def append(type, data, metadata, opts) when is_binary(type) and is_binary(data) and is_binary(metadata) do
    repo = Map.fetch!(opts, :repo)

    repo.transaction(fn ->
      # First, and released only by COMMIT. See DMEx.Storage's module
      # documentation for why anything shorter reintroduces silent id gaps.
      lock!(repo, opts)

      insert!(repo, type, data, metadata, opts)
    end)
  end

  @impl DMEx.Storage
  def read(cursor, max_rows, max_bytes, :all, opts)
      when is_integer(cursor) and cursor >= 0 and is_integer(max_rows) and max_rows > 0 and is_integer(max_bytes) and
             max_bytes > 0 do
    query(read_sql("", opts), [cursor, max_rows, max_bytes], opts)
  end

  def read(cursor, max_rows, max_bytes, types, opts)
      when is_integer(cursor) and cursor >= 0 and is_integer(max_rows) and max_rows > 0 and is_integer(max_bytes) and
             max_bytes > 0 and is_list(types) do
    # A separate statement, not a nullable predicate — see `c:DMEx.Storage.read/5`.
    query(read_sql("AND type = ANY($4)", opts), [cursor, max_rows, max_bytes, types], opts)
  end

  # Public so `DMEx.Storage.PostgresTest` can EXPLAIN the real statement.
  @doc false
  def __read_sql__(predicate, opts), do: read_sql(predicate, opts)

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
  # **`measured`** adds the *exclusive* prefix sum over the same ordering —
  # bytes of the rows strictly before this one; `sum()` over an empty frame is
  # NULL, hence the coalesce, and that NULL-to-zero is exactly the first row.
  #
  # **The outer filter** applies the byte bound — inclusive, plus
  # `prefix_bytes = 0` so one oversized row cannot wedge the subscription. That
  # identifies the first row exactly: `type` is `NOT NULL` and at least
  # one character, so every row contributes at least one byte. See
  # `c:DMEx.Storage.read/5`.
  #
  # The ORDER BY is repeated at the top because a filter over a subquery does
  # not contractually preserve its ordering, and ordering here is a delivery
  # guarantee.
  defp read_sql(predicate, opts) do
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
                   octet_length(data) + octet_length(metadata) + octet_length(type)
                     AS bytes
              FROM (
                SELECT id, type, data::text AS data, metadata::text AS metadata,
                       inserted_at
                  FROM #{table(@messages, opts)}
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

  defp query(sql, params, opts) do
    case SQL.query(Map.fetch!(opts, :repo), sql, params) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, &to_row/1)}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def max_id(opts) do
    repo = Map.fetch!(opts, :repo)
    sql = "SELECT coalesce(max(id), 0) FROM #{table(@messages, opts)}"

    case SQL.query(repo, sql, []) do
      {:ok, %{rows: [[max_id]]}} -> {:ok, max_id}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def appended_since(cursor, opts) when is_integer(cursor) and cursor >= 0 do
    repo = Map.fetch!(opts, :repo)

    # One statement for both halves the poll needs: a primary-key range scan
    # over one poll interval of appends. On an idle log `max(id)` is NULL, which
    # is the empty case.
    sql = """
    SELECT max(id), coalesce(array_agg(DISTINCT type), '{}')
      FROM #{table(@messages, opts)}
     WHERE id > $1
    """

    case SQL.query(repo, sql, [cursor]) do
      {:ok, %{rows: [[nil, _types]]}} -> {:ok, {nil, []}}
      {:ok, %{rows: [[max_id, types]]}} -> {:ok, {max_id, types}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def last_seen(subscription, opts) when is_binary(subscription) do
    repo = Map.fetch!(opts, :repo)
    sql = "SELECT last_seen FROM #{table(@cursors, opts)} WHERE subscription = $1"

    case SQL.query(repo, sql, [subscription]) do
      {:ok, %{rows: [[last_seen]]}} -> {:ok, last_seen}
      {:ok, %{rows: []}} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def cursor(subscription, opts) when is_binary(subscription) do
    repo = Map.fetch!(opts, :repo)

    sql = """
    SELECT last_seen, updated_at
      FROM #{table(@cursors, opts)}
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
  def delete_cursor(subscription, opts) when is_binary(subscription) do
    repo = Map.fetch!(opts, :repo)
    sql = "DELETE FROM #{table(@cursors, opts)} WHERE subscription = $1"

    case SQL.query(repo, sql, [subscription]) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def transaction(fun, opts) when is_function(fun, 0) do
    repo = Map.fetch!(opts, :repo)

    repo.transaction(fun)
  end

  @impl DMEx.Storage
  def advance_cursor(subscription, last_seen, opts) when is_binary(subscription) and is_integer(last_seen) do
    repo = Map.fetch!(opts, :repo)

    sql = """
    INSERT INTO #{table(@cursors, opts)} (subscription, last_seen, updated_at)
         VALUES ($1, $2, now())
    ON CONFLICT (subscription)
      DO UPDATE SET last_seen = EXCLUDED.last_seen,
                    updated_at = now()
    """

    {:ok, _} = SQL.query(repo, sql, [subscription, last_seen])

    :ok
  end

  @impl DMEx.Storage
  def record_dead_message(
        %{"subscription" => subscription, "id" => id, "type" => type, "data" => data, "metadata" => metadata},
        opts
      ) do
    repo = Map.fetch!(opts, :repo)

    sql = """
    INSERT INTO #{table(@dead_messages, opts)}
           (subscription, id, type, data, metadata)
         VALUES ($1, $2, $3, $4::text::jsonb, $5::text::jsonb)
    """

    # A dead row arrives as the consumer held it, so the encode happens here —
    # and lands in the same `::text::jsonb` parameters an append uses.
    params = [subscription, id, type, JSON.encode!(data), JSON.encode!(metadata)]

    case SQL.query(repo, sql, params) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def list_dead_messages(subscription, opts) when is_binary(subscription) do
    sql = """
    SELECT id, type, data::text, metadata::text, inserted_at
      FROM #{table(@dead_messages, opts)}
     WHERE subscription = $1
     ORDER BY id
    """

    query(sql, [subscription], opts)
  end

  @impl DMEx.Storage
  def delete_dead_message(subscription, id, opts) when is_binary(subscription) and is_integer(id) do
    repo = Map.fetch!(opts, :repo)

    sql = "DELETE FROM #{table(@dead_messages, opts)} WHERE subscription = $1 AND id = $2"

    case SQL.query(repo, sql, [subscription, id]) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def default_notifier, do: DMEx.Notifier.Postgres

  ## Append steps

  defp lock!(repo, opts) do
    {classid, objid} = Map.fetch!(opts, :lock)

    {:ok, _} =
      SQL.query(repo, "SELECT pg_advisory_xact_lock($1, $2)", [
        classid,
        objid
      ])
  end

  defp insert!(repo, type, data, metadata, opts) do
    sql = """
    INSERT INTO #{table(@messages, opts)} (type, data, metadata)
         VALUES ($1, $2::text::jsonb, $3::text::jsonb)
      RETURNING id
    """

    {:ok, %{rows: [[id]]}} = SQL.query(repo, sql, [type, data, metadata])

    id
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
      inserted_at: inserted_at
    }
  end

  ## Naming

  defp table(name, opts), do: __table__(name, Map.get(opts, :prefix))

  # Public so `DMEx.Storage.Postgres.Migration` renders qualified names the same
  # way the queries do.
  @doc false
  @spec __table__(String.t() | atom(), String.t() | nil) :: String.t()
  def __table__(name, nil), do: ~s("#{name}")
  def __table__(name, prefix), do: ~s("#{prefix}"."#{name}")
end
