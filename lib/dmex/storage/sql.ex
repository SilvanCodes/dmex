defmodule DMEx.Storage.SQL do
  @moduledoc """
  Every statement both shipped adapters issue, rendered and run from one dialect.

  Internal to DMEx: an adapter calls this and nothing else does.

  `DMEx.Storage.Postgres` and `DMEx.Storage.SQLite` are the same eleven statements
  against two spellings of SQL. Written out twice they diverged in ways nothing
  caught — a `now()` that is transaction-start on one engine and statement-time on
  the other, an `appended_since` that lost a type, an `ON CONFLICT` on one insert
  and not its twin. So each adapter states what it spells differently and this
  module owns the statements, which is what makes the two engines' answers the
  same answer by construction rather than by review.

  What an adapter keeps is what genuinely differs: `c:DMEx.Storage.append/4`,
  where Postgres takes an advisory lock and SQLite relies on having one writer;
  `c:DMEx.Storage.validate!/1`; and the dialect below.

  ## The dialect

  Everything the two engines disagree about, and nothing else:

    * `:messages`, `:cursors`, `:dead_messages` — the tables, rendered as that
      engine renders them. Postgres quotes and schema-qualifies through
      `DMEx.Storage.Postgres.__table__/2`; SQLite has no schemas and interpolates
      the bare name.
    * `:param` — the sigil a numbered placeholder starts with, `"$"` or `"?"`.
      Both engines number from 1 and bind one value to every occurrence of a
      number, so the sigil is the whole difference.
    * `:now` — the engine's statement clock, for the two `updated_at`/`inserted_at`
      columns a write touches. Never the log's `inserted_at`, which is the
      migration's default and has to ascend with `id`.
    * `:text` — how one payload column is projected as JSON text, which is what
      `c:DMEx.Storage.read/5` promises. Postgres stores `jsonb` and casts; SQLite's
      columns are already text.
    * `:json` — the inverse, for a parameter going *into* a payload column.
      Postgres pins it through `::text::jsonb` so its driver cannot re-encode an
      already-encoded binary; SQLite binds it plain.
    * `:bytes` — the byte length of one column. `octet_length(x)` on Postgres,
      `length(CAST(x AS BLOB))` on SQLite, which is the spelling that does not
      need SQLite 3.43.
    * `:type_agg` and `:types` — a pair: the `DISTINCT type` aggregate expression,
      and how its result comes back. Postgres has `array_agg` and hands back a
      list; SQLite has `group_concat`, whose separator is not ours to choose in
      its `DISTINCT` form, and hands back a string. Splitting on a comma is
      lossless because the `type` CHECK forbids one inside a type.
    * `:timestamp` — how a stored timestamp becomes the `%DateTime{}`
      `t:DMEx.Storage.row/0` promises. Postgres's driver already returns one.

  `:text`, `:json` and `:bytes` are per-column functions rather than finished
  expressions, and that is the point of the split: *which* columns are measured,
  and that the same ones are projected, is the shared invariant — an engine gets
  to spell "the byte length of" and nothing more. A finished expression would hand
  back the ability to measure a different set of columns on one engine, which is
  the bug the read statement exists to make unrepresentable.
  """

  alias Ecto.Adapters.SQL, as: Adapter

  @typedoc "What one engine spells differently. See the module documentation."
  @type dialect :: %{
          messages: String.t(),
          cursors: String.t(),
          dead_messages: String.t(),
          param: String.t(),
          now: String.t(),
          text: (String.t() -> String.t()),
          json: (String.t() -> String.t()),
          bytes: (String.t() -> String.t()),
          type_agg: String.t(),
          types: (term() -> [String.t()]),
          timestamp: (term() -> DateTime.t())
        }

  ## Reading the log

  @doc """
  Renders the read statement, whose bound is the reason this module exists.

  Four nested levels, each load-bearing.

  **`page`** applies the row limit against the index, so everything above it
  works on at most `LIMIT` rows. Not stylistic: within one SELECT both engines
  compute window functions *before* LIMIT, so a window at the same level as the
  LIMIT would run the sum over every matching row in the log — and neither
  flattens a subquery that contains one, so the nesting holds. Any `:text`
  projection happens here, once.

  **`sized`** gives each row its byte count, off the already-projected columns.

  **`measured`** adds the *exclusive* prefix sum over the same ordering — bytes
  of the rows strictly before this one; `sum()` over an empty frame is NULL,
  hence the coalesce, and that NULL-to-zero is exactly the first row.

  **The outer filter** applies the byte bound — inclusive, plus
  `prefix_bytes = 0` so one oversized row cannot wedge the subscription. That
  identifies the first row exactly: `type` is `NOT NULL` and at least one
  character, so every row contributes at least one byte. See
  `c:DMEx.Storage.read/5`.

  The ORDER BY is repeated at the top because a filter over a subquery does not
  contractually preserve its ordering, and ordering here is a delivery
  guarantee.

  `predicate` is the type selection for this one call, rendered by the adapter
  because the engines spell set membership differently — one `= ANY($4)` against
  N placeholders — so the numbering of everything after the three fixed
  parameters is the adapter's business. Empty string for a read of every type.
  """
  @spec read_sql(dialect(), String.t()) :: String.t()
  def read_sql(%{messages: table, param: param, text: text, bytes: bytes}, predicate) do
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
                   #{bytes.("data")} +
                   #{bytes.("metadata")} +
                   #{bytes.("type")} AS bytes
              FROM (
                SELECT id, type, #{projected(text, "data")}, #{projected(text, "metadata")},
                       inserted_at
                  FROM #{table}
                 WHERE id > #{param}1 #{predicate}
                 ORDER BY id
                 LIMIT #{param}2
              ) page
          ) sized
      ) measured
     WHERE prefix_bytes + bytes <= #{param}3 OR prefix_bytes = 0
     ORDER BY id
    """
  end

  @doc "See `c:DMEx.Storage.read/5`. `predicate` and its parameters are the adapter's."
  @spec read(dialect(), DMEx.Storage.opts(), String.t(), [term()]) ::
          {:ok, [DMEx.Storage.row()]} | {:error, term()}
  def read(dialect, opts, predicate, params) do
    rows(dialect, opts, read_sql(dialect, predicate), params)
  end

  @doc "See `c:DMEx.Storage.max_id/1`."
  @spec max_id(dialect(), DMEx.Storage.opts()) :: {:ok, non_neg_integer()} | {:error, term()}
  def max_id(%{messages: table} = dialect, opts) do
    with {:ok, [[max_id]]} <- query(dialect, opts, "SELECT coalesce(max(id), 0) FROM #{table}", []) do
      {:ok, max_id}
    end
  end

  @doc """
  See `c:DMEx.Storage.appended_since/2`.

  One statement for both halves the poll needs: a primary-key range scan over one
  poll interval of appends. On an idle log `max(id)` is NULL, and that is the
  empty case for the types too — no arm of its own, on either engine.
  """
  @spec appended_since(dialect(), DMEx.Storage.opts(), non_neg_integer()) ::
          {:ok, {non_neg_integer() | nil, [String.t()]}} | {:error, term()}
  def appended_since(%{messages: table, param: param, type_agg: aggregate, types: types} = dialect, opts, cursor) do
    sql = """
    SELECT max(id), #{aggregate}
      FROM #{table}
     WHERE id > #{param}1
    """

    with {:ok, [[max_id, aggregated]]} <- query(dialect, opts, sql, [cursor]) do
      case max_id do
        nil -> {:ok, {nil, []}}
        max_id -> {:ok, {max_id, types.(aggregated)}}
      end
    end
  end

  ## Cursors

  @doc "See `c:DMEx.Storage.last_seen/2`."
  @spec last_seen(dialect(), DMEx.Storage.opts(), String.t()) ::
          {:ok, non_neg_integer() | nil} | {:error, term()}
  def last_seen(%{cursors: table, param: param} = dialect, opts, subscription) do
    sql = "SELECT last_seen FROM #{table} WHERE subscription = #{param}1"

    with {:ok, rows} <- query(dialect, opts, sql, [subscription]) do
      case rows do
        [[last_seen]] -> {:ok, last_seen}
        [] -> {:ok, nil}
      end
    end
  end

  @doc "See `c:DMEx.Storage.cursor/2`."
  @spec cursor(dialect(), DMEx.Storage.opts(), String.t()) :: {:ok, map() | nil} | {:error, term()}
  def cursor(%{cursors: table, param: param, timestamp: timestamp} = dialect, opts, subscription) do
    sql = "SELECT last_seen, updated_at FROM #{table} WHERE subscription = #{param}1"

    with {:ok, rows} <- query(dialect, opts, sql, [subscription]) do
      case rows do
        [[last_seen, updated_at]] -> {:ok, %{last_seen: last_seen, updated_at: timestamp.(updated_at)}}
        [] -> {:ok, nil}
      end
    end
  end

  @doc "See `c:DMEx.Storage.advance_cursor/3`."
  @spec advance_cursor(dialect(), DMEx.Storage.opts(), String.t(), non_neg_integer()) :: :ok
  def advance_cursor(%{cursors: table, param: param, now: now} = dialect, opts, subscription, last_seen) do
    sql = """
    INSERT INTO #{table} (subscription, last_seen, updated_at)
         VALUES (#{param}1, #{param}2, #{now})
    ON CONFLICT (subscription)
      DO UPDATE SET last_seen = excluded.last_seen,
                    updated_at = #{now}
    """

    write(dialect, opts, sql, [subscription, last_seen])
  end

  @doc "See `c:DMEx.Storage.delete_cursor/2`."
  @spec delete_cursor(dialect(), DMEx.Storage.opts(), String.t()) :: :ok
  def delete_cursor(%{cursors: table, param: param} = dialect, opts, subscription) do
    write(dialect, opts, "DELETE FROM #{table} WHERE subscription = #{param}1", [subscription])
  end

  ## Dead messages

  @doc """
  See `c:DMEx.Storage.record_dead_message/2`.

  An upsert, because a cursor moves both ways and the same message can arrive
  twice — the callback docs have the whole argument. A dead row arrives as the
  consumer held it, so the payload encode happens here.

  The whole row shape is in the head, so one short of a key raises here rather
  than being written half-formed.
  """
  @spec record_dead_message(dialect(), DMEx.Storage.opts(), DMEx.Storage.dead_row()) :: :ok
  def record_dead_message(%{dead_messages: table, param: param, now: now, json: json} = dialect, opts, %{
        "subscription" => subscription,
        "id" => id,
        "type" => type,
        "data" => data,
        "metadata" => metadata
      }) do
    sql = """
    INSERT INTO #{table} (subscription, id, type, data, metadata)
         VALUES (#{param}1, #{param}2, #{param}3, #{json.("#{param}4")}, #{json.("#{param}5")})
    ON CONFLICT (subscription, id)
      DO UPDATE SET type = excluded.type,
                    data = excluded.data,
                    metadata = excluded.metadata,
                    inserted_at = #{now}
    """

    write(dialect, opts, sql, [subscription, id, type, JSON.encode!(data), JSON.encode!(metadata)])
  end

  @doc "See `c:DMEx.Storage.list_dead_messages/2`."
  @spec list_dead_messages(dialect(), DMEx.Storage.opts(), String.t()) ::
          {:ok, [DMEx.Storage.row()]} | {:error, term()}
  def list_dead_messages(%{dead_messages: table, param: param, text: text} = dialect, opts, subscription) do
    sql = """
    SELECT id, type, #{projected(text, "data")}, #{projected(text, "metadata")}, inserted_at
      FROM #{table}
     WHERE subscription = #{param}1
     ORDER BY id
    """

    rows(dialect, opts, sql, [subscription])
  end

  @doc "See `c:DMEx.Storage.delete_dead_message/3`."
  @spec delete_dead_message(dialect(), DMEx.Storage.opts(), String.t(), non_neg_integer()) :: :ok
  def delete_dead_message(%{dead_messages: table, param: param} = dialect, opts, subscription, id) do
    sql = "DELETE FROM #{table} WHERE subscription = #{param}1 AND id = #{param}2"

    write(dialect, opts, sql, [subscription, id])
  end

  ## Running

  # A write returns `:ok` or raises — see *how a write fails* in `DMEx.Storage`.
  defp write(_dialect, opts, sql, params) do
    Adapter.query!(Map.fetch!(opts, :repo), sql, params)

    :ok
  end

  defp query(_dialect, opts, sql, params) do
    with {:ok, %{rows: rows}} <- Adapter.query(Map.fetch!(opts, :repo), sql, params) do
      {:ok, rows}
    end
  end

  # The SELECT list and this shape are one thing: change the columns without
  # changing the keys and rows come out mis-keyed. Not silently —
  # `test/dmex/storage/conformance_test.exs` value-pins `type`.
  defp rows(%{timestamp: timestamp} = dialect, opts, sql, params) do
    with {:ok, rows} <- query(dialect, opts, sql, params) do
      {:ok,
       Enum.map(rows, fn [id, type, data, metadata, inserted_at] ->
         %{id: id, type: type, data: data, metadata: metadata, inserted_at: timestamp.(inserted_at)}
       end)}
    end
  end

  ## Rendering

  # `data::text AS data` on an engine that casts, bare `data` on one that does
  # not — the alias matters, since the outer levels select the column by name.
  defp projected(text, column) do
    case text.(column) do
      ^column -> column
      cast -> "#{cast} AS #{column}"
    end
  end
end
