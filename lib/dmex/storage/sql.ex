defmodule DMEx.Storage.SQL do
  @moduledoc """
  Every statement both shipped adapters issue, rendered and run from one dialect.

  Internal to DMEx: an adapter calls this and nothing else does.

  `DMEx.Storage.Postgres` and `DMEx.Storage.SQLite` are the same statements
  against two spellings of SQL. Written out twice they diverged in ways nothing
  caught — a `now()` that is transaction-start on one engine and statement-time on
  the other, an `appended_since` that lost a type, an `ON CONFLICT` on one insert
  and not its twin. So each adapter states what it spells differently and this
  module owns the statements, which is what makes the two engines' answers the
  same answer by construction rather than by review.

  What an adapter keeps is what genuinely differs: the dialect below,
  `c:DMEx.Storage.append/4`, where Postgres takes an advisory lock and SQLite
  relies on having one writer, and the callbacks that name an engine's own
  choices, `c:DMEx.Storage.validate!/1` among them.

  ## The dialect

  Everything the two engines disagree about, and nothing else:

    * `:messages`, `:message_entities`, `:cursors`, `:dead_messages` — the tables,
      rendered as that engine renders them. Postgres quotes and schema-qualifies through
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
    * `:selection` — how a list of values becomes a predicate on a **named
      column** and the parameters it binds, given the number the first of them
      takes. Postgres has an array type and binds one `= ANY($n)`; SQLite has
      neither and renders N placeholders. The index is passed rather than fixed
      because the statements bind a selection at different offsets, and the column
      is passed because two axes narrow this way — `type` on the log, `entity`
      inside the sub-select that `select/3` builds. One closure rather than one
      per axis, so an engine cannot spell set membership two ways.

      It renders the **bare predicate**, with no leading connective: `select/3`
      needs one of them as a sub-select's `WHERE`. `conjoin/1` is what puts the
      `AND` back where a call site wants it. Why a selection is a separate
      statement rather than a nullable predicate is on `read/6`.

  `:text`, `:json` and `:bytes` are per-column functions rather than finished
  expressions, and that is the point of the split: *which* columns are measured,
  and that the same ones are projected, is the shared invariant — an engine gets
  to spell "the byte length of" and nothing more. A finished expression would hand
  back the ability to measure a different set of columns on one engine, which is
  the bug the read statement exists to make unrepresentable.

  ## What an adapter is

  `use DMEx.Storage.SQL` implements every statement below from `c:dialect/1` and
  nothing else, so a SQL adapter *is* its dialect plus the callbacks that are
  genuinely its own.

  `DMEx.Storage` itself stays open to a store with no dialect at all — the
  in-memory stub the test suite records reads through implements those callbacks
  directly, which is what keeps the behaviour a behaviour rather than a SQL
  template.
  """

  alias Ecto.Adapters.SQL, as: Adapter

  @typedoc "What one engine spells differently. See the module documentation."
  @type dialect :: %{
          messages: String.t(),
          message_entities: String.t(),
          cursors: String.t(),
          dead_messages: String.t(),
          param: String.t(),
          now: String.t(),
          text: (String.t() -> String.t()),
          json: (String.t() -> String.t()),
          bytes: (String.t() -> String.t()),
          type_agg: String.t(),
          types: (term() -> [String.t()]),
          timestamp: (term() -> DateTime.t()),
          selection: (String.t(), [String.t()], pos_integer() -> {String.t(), [term()]})
        }

  @doc """
  This engine's dialect, for these options.

  A function rather than a constant because `:prefix` reaches the table names, and
  because three of the entries are closures, which a module attribute cannot hold.
  """
  @callback dialect(DMEx.Storage.opts()) :: dialect()

  @doc false
  defmacro __using__(_opts) do
    quote do
      @behaviour DMEx.Storage
      @behaviour DMEx.Storage.SQL

      @impl DMEx.Storage
      def read(cursor, max_rows, max_bytes, types, opts),
        do: DMEx.Storage.SQL.read(dialect(opts), opts, cursor, max_rows, max_bytes, types)

      @impl DMEx.Storage
      def max_id(opts), do: DMEx.Storage.SQL.max_id(dialect(opts), opts)

      @impl DMEx.Storage
      def appended_since(cursor, opts), do: DMEx.Storage.SQL.appended_since(dialect(opts), opts, cursor)

      @impl DMEx.Storage
      def last_seen(subscription, opts), do: DMEx.Storage.SQL.last_seen(dialect(opts), opts, subscription)

      @impl DMEx.Storage
      def cursor(subscription, opts), do: DMEx.Storage.SQL.cursor(dialect(opts), opts, subscription)

      @impl DMEx.Storage
      def advance_cursor(subscription, last_seen, opts),
        do: DMEx.Storage.SQL.advance_cursor(dialect(opts), opts, subscription, last_seen)

      @impl DMEx.Storage
      def delete_cursor(subscription, opts), do: DMEx.Storage.SQL.delete_cursor(dialect(opts), opts, subscription)

      @impl DMEx.Storage
      def record_dead_message(row, opts), do: DMEx.Storage.SQL.record_dead_message(dialect(opts), opts, row)

      @impl DMEx.Storage
      def list_dead_messages(subscription, opts),
        do: DMEx.Storage.SQL.list_dead_messages(dialect(opts), opts, subscription)

      @impl DMEx.Storage
      def delete_dead_message(subscription, id, opts),
        do: DMEx.Storage.SQL.delete_dead_message(dialect(opts), opts, subscription, id)

      @impl DMEx.Storage
      def select(filters, opts), do: DMEx.Storage.SQL.select(dialect(opts), opts, filters)

      # The one statement a test wants to EXPLAIN. One arity for both engines.
      @doc false
      def __read_sql__(predicate, opts), do: DMEx.Storage.SQL.read_sql(dialect(opts), predicate)
    end
  end

  ## Appending

  @doc """
  Renders the append statement.

  The one statement not generated into the adapter, because
  `c:DMEx.Storage.append/4` is where the engines genuinely differ: Postgres runs
  it inside a transaction holding `pg_advisory_xact_lock`, SQLite lets it be its
  own implicit write transaction. Only the running is theirs — the statement is
  rendered here like every other, so an `ON CONFLICT` or a cast cannot appear on
  one engine's insert and not its twin.
  """
  @spec append_sql(dialect()) :: String.t()
  def append_sql(%{messages: table, param: param, json: json}) do
    """
    INSERT INTO #{table} (type, data, metadata)
         VALUES (#{param}1, #{json.("#{param}2")}, #{json.("#{param}3")})
      RETURNING id
    """
  end

  @doc """
  Renders the insert for one message's entity rows — **one** statement, whatever
  the count.

  `c:DMEx.Storage.append/5` requires this to commit with the message, so it runs
  inside whatever serializes the append: the Postgres adapter's lock transaction,
  or an explicit immediate transaction on SQLite. That is why it is one multi-row
  INSERT rather than one per entity — the statement count there is append
  throughput for the whole application.

  The row id takes parameter 1 and the entities follow, so the id is bound once
  and repeated by number. Postgres could fold the whole append into a single
  data-modifying CTE; SQLite has no DML in a CTE, so two statements is the shape
  both engines share.
  """
  @spec entities_sql(dialect(), pos_integer()) :: String.t()
  def entities_sql(%{message_entities: table, param: param}, count) when count > 0 do
    values = Enum.map_join(2..(count + 1)//1, ", ", &"(#{param}#{&1}, #{param}1)")

    "INSERT INTO #{table} (entity, id) VALUES #{values}"
  end

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

  @doc """
  See `c:DMEx.Storage.read/5`.

  A selection is a *separate statement*, never a nullable predicate — the reason
  is the index rather than the syntax. `:selection` renders it and the parameters
  it binds, because that is the one part of this statement the engines spell
  incompatibly: one `= ANY($4)` against N placeholders. Either way `1` to `3` are
  the parameters every read binds, so a selection appends its own and nothing
  renumbers.
  """
  @spec read(dialect(), DMEx.Storage.opts(), non_neg_integer(), pos_integer(), pos_integer(), :all | [String.t()]) ::
          {:ok, [DMEx.Storage.row()]} | {:error, term()}
  def read(dialect, opts, cursor, max_rows, max_bytes, :all) do
    rows(dialect, opts, read_sql(dialect, ""), [cursor, max_rows, max_bytes])
  end

  def read(dialect, opts, cursor, max_rows, max_bytes, types) when is_list(types) do
    {predicate, params} = narrowing(dialect, :type, types, 4)

    rows(dialect, opts, read_sql(dialect, conjoin(predicate)), [cursor, max_rows, max_bytes | params])
  end

  @doc """
  See `c:DMEx.Storage.select/2`.

  A plain filtered scan, deliberately unlike `read/6`: no byte bound and no
  window functions, because the caller is a person with a `limit:` rather than a
  send loop with a socket. The type selection comes first so that `(type, id)`
  serves it, and `id` is the only orderable column — `inserted_at` is
  observability and never an ordering key
  (`DMEx.Storage.Postgres.Migration` says why).

  **Every narrowing is optional and rendered the same way.** Four of them, in the
  order their parameters bind: the floor and the ceiling through `bound/4` under
  one rule, then `narrowing/4` once per axis, each clause rendering its own from
  the dialect's `:selection` closure. `where/1` joins whatever
  survived, and every offset is threaded from the one before rather than assumed,
  so no narrowing knows what the others rendered. A statement with no narrowing
  at all has no `WHERE`.

  Both bounds are exclusive and neither depends on `:order`: together they are a
  window on `id`, and `:order` only says which end of it the `LIMIT` takes. That
  is what makes a backwards page the same statement as a forwards one, and
  `:desc` reads the same index backwards, so `LIMIT` with it is the newest rows
  for one seek rather than a sort.

  **The entity narrowing is a sub-select on `(entity, id)`**, which is the whole
  of `dmex_message_entities`: seek the entities, and let the outer `id > ?` and
  type predicate filter what comes back. `IN` rather than a join, because a row
  carrying two of the selected entities must come back **once**, and a semi-join
  is what says that in SQL — Postgres satisfies it with a `HashAggregate` rather
  than the sort an explicit `DISTINCT` over a plain join would ask for.

  Which plan that becomes depends on how much of the log the entity carries, and
  `DMEx.Storage.Postgres.Migration` records the `EXPLAIN` session: a selective
  entity seeks this table and sorts the bounded result, a broad one inverts into
  walking the log's primary key and probing this table per row. Both stop at the
  `LIMIT`, which is why there always is one.

  Each axis is absent from the statement entirely when it is `:all`, rather than
  present as a predicate that matches everything: `c:DMEx.Storage.read/5` has the
  argument, and it is about the plan rather than the syntax.

  The direction is rendered here rather than through the dialect: `ASC` and `DESC`
  are spelled the same by every engine, and the dialect is for what is not.
  """
  @spec select(dialect(), DMEx.Storage.opts(), DMEx.Storage.filters()) ::
          {:ok, [DMEx.Storage.row()]} | {:error, term()}
  def select(%{messages: table, param: param, text: text} = dialect, opts, filters) do
    {lower, lower_params} = bound(dialect, ">", filters.after_id, 1)
    {upper, upper_params} = bound(dialect, "<", filters.before_id, 1 + length(lower_params))

    first = 1 + length(lower_params) + length(upper_params)
    {entities, entity_params} = narrowing(dialect, :entity, filters.entities, first)
    {types, type_params} = narrowing(dialect, :type, filters.types, first + length(entity_params))

    params = lower_params ++ upper_params ++ entity_params ++ type_params

    sql = """
    SELECT id, type, #{projected(text, "data")}, #{projected(text, "metadata")}, inserted_at
      FROM #{table}
    #{where([lower, upper, entities, types])}
     ORDER BY id #{direction(filters.order)}
     LIMIT #{param}#{length(params) + 1}
    """

    rows(dialect, opts, sql, params ++ [filters.limit])
  end

  # Both bounds, one function, one rule: a value renders a clause and `nil` does
  # not. `t:DMEx.Storage.filters/0` calls them a symmetric pair, and this is the
  # place that has to be true — a lower bound spelled as the always-present
  # leading predicate and an upper one spelled as an optional extra is two kinds
  # of thing wearing one name, and it puts the parameter numbering of everything
  # downstream in two different places.
  #
  # The alternative was rendering both unconditionally, with the ceiling
  # defaulting to a sentinel. It has to be rejected on correctness rather than on
  # cost: the only candidate is the largest id a `bigint` can hold, and that value
  # *is* a possible id, so `id < sentinel` would hide the last row either engine
  # can ever assign. There is no integer strictly above every id, where 0 is
  # strictly below every one of them — the asymmetry is in the values, not in the
  # code. It is not a performance trade either: the session recorded in
  # `DMEx.Storage.Postgres.Migration` measured the floor present, absent, and
  # against a sentinel, and all three produce the same plan, the same row
  # estimates and the same rows touched.
  #
  # `:after_id` still arrives as 0 rather than `nil` for "from the start", because
  # 0 is a real position in DMEx's one position vocabulary. So the common forwards
  # query renders `id > $1` exactly as it always did.
  defp bound(_dialect, _operator, nil, _first), do: {"", []}

  defp bound(%{param: param}, operator, id, first), do: {"id #{operator} #{param}#{first}", [id]}

  # Every narrowing is optional, so the keyword appears only if one survived, and
  # the `AND`s are joined here rather than carried by each predicate. One line and
  # single spaces throughout: this statement is flat enough not to want the
  # alignment `read_sql/2` is padded into, and that padding is what used to leave
  # a blank line behind when nothing narrowed.
  defp where(predicates) do
    case Enum.reject(predicates, &(&1 == "")) do
      [] -> ""
      predicates -> "WHERE " <> Enum.join(predicates, " AND ")
    end
  end

  # The set-membership narrowings: one function, one clause per axis, and the
  # axis named rather than described. Each clause renders its own SQL in full —
  # a type predicates the column directly, an entity predicates it inside a
  # sub-select on `dmex_message_entities` — so the half that differs is on the
  # page instead of behind an argument, while `:all` is answered once for both.
  #
  # The dialect's `:selection` closure stays the only thing that renders set
  # membership, so an engine cannot spell it one way for a query and another for
  # a reader.
  defp narrowing(_dialect, _axis, :all, _first), do: {"", []}

  defp narrowing(%{selection: selection}, :type, types, first), do: selection.("type", types, first)

  defp narrowing(%{message_entities: table, selection: selection}, :entity, entities, first) do
    {predicate, params} = selection.("entity", entities, first)

    {"id IN (SELECT id FROM #{table} WHERE #{predicate})", params}
  end

  # The selection closure renders a bare predicate, since `select/3` needs one of
  # them as a sub-select's `WHERE` and joins the rest itself. `read_sql/2` splices
  # its predicate in after a `WHERE` that is always there, so it needs the
  # connective put back.
  defp conjoin(""), do: ""
  defp conjoin(predicate), do: "AND " <> predicate

  # Rendered from the atom the behaviour already constrains, never interpolated
  # from a caller's string: this is the one place in a statement where a value
  # cannot be a parameter.
  defp direction(:asc), do: "ASC"
  defp direction(:desc), do: "DESC"

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
