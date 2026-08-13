defmodule DMEx.Storage.SQLite do
  @moduledoc """
  The SQLite implementation of `DMEx.Storage`.

  Raw SQL through `Ecto.Adapters.SQL`, like the Postgres adapter, and for one of
  the same reasons — the read has to hand back `data` and `metadata` as text. Here
  that costs nothing at all: the columns *are* text.

  Three things here are SQLite's own — `c:dialect/1`,
  `c:DMEx.Storage.append/4`, which needs no lock, and
  `c:DMEx.Storage.validate!/1`. Every statement comes from `DMEx.Storage.SQL`,
  which says why.

  ## `append/4` takes no lock, because the engine is the lock

  `DMEx.Storage` covers why id assignment order must equal commit order, and why
  an engine with one database-wide write transaction satisfies it without a
  lock: a second writer cannot execute its INSERT — cannot draw a rowid — until
  the first has committed, so the interleaving the Postgres adapter guards
  against is unrepresentable.

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
  Postgres's `jsonb` normalizes both away. A `json_valid` CHECK keeps the one
  guarantee `jsonb` gave. See `DMEx.Storage.SQLite.Migration`.

  ## Timestamps are millisecond text

  `inserted_at` is fixed-width ISO-8601 to the millisecond, so byte order is
  time order.

  ## No `:prefix`

  SQLite has no schemas. `validate!/1` raises rather than ignoring one.
  """

  use DMEx.Storage.SQL

  alias Ecto.Adapters.SQL, as: Adapter

  # From the behaviour, not restated: see `DMEx.Storage.__table__/1`.
  @messages DMEx.Storage.__table__(:messages)
  @cursors DMEx.Storage.__table__(:cursors)
  @dead_messages DMEx.Storage.__table__(:dead_messages)

  # `strftime('%f')` renders `SS.SSS`, so every stored value is fixed-width and
  # comparable as bytes, which is what makes `updated_at` orderable at all —
  # SQLite has no timestamp type. Stated once: `DMEx.Storage.SQLite.Migration` renders
  # all three of its column defaults from `__now__/0` below, so no writer of
  # these tables can disagree about the format.
  @now "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"

  @doc """
  What this engine spells differently — `DMEx.Storage.SQL` has the vocabulary.

  No text projection and no JSON cast: the columns are text already, which is what
  `c:DMEx.Storage.read/5` asks for. Bytes are `length(CAST(x AS BLOB))` rather than
  `octet_length`, which needs SQLite 3.43 — exqlite can be built against the system
  library, so the portable spelling is the one that cannot be wrong.
  """
  @impl DMEx.Storage.SQL
  def dialect(_opts) do
    %{
      messages: @messages,
      cursors: @cursors,
      dead_messages: @dead_messages,
      param: "?",
      now: @now,
      text: &Function.identity/1,
      json: &Function.identity/1,
      bytes: &"length(CAST(#{&1} AS BLOB))",
      type_agg: "coalesce(group_concat(DISTINCT type), '')",
      types: &String.split(&1, ","),
      timestamp: &datetime!/1,
      # No array type, so a selection is N placeholders binding N parameters. The
      # varying arity costs nothing: exqlite prepares every statement anyway.
      selection: &{"AND type IN (#{placeholders(&1)})", &1}
    }
  end

  # Public so `DMEx.Storage.SQLite.Migration` stamps its defaults with the same
  # expression a cursor write uses — the counterpart to
  # `DMEx.Storage.Postgres.__table__/2`, shared for the same reason.
  @doc false
  @spec __now__() :: String.t()
  def __now__, do: @now

  # A `%DateTime{}` rather than the stored binary, because `t:DMEx.Storage.row/0`
  # says so.
  defp datetime!(text) when is_binary(text) do
    {:ok, at, 0} = DateTime.from_iso8601(text)

    at
  end

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

  ## Appending — the one statement that is this engine's own

  @impl DMEx.Storage
  def append(type, data, metadata, opts) when is_binary(type) and is_binary(data) and is_binary(metadata) do
    sql = """
    INSERT INTO #{@messages} (type, data, metadata)
         VALUES (?1, ?2, ?3)
      RETURNING id
    """

    # One statement, so SQLite wraps it in an implicit write transaction of its
    # own — see the module documentation for why that is the whole locking story.
    case Adapter.query(Map.fetch!(opts, :repo), sql, [type, data, metadata]) do
      {:ok, %{rows: [[id]]}} -> {:ok, id}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Storage
  def transaction(fun, opts) when is_function(fun, 0) do
    # `:immediate` here as well as in the repo's configuration, so the consumer
    # half is correct even against a repo `validate!/1` never saw. A nested call
    # becomes a SAVEPOINT and ignores the mode, which is right.
    Map.fetch!(opts, :repo).transaction(fun, mode: :immediate)
  end

  @impl DMEx.Storage
  def default_notifier, do: DMEx.Notifier.Beam

  @impl DMEx.Storage
  def migration, do: DMEx.Storage.SQLite.Migration

  # `?4` onward: `?1` to `?3` are the parameters every read binds.
  defp placeholders(types), do: Enum.map_join(4..(3 + length(types))//1, ",", &"?#{&1}")
end
