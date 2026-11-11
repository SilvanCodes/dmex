defmodule DMEx.Storage.SQLite do
  @moduledoc """
  The SQLite implementation of `DMEx.Storage`.

  Raw SQL through `Ecto.Adapters.SQL`, like the Postgres adapter, and for one of
  the same reasons — the read has to hand back `data` and `metadata` as text. Here
  that costs nothing at all: the columns *are* text.

  Three things here are SQLite's own — `c:DMEx.Storage.SQL.dialect/1`,
  `c:DMEx.Storage.append/5`, which needs no lock, and
  `c:DMEx.Storage.validate!/1`. Every statement comes from `DMEx.Storage.SQL`,
  which says why.

  ## `append/5` takes no lock, because the engine is the lock

  `c:DMEx.Storage.append/5` covers why id assignment order must equal commit
  order. SQLite has one writer database-wide, so a second writer cannot execute
  its INSERT — cannot draw a rowid — until the first has committed, and the
  interleaving the Postgres adapter guards against is unrepresentable.
  `opts.lock` is unused either way.

  So a message with no entities is **one statement**, `INSERT ... RETURNING id`,
  which is its own implicit write transaction and needs nothing further.

  A message *with* entities is two, and the fourth contract on
  `c:DMEx.Storage.append/5` requires them to commit together — so that case takes
  an explicit transaction. It is the one below that makes this safe rather than a
  `SQLITE_BUSY_SNAPSHOT` waiting to happen: `:immediate` takes the write lock at
  `BEGIN`, so there is no read snapshot to promote, and `validate!/1` refuses a
  repo configured any other way. Both paths are kept, rather than one explicit
  transaction for everything, because the no-entity append is the common one and
  there is nothing for a transaction to buy it.

  ## The repo must be `default_transaction_mode: :immediate`

  `BEGIN DEFERRED` — ecto_sqlite3's default — starts a *read* transaction, and
  promoting it to a write returns `SQLITE_BUSY`, or in WAL mode
  `SQLITE_BUSY_SNAPSHOT`, which no busy handler can resolve because the read
  snapshot is already stale. Any caller wrapping `append/2` in a transaction of
  its own would hit it under concurrency. `:immediate` takes the write lock at
  `BEGIN`, leaving only the retryable case. `validate!/1` refuses a repo without
  it at boot, and `transaction/2` passes the mode itself.

  ## No `:prefix`

  SQLite has no schemas. `validate!/1` raises rather than ignoring one.
  """

  use DMEx.Storage.SQL

  alias DMEx.Storage.SQL
  alias Ecto.Adapters.SQL, as: Adapter

  # From the behaviour, not restated: see `DMEx.Storage.__table__/1`. Rendered to
  # strings here rather than left as the atoms that function answers with — an
  # atom interpolates to the same SQL, but `t:DMEx.Storage.SQL.dialect/0` says a
  # table entry is already-rendered SQL, and the Postgres adapter's quoting makes
  # that true on its side.
  @messages to_string(DMEx.Storage.__table__(:messages))
  @message_entities to_string(DMEx.Storage.__table__(:message_entities))
  @cursors to_string(DMEx.Storage.__table__(:cursors))
  @dead_messages to_string(DMEx.Storage.__table__(:dead_messages))

  # `strftime('%f')` renders `SS.SSS`, so every stored value is fixed-width and
  # comparable as bytes, which is what makes `updated_at` orderable at all —
  # SQLite has no timestamp type.
  @now "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"

  @doc """
  What this engine spells differently — `DMEx.Storage.SQL` has the vocabulary.

  No text projection and no JSON cast: the columns are text already, which is what
  `c:DMEx.Storage.read/5` asks for. Bytes are `length(CAST(x AS BLOB))` rather than
  `octet_length`, which needs SQLite 3.43 — exqlite can be built against the system
  library, so the portable spelling is the one that cannot be wrong.
  """
  @impl SQL
  def dialect(_opts) do
    %{
      messages: @messages,
      message_entities: @message_entities,
      cursors: @cursors,
      dead_messages: @dead_messages,
      param: "?",
      now: @now,
      text: &Function.identity/1,
      json: &Function.identity/1,
      bytes: &"length(CAST(#{&1} AS BLOB))",
      type_agg: "group_concat(DISTINCT type)",
      types: &String.split(&1, ","),
      timestamp: &datetime!/1,
      # No array type, so a selection is N placeholders binding N parameters, on
      # whichever column. The varying arity costs nothing: exqlite prepares every
      # statement anyway.
      selection: &{"#{&1} IN (#{placeholders(&2, &3)})", &2}
    }
  end

  # Public so `DMEx.Storage.SQLite.Migration` stamps its defaults with the same
  # expression a cursor write uses — the counterpart to
  # `DMEx.Storage.Postgres.__table__/2`, shared for the same reason.
  @doc false
  @spec __now__() :: String.t()
  def __now__, do: @now

  # Public so `DMEx.Storage.SQLite.Migration` refuses a `:prefix` in the same
  # words: one rule, whichever half of DMEx is asked to honour it — the same
  # move `__now__/0` makes for the timestamp format.
  @doc false
  @spec __refuse_prefix__(String.t() | nil) :: :ok
  def __refuse_prefix__(nil), do: :ok

  def __refuse_prefix__(prefix) do
    raise ArgumentError,
          "SQLite has no schemas, so #{inspect(__MODULE__)} cannot honour " <>
            "`prefix: #{inspect(prefix)}`. Drop it, or give this database a file " <>
            "of its own — which is what a schema would have bought."
  end

  # A `%DateTime{}` rather than the stored binary, because `t:DMEx.Storage.row/0`
  # says so.
  defp datetime!(text) when is_binary(text) do
    {:ok, at, 0} = DateTime.from_iso8601(text)

    at
  end

  @impl DMEx.Storage
  def validate!(opts) do
    __refuse_prefix__(Map.get(opts, :prefix))

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

  ## Appending

  @impl DMEx.Storage
  def append(type, data, metadata, [], opts) when is_binary(type) and is_binary(data) and is_binary(metadata) do
    sql = SQL.append_sql(dialect(opts))

    # One statement, so SQLite wraps it in an implicit write transaction of its
    # own — see the module documentation for why that is the whole locking story.
    case Adapter.query(Map.fetch!(opts, :repo), sql, [type, data, metadata]) do
      {:ok, %{rows: [[id]]}} -> {:ok, id}
      {:error, reason} -> {:error, reason}
    end
  end

  def append(type, data, metadata, entities, opts)
      when is_binary(type) and is_binary(data) and is_binary(metadata) and is_list(entities) do
    repo = Map.fetch!(opts, :repo)
    dialect = dialect(opts)

    # Two statements, so the implicit transaction is no longer the whole story:
    # the entity rows have to commit with the message. `:immediate` is what keeps
    # the explicit one safe — see the module documentation.
    repo.transaction(
      fn ->
        with {:ok, %{rows: [[id]]}} <-
               Adapter.query(repo, SQL.append_sql(dialect), [type, data, metadata]),
             {:ok, _result} <-
               Adapter.query(repo, SQL.entities_sql(dialect, length(entities)), [id | entities]) do
          id
        else
          {:error, reason} -> repo.rollback(reason)
        end
      end,
      mode: :immediate
    )
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

  # From `first` onward: a read binds three parameters before its selection, an
  # ad-hoc select none.
  defp placeholders(types, first), do: Enum.map_join(first..(first + length(types) - 1)//1, ",", &"?#{&1}")
end
