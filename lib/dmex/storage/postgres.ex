defmodule DMEx.Storage.Postgres do
  @moduledoc """
  The Postgres implementation of `DMEx.Storage`.

  Raw SQL through `Ecto.Adapters.SQL` rather than Ecto schemas: the `data::text`
  that `c:DMEx.Storage.read/5` requires cannot be expressed as a schema load.

  Three things here are Postgres's own — `c:DMEx.Storage.SQL.dialect/1`, `c:DMEx.Storage.append/4`,
  which serializes itself, and `c:DMEx.Storage.validate!/1`. Every statement comes
  from `DMEx.Storage.SQL`, which says why.

  ## The driver never touches `jsonb`

  Parameters go in as **`$n::text::jsonb`** and `data`/`metadata` come back as
  `column::text`, so Postgrex is never asked to encode or decode `jsonb` and
  DMEx never needs `config :postgrex, :json_library`. `c:DMEx.Storage.record_dead_message/2`
  is the one call handed terms rather than column text, and encodes them into the
  same text parameters, so the rule above still holds.

  The double cast is load-bearing and `$n::jsonb` is the trap: Postgres would
  describe the parameter as `jsonb`, at which point Postgrex encodes the binary
  through its own JSON codec and stores a *string* rather than an object. Going
  through `::text` pins the parameter and Postgres does the parse. The failure
  is silent, so a test asserts `jsonb_typeof(data) = 'object'`.
  """

  use DMEx.Storage.SQL

  alias DMEx.Storage.SQL
  alias Ecto.Adapters.SQL, as: Adapter

  # From the behaviour, not restated: see `DMEx.Storage.__table__/1`.
  @messages DMEx.Storage.__table__(:messages)
  @cursors DMEx.Storage.__table__(:cursors)
  @dead_messages DMEx.Storage.__table__(:dead_messages)

  # Postgres's own unquoted-identifier charset. `Regex.source/1` is what puts it
  # in the error message it enforces, so the pattern is compiled once here rather
  # than per boot.
  @prefix_charset ~r/^[A-Za-z_][A-Za-z0-9_$]*$/

  @doc """
  What this engine spells differently — `DMEx.Storage.SQL` has the vocabulary.

  `now()` rather than `clock_timestamp()`: these are the cursor and dead-message
  clocks, and only the log's own `inserted_at` has to ascend with `id`, which is
  the migration's default rather than anything issued here.
  """
  @impl SQL
  def dialect(opts) do
    %{
      messages: table(@messages, opts),
      cursors: table(@cursors, opts),
      dead_messages: table(@dead_messages, opts),
      param: "$",
      now: "now()",
      text: &"#{&1}::text",
      json: &"#{&1}::text::jsonb",
      bytes: &"octet_length(#{&1})",
      type_agg: "array_agg(DISTINCT type)",
      types: &Function.identity/1,
      timestamp: &Function.identity/1,
      # One array parameter, whatever the selection's size.
      selection: &{"AND type = ANY($#{&2})", [&1]}
    }
  end

  @impl DMEx.Storage
  def validate!(opts), do: validate_prefix!(Map.get(opts, :prefix))

  # A schema name reaches `__table__/2`'s quoted interpolation, so it is checked
  # rather than trusted: anything outside the charset would need quoting rules
  # DMEx is not going to invent, and a `"` would break out of the quoting it does.
  defp validate_prefix!(nil), do: :ok

  defp validate_prefix!(prefix) when is_binary(prefix) do
    if Regex.match?(@prefix_charset, prefix) do
      :ok
    else
      raise ArgumentError,
            "`prefix: #{inspect(prefix)}` is not a usable Postgres schema name. " <>
              "#{inspect(__MODULE__)} splices it into quoted identifiers, so it must " <>
              "match #{Regex.source(@prefix_charset)}."
    end
  end

  defp validate_prefix!(prefix) do
    raise ArgumentError,
          "`prefix:` must be a schema name as a string or nil, got: #{inspect(prefix)}"
  end

  ## Appending

  @impl DMEx.Storage
  def append(type, data, metadata, opts) when is_binary(type) and is_binary(data) and is_binary(metadata) do
    repo = Map.fetch!(opts, :repo)
    sql = SQL.append_sql(dialect(opts))

    repo.transaction(fn ->
      # First, and released only by COMMIT — `c:DMEx.Storage.append/4` says why
      # anything shorter reintroduces silent id gaps.
      lock!(repo, opts)

      # `query/3` rather than `query!/3`: `c:DMEx.Storage.append/4` answers with a
      # tuple, and `rollback/1` is what turns a row the database refused into one.
      case Adapter.query(repo, sql, [type, data, metadata]) do
        {:ok, %{rows: [[id]]}} -> id
        {:error, reason} -> repo.rollback(reason)
      end
    end)
  end

  defp lock!(repo, opts) do
    {classid, objid} = Map.fetch!(opts, :lock)

    Adapter.query!(repo, "SELECT pg_advisory_xact_lock($1, $2)", [classid, objid])
  end

  @impl DMEx.Storage
  def transaction(fun, opts) when is_function(fun, 0), do: Map.fetch!(opts, :repo).transaction(fun)

  @impl DMEx.Storage
  def default_notifier, do: DMEx.Notifier.Postgres

  @impl DMEx.Storage
  def migration, do: DMEx.Storage.Postgres.Migration

  ## Naming

  defp table(name, opts), do: __table__(name, Map.get(opts, :prefix))

  # Public so `DMEx.Storage.Postgres.Migration` renders qualified names the same
  # way the queries do.
  @doc false
  @spec __table__(String.t() | atom(), String.t() | nil) :: String.t()
  def __table__(name, nil), do: ~s("#{name}")
  def __table__(name, prefix), do: ~s("#{prefix}"."#{name}")
end
