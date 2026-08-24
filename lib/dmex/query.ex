defmodule DMEx.Query do
  @moduledoc """
  Reads a log by message module, for a console or an operator script.

      DMEx.Query.all(MyApp.Messages.OrderPlaced, repo: MyApp.Repo)
      #=> {:ok, [%MyApp.Messages.OrderPlaced{order_id: 7}]}

  The module *is* the filter: its `c:DMEx.Message.type/0` and every type it
  retired, so a query survives a rename that a hand-written `type = '…'` would
  not. What comes back is cast structs, through the same `c:DMEx.Message.cast/1`
  a subscription's handler receives — there is no second decode path.

  ## Not a consumption path

  No cursor is read or written, nothing is retried, and a row comes back every
  time it is asked for. A consumer is `use DMEx.Handler`; this is for looking.

  ## Options

    * `:repo` — required, plus optional `:storage` and `:prefix`. See
      `DMEx.Supervisor.storage!/1`.
    * `:limit` — how many rows at most. Defaults to `default_limit/0`, because an
      unbounded read of an append-only log is a memory incident waiting for the
      log to grow.
    * `:order` — `:asc` (default) for oldest first, `:desc` for newest first.
      Ordering is on `id` and only on `id`: it is the only column that is dense,
      unique and ascending with commit order, which is what makes a page stable.
    * `:after_id` — exclusive lower bound on `id`, whichever way `:order` points.
      With `:desc` and no `:after_id` you get the newest rows, which is the
      common case; paging *backwards* through history would need a `:before_id`
      that does not exist yet.
  """

  alias DMEx.Config
  alias DMEx.Storage

  @query_opts [:limit, :order, :after_id]
  @storage_opts [:repo, :storage, :prefix]

  # Not a `DMEx.Config` setting: that vocabulary is a log's and a handler's, and
  # this number belongs to neither.
  @default_limit 100

  @doc "The `:limit` a query gets when it names none."
  @spec default_limit() :: pos_integer()
  def default_limit, do: @default_limit

  @doc """
  Every row of `message`'s types, cast into structs.

  `{:error, reason}` for a storage failure, the tuple every read in DMEx answers
  with. Raises for a module that is not a `DMEx.Message`, for an option this does
  not take, and for an adapter with no `c:DMEx.Storage.select/2`.
  """
  @spec all(module(), keyword()) :: {:ok, [struct()]} | {:error, term()}
  def all(message, opts \\ []) when is_atom(message) and is_list(opts) do
    Config.keys!(
      opts,
      @query_opts ++ @storage_opts,
      "`DMEx.Query.all/2`",
      " A query is `DMEx.Query.all(MyApp.Messages.OrderPlaced, repo: MyApp.Repo)`."
    )

    storage = DMEx.Supervisor.storage!(opts)

    with {:ok, rows} <- Storage.select(storage, filters!(message, opts)) do
      # `decode/2`, not `decode/1`: the module is in hand — it is what the type
      # filter was built from — so resolving the row's type through the node's
      # index would add a dependency on that index being current for nothing.
      {:ok,
       Enum.map(rows, fn row ->
         {:ok, cast, _metadata} = DMEx.Message.decode(row, message)
         cast
       end)}
    end
  end

  # `declared/1` rather than the two calls behind it, because a retired type names
  # rows this module still answers for and `DMEx.Message` owns that pairing.
  defp filters!(message, opts) do
    if !DMEx.Message.message?(message) do
      raise ArgumentError,
            "#{inspect(message)} is not a DMEx message module, so there is no type to query " <>
              "for. Add `use DMEx.Message` to it."
    end

    %{
      types: DMEx.Message.declared(message),
      after_id: bound!(opts, :after_id, &(is_integer(&1) and &1 >= 0), "a non-negative integer") || 0,
      order: bound!(opts, :order, &(&1 in [:asc, :desc]), "either :asc or :desc") || :asc,
      limit: bound!(opts, :limit, &(is_integer(&1) and &1 > 0), "a positive integer") || @default_limit
    }
  end

  # Checked here rather than left to the driver: `:order` reaches a statement as
  # SQL rather than as a parameter, and `:after_id` given as a string compares as
  # text on one engine and fails on the other. One message beats two driver errors.
  defp bound!(opts, key, valid?, expected) do
    case Keyword.get(opts, key) do
      nil -> nil
      value -> if valid?.(value), do: value, else: raise_bound(key, value, expected)
    end
  end

  defp raise_bound(key, value, expected) do
    raise ArgumentError, "`#{key}:` must be #{expected}, got: #{inspect(value)}"
  end
end
