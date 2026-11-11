defmodule DMEx.Query do
  @moduledoc """
  Reads a log by message type and by entity, for a console or an operator script.

      DMEx.Query.all(MyApp.Messages.OrderPlaced, repo: MyApp.Repo)
      #=> {:ok, [%MyApp.Messages.OrderPlaced{order_id: "0191f3c8-…"}]}

      DMEx.Query.all(:all, entities: ["0191f3c8-…"])
      #=> {:ok, [%OrderPlaced{…}, %PaymentCaptured{…}, %OrderShipped{…}]}

  Two axes, and they compose. A **module** is a type filter: its
  `c:DMEx.Message.type/0` and every type it retired, so a query survives a rename
  that a hand-written `type = '…'` would not. An **entity** is an identifier the
  appending message declared in its `entities:` — the question "everything
  concerning this order", which spans message types and so needs
  `:all` or a list of modules rather than one.

  What comes back is cast structs, through the same `c:DMEx.Message.cast/1` a
  subscription's handler receives — there is no second decode path.

  ## Not a consumption path

  No cursor is read or written, nothing is retried, and a row comes back every
  time it is asked for. A consumer is `use DMEx.Handler`; this is for looking.

  ## The first argument

    * a **module** — that message type and its retired ones. Decoding resolves
      through the module in hand, so it needs nothing of the node's type index.
    * a **list of modules** — their types, unioned.
    * **`:all`** — every type. Each row's own `type` resolves its module, and a
      type this node has no module for is **skipped**: a reader legitimately sees
      a log wider than its own message modules, which is the stance
      `DMEx.Message.decode/1` already takes.

  ## Options

    * `:entities` — one identifier or a list, admitting a row that carries **any**
      of them. Omitted, the entity axis is not narrowed at all. An empty list
      admits nothing and answers `{:ok, []}` without a query.
    * `:repo` — required unless `config :dmex, repo:` names one, plus optional
      `:storage` and `:prefix`. See `DMEx.Supervisor.storage!/1`.
    * `:limit` — how many rows at most. Defaults to `default_limit/0`, because an
      unbounded read of an append-only log is a memory incident waiting for the
      log to grow. For `stream/2` it is the page size instead.
    * `:order` — `:asc` (default) for oldest first, `:desc` for newest first.
      Ordering is on `id` and only on `id`: it is the only column that is dense,
      unique and ascending with commit order, which is what makes a page stable.
    * `:after_id` — exclusive lower bound on `id`, whichever way `:order` points.
      With `:desc` and no `:after_id` you get the newest rows, which is the
      common case; paging *backwards* through history would need a `:before_id`
      that does not exist yet.
  """

  alias DMEx.Config
  alias DMEx.Message
  alias DMEx.Storage

  @query_opts [:limit, :order, :after_id, :entities]
  @storage_opts [:repo, :storage, :prefix]

  # Not a `DMEx.Config` setting: that vocabulary is a log's and a handler's, and
  # this number belongs to neither.
  @default_limit 100

  @hint " A query is `DMEx.Query.all(MyApp.Messages.OrderPlaced, repo: MyApp.Repo)`, " <>
          "or `DMEx.Query.all(:all, entities: [order_id])` for one entity's history across types."

  @doc "The `:limit` a query gets when it names none."
  @spec default_limit() :: pos_integer()
  def default_limit, do: @default_limit

  @typedoc "What a query filters types by. See *The first argument*."
  @type selection :: module() | [module()] | :all

  @doc """
  Every matching row, cast into structs.

  `{:error, reason}` for a storage failure, the tuple every read in DMEx answers
  with. Raises for a module that is not a `DMEx.Message`, for an option this does
  not take, and for an adapter with no `c:DMEx.Storage.select/2`.
  """
  @spec all(selection(), keyword()) :: {:ok, [struct()]} | {:error, term()}
  def all(messages, opts \\ []) when is_list(opts) do
    Config.keys!(opts, @query_opts ++ @storage_opts, "`DMEx.Query.all/2`", @hint)

    storage = DMEx.Supervisor.storage!(opts)

    with {:ok, rows} <- Storage.select(storage, filters!(messages, opts)) do
      {:ok, cast(rows, messages)}
    end
  end

  @doc """
  The same query as a lazy `Stream`, paging past `:limit` rather than stopping at
  it.

  What `:limit` bounds here is one page — the rows held in memory at once —
  which is what makes reading a whole entity's history out of a log of any size
  an ordinary thing to do. Paging is **keyset**, on the `id` the previous page
  ended at, so it costs one index seek per page and cannot skip or repeat a row
  the way an `OFFSET` can.

  `:asc` only. Paging backwards needs the `:before_id` that does not exist, and
  a `:desc` stream would otherwise walk forwards while claiming to walk back.

  A storage failure **raises** rather than answering `{:error, reason}`: a stream
  already in flight has nowhere to put a tuple. Use `all/2` where the error is
  worth handling.
  """
  @spec stream(selection(), keyword()) :: Enumerable.t()
  def stream(messages, opts \\ []) when is_list(opts) do
    Config.keys!(opts, @query_opts ++ @storage_opts, "`DMEx.Query.stream/2`", @hint)

    storage = DMEx.Supervisor.storage!(opts)
    filters = filters!(messages, opts)

    if filters.order == :desc do
      raise ArgumentError,
            "`DMEx.Query.stream/2` reads oldest first, so `order: :desc` has nothing to page " <>
              "into: a backwards page needs a `:before_id` bound DMEx does not have. Use " <>
              "`all/2` with `order: :desc` for the newest rows."
    end

    Stream.resource(
      fn -> filters.after_id end,
      &page(storage, filters, messages, &1),
      & &1
    )
  end

  defp page(storage, filters, messages, after_id) do
    case Storage.select(storage, %{filters | after_id: after_id}) do
      {:ok, []} ->
        {:halt, after_id}

      {:ok, rows} ->
        # The last row's id, not the last *cast struct* — a skipped unknown type
        # must still advance the page, or the stream repeats it forever.
        {cast(rows, messages), List.last(rows).id}

      {:error, reason} ->
        raise "DMEx.Query.stream/2 failed to read the log: #{inspect(reason)}"
    end
  end

  # `decode/2` when the module is in hand — it is what the type filter was built
  # from, so resolving the row's type through the node's index would add a
  # dependency on that index being current for nothing. Otherwise `decode/1`,
  # which resolves it and answers `:unknown` for a type this node cannot decode.
  defp cast(rows, module) when is_atom(module) and module != :all do
    Enum.map(rows, fn row ->
      {:ok, cast, _metadata} = Message.decode(row, module)
      cast
    end)
  end

  defp cast(rows, _messages) do
    Enum.flat_map(rows, fn row ->
      case Message.decode(row) do
        {:ok, cast, _metadata} -> [cast]
        :unknown -> []
      end
    end)
  end

  defp filters!(messages, opts) do
    %{
      types: types!(messages),
      entities: entities!(opts),
      after_id: bound!(opts, :after_id, &(is_integer(&1) and &1 >= 0), "a non-negative integer") || 0,
      order: bound!(opts, :order, &(&1 in [:asc, :desc]), "either :asc or :desc") || :asc,
      limit: bound!(opts, :limit, &(is_integer(&1) and &1 > 0), "a positive integer") || @default_limit
    }
  end

  defp types!(:all), do: :all

  # `declared/1` rather than the two calls behind it, because a retired type names
  # rows this module still answers for and `DMEx.Message` owns that pairing.
  defp types!(module) when is_atom(module), do: Message.declared(message!(module))

  # Unioned rather than checked for overlap: two modules cannot declare one type
  # — `DMEx.Message.index!/1` refuses that node-wide — so a duplicate here could
  # only be the same module named twice, which says nothing twice.
  defp types!([]) do
    raise ArgumentError,
          "`DMEx.Query` was given an empty list of message modules, which admits nothing. " <>
            "Pass `:all` to query every type." <> @hint
  end

  defp types!(modules) when is_list(modules) do
    modules |> Enum.flat_map(&Message.declared(message!(&1))) |> Enum.uniq()
  end

  defp types!(other) do
    raise ArgumentError,
          "`DMEx.Query` takes a message module, a list of them, or `:all`, got: #{inspect(other)}" <>
            @hint
  end

  defp message!(module) when is_atom(module) do
    if !Message.message?(module) do
      raise ArgumentError,
            "#{inspect(module)} is not a DMEx message module, so there is no type to query " <>
              "for. Add `use DMEx.Message` to it."
    end

    module
  end

  defp message!(other) do
    raise ArgumentError, "`DMEx.Query` takes message modules, got: #{inspect(other)}"
  end

  # Absent is `:all` rather than `[]`: an unnarrowed axis and one narrowed to
  # nothing are opposite answers, and `c:DMEx.Storage.select/2` renders them as
  # different statements.
  #
  # One entity is wrapped rather than handled: both shapes then go through
  # `entity!/1`, so there is no check a single identifier can skip by not being
  # in a list. An unchecked `entities: ""` would have reached SQL as a predicate
  # that silently matches nothing.
  defp entities!(opts) do
    case Keyword.get(opts, :entities) do
      nil -> :all
      entities when is_list(entities) -> Enum.map(entities, &entity!/1)
      entity -> [entity!(entity)]
    end
  end

  defp entity!(entity) when is_binary(entity) and entity != "", do: entity
  defp entity!(other), do: raise_entities(other)

  defp raise_entities(value) do
    raise ArgumentError,
          "`entities:` takes an entity identifier or a list of them, as non-empty strings, " <>
            "got: #{inspect(value)}. An identifier is stored exactly as the appending message " <>
            "declared it, so it is compared as a string and never coerced."
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
