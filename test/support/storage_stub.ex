defmodule DMEx.Test.Storage do
  @moduledoc """
  An in-memory `DMEx.Storage` for tests that have no business touching Postgres.

  Everything on the producer's HTTP path — header parsing, the send loop, frame
  shaping — is logic over what a read returned, so testing it against a real
  database buys nothing and costs `async: true`. This stub lets those tests run
  in parallel with no sandbox at all.

  It also records every read, which is how the loop tests assert things a real
  adapter could only hint at: that an empty read sends no frame, that the cursor
  advances to the last id sent, and that `max_batch_size` really is the limit.
  """

  @behaviour DMEx.Storage

  defstruct rows: [], reads: [], appends: [], cursors: %{}, dead: [], fail: nil

  @doc """
  Starts a stub and returns the `t:DMEx.Storage.opts/0` map addressing it.

  `rows` may be a list of `t:DMEx.Storage.row/0` maps, or an integer count of
  generated rows.
  """
  @spec new(term()) :: DMEx.Storage.opts()
  def new(rows \\ []) do
    {:ok, pid} = Agent.start_link(fn -> %__MODULE__{rows: rows(rows)} end)

    %{repo: __MODULE__, store: pid, lock: {0, 0}, channel: "dmex_stub", prefix: nil}
  end

  @doc "Builds `count` rows, ids 1..count, with predictable payloads."
  @spec rows(term()) :: [DMEx.Storage.row()]
  def rows(count) when is_integer(count), do: Enum.map(1..count//1, &row/1)
  def rows(rows) when is_list(rows), do: rows

  @doc "One row with the given id."
  @spec row(pos_integer(), keyword()) :: DMEx.Storage.row()
  def row(id, opts \\ []) do
    %{
      id: id,
      message_type: Keyword.get(opts, :message_type, "DMEx.Test.Messages.OrderPlaced"),
      data: Keyword.get(opts, :data, ~s({"order_id":#{id}})),
      metadata: Keyword.get(opts, :metadata, "{}"),
      inserted_at: Keyword.get(opts, :inserted_at, ~U[2026-07-27 10:31:04.221Z])
    }
  end

  @doc "Replaces the stub's rows."
  @spec put_rows(DMEx.Storage.opts(), [DMEx.Storage.row()] | non_neg_integer()) :: :ok
  def put_rows(opts, rows), do: update(opts, &%{&1 | rows: rows(rows)})

  @doc "Appends rows to the stub, as a publish would."
  @spec add_rows(DMEx.Storage.opts(), [DMEx.Storage.row()]) :: :ok
  def add_rows(opts, rows), do: update(opts, &%{&1 | rows: &1.rows ++ rows})

  @doc "Every `{cursor, limit}` pair `read/3` has been called with, oldest first."
  @spec reads(DMEx.Storage.opts()) :: [{non_neg_integer(), pos_integer()}]
  def reads(opts), do: opts |> get() |> Map.fetch!(:reads) |> Enum.reverse()

  @doc "Every `{type, data, metadata}` triple `append/4` has been called with."
  @spec appends(DMEx.Storage.opts()) :: [{String.t(), binary(), binary()}]
  def appends(opts), do: opts |> get() |> Map.fetch!(:appends) |> Enum.reverse()

  @doc "Every row written by `dead_message/2`."
  @spec dead_messages(DMEx.Storage.opts()) :: [map()]
  def dead_messages(opts), do: opts |> get() |> Map.fetch!(:dead) |> Enum.reverse()

  @doc "Makes the next call to `fun` return `{:error, reason}`."
  @spec fail(DMEx.Storage.opts(), atom(), term()) :: :ok
  def fail(opts, fun, reason \\ :boom), do: update(opts, &%{&1 | fail: {fun, reason}})

  ## DMEx.Storage

  @impl DMEx.Storage
  def append(opts, type, data, metadata) do
    Agent.get_and_update(store(opts), fn state ->
      id = length(state.rows) + 1
      row = row(id, message_type: type, data: data, metadata: metadata)

      {{:ok, id},
       %{state | rows: state.rows ++ [row], appends: [{type, data, metadata} | state.appends]}}
    end)
  end

  @impl DMEx.Storage
  def read(opts, cursor, limit) do
    Agent.get_and_update(store(opts), fn state ->
      do_read(%{state | reads: [{cursor, limit} | state.reads]}, cursor, limit)
    end)
  end

  defp do_read(%{fail: {:read, reason}} = state, _cursor, _limit) do
    {{:error, reason}, %{state | fail: nil}}
  end

  defp do_read(state, cursor, limit) do
    rows = state.rows |> Enum.filter(&(&1.id > cursor)) |> Enum.take(limit)
    max_id = if rows == [], do: nil, else: max_id_of(state.rows)

    {{:ok, {rows, max_id}}, state}
  end

  @impl DMEx.Storage
  def max_id(opts) do
    Agent.get(store(opts), fn state -> {:ok, max_id_of(state.rows)} end)
  end

  @impl DMEx.Storage
  def last_seen(opts, handler) do
    Agent.get(store(opts), fn state -> {:ok, Map.get(state.cursors, handler)} end)
  end

  @impl DMEx.Storage
  def cursor(opts, handler) do
    Agent.get(store(opts), fn state ->
      case Map.get(state.cursors, handler) do
        nil -> {:ok, nil}
        id -> {:ok, %{last_seen: id, producer_url: "stub", updated_at: nil}}
      end
    end)
  end

  @impl DMEx.Storage
  def delete_cursor(opts, handler) do
    update(opts, &%{&1 | cursors: Map.delete(&1.cursors, handler)})
  end

  @impl DMEx.Storage
  def transaction(_opts, fun), do: {:ok, fun.()}

  @impl DMEx.Storage
  def advance(opts, handler, id, _url) do
    update(opts, &%{&1 | cursors: Map.put(&1.cursors, handler, id)})
  end

  @impl DMEx.Storage
  def dead_message(opts, attrs), do: update(opts, &%{&1 | dead: [attrs | &1.dead]})

  @impl DMEx.Storage
  def dead_messages(opts, handler) do
    Agent.get(store(opts), fn state ->
      rows =
        state.dead
        |> Enum.reverse()
        |> Enum.filter(&(&1.handler == handler))
        |> Enum.map(&Map.take(&1, [:id, :message_type, :data, :metadata, :inserted_at]))

      {:ok, rows}
    end)
  end

  @impl DMEx.Storage
  def delete_dead_message(opts, handler, id) do
    update(opts, fn state ->
      %{state | dead: Enum.reject(state.dead, &(&1.handler == handler and &1.id == id))}
    end)
  end

  # An in-memory log has nothing to connect to and nothing to notify through —
  # the same answers a SQLite adapter gives.
  @impl DMEx.Storage
  def child_specs(_opts), do: []

  @impl DMEx.Storage
  def listen(_opts), do: :unsupported

  ## Internals

  defp store(%{store: pid}), do: pid

  defp get(opts), do: Agent.get(store(opts), & &1)

  defp update(opts, fun) do
    Agent.update(store(opts), fun)
  end

  defp max_id_of([]), do: 0
  defp max_id_of(rows), do: rows |> Enum.map(& &1.id) |> Enum.max()
end
