defmodule DMEx.Test.Storage do
  @moduledoc """
  An in-memory `DMEx.Storage` for tests that have no business touching Postgres.

  Everything on the log's HTTP path — header parsing, the send loop, frame
  shaping — is logic over what a read returned, so testing it against a real
  database buys nothing and costs `async: true`. This stub lets those tests run
  in parallel with no sandbox at all.

  It also records every read, which is how the loop tests assert things a real
  adapter could only hint at: that an empty read sends no frame, that the cursor
  advances to the last id sent, and that `max_batch_size` really is the limit.
  """

  @behaviour DMEx.Storage

  defstruct rows: [],
            reads: [],
            read_bytes: [],
            appends: [],
            cursors: %{},
            dead: [],
            fail: nil,
            hang: nil

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
      # `read/5` overwrites this with the real predecessor in the delivered
      # sequence; a row built directly is assumed to follow the one before it.
      previous_id: Keyword.get(opts, :previous_id, id - 1),
      message_type: Keyword.get(opts, :message_type, "DMEx.Test.Messages.OrderPlaced"),
      data: Keyword.get(opts, :data, ~s({"order_id":#{id}})),
      metadata: Keyword.get(opts, :metadata, "{}"),
      inserted_at: Keyword.get(opts, :inserted_at, ~U[2026-07-27 10:31:04.221Z])
    }
  end

  @doc "Replaces the stub's rows."
  @spec put_rows(DMEx.Storage.opts(), [DMEx.Storage.row()] | non_neg_integer()) :: :ok
  def put_rows(opts, rows), do: update(opts, &%{&1 | rows: rows(rows)})

  @doc "Appends rows to the stub, as an append would."
  @spec add_rows(DMEx.Storage.opts(), [DMEx.Storage.row()]) :: :ok
  def add_rows(opts, rows), do: update(opts, &%{&1 | rows: &1.rows ++ rows})

  @doc "Every `{cursor, limit, types}` triple `read/5` was called with, oldest first."
  @spec reads(DMEx.Storage.opts()) :: [
          {non_neg_integer(), pos_integer(), :all | [String.t()]}
        ]
  def reads(opts), do: opts |> get() |> Map.fetch!(:reads) |> Enum.reverse()

  @doc "The byte budget each read was given, oldest first."
  @spec read_bytes(DMEx.Storage.opts()) :: [pos_integer()]
  def read_bytes(opts), do: opts |> get() |> Map.fetch!(:read_bytes) |> Enum.reverse()

  @doc "Every `{type, data, metadata}` triple `append/2` has been handed, oldest first."
  @spec appends(DMEx.Storage.opts()) :: [{String.t(), binary(), binary()}]
  def appends(opts), do: opts |> get() |> Map.fetch!(:appends) |> Enum.reverse()

  @doc "Every row written by `dead_message/2`."
  @spec dead_messages(DMEx.Storage.opts()) :: [map()]
  def dead_messages(opts), do: opts |> get() |> Map.fetch!(:dead) |> Enum.reverse()

  @doc "Makes the next `read/5` return `{:error, reason}`. Only `:read` is honoured."
  @spec fail(DMEx.Storage.opts(), atom(), term()) :: :ok
  def fail(opts, fun, reason \\ :boom), do: update(opts, &%{&1 | fail: {fun, reason}})

  @doc """
  Makes every subsequent `read/5` block its caller forever. Only `:read` is
  honoured.

  This is the failure no transport-level timeout can see: a query that never
  comes back wedges the send loop with a healthy socket underneath it.
  """
  @spec hang(DMEx.Storage.opts(), atom()) :: :ok
  def hang(opts, fun), do: update(opts, &%{&1 | hang: fun})

  ## DMEx.Storage

  @impl DMEx.Storage
  def append(opts, entries) do
    Agent.get_and_update(store(opts), fn state ->
      {rows, ids} =
        entries
        |> Enum.with_index(length(state.rows) + 1)
        |> Enum.map(fn {{type, data, metadata}, id} ->
          {row(id, message_type: type, data: data, metadata: metadata), id}
        end)
        |> Enum.unzip()

      {{:ok, ids},
       %{
         state
         | rows: state.rows ++ rows,
           appends: Enum.reverse(entries) ++ state.appends
       }}
    end)
  end

  @impl DMEx.Storage
  def read(opts, cursor, limit, max_bytes, types) do
    # Before the agent call, so the caller wedges rather than the stub.
    maybe_hang(opts, :read)

    Agent.get_and_update(store(opts), fn state ->
      state = %{
        state
        | reads: [{cursor, limit, types} | state.reads],
          read_bytes: [max_bytes | state.read_bytes]
      }

      do_read(state, cursor, limit, max_bytes, types)
    end)
  end

  defp do_read(%{fail: {:read, reason}} = state, _cursor, _limit, _max_bytes, _types) do
    {{:error, reason}, %{state | fail: nil}}
  end

  defp do_read(state, cursor, limit, max_bytes, types) do
    rows =
      state.rows
      |> Enum.filter(&(&1.id > cursor and selected?(&1, types)))
      |> Enum.take(limit)
      |> cap_bytes(max_bytes)
      |> chain(cursor)

    {{:ok, rows}, state}
  end

  # What the adapter's `lag(id, 1, cursor)` does: each row states the id of the
  # row before it *in what this read returned*, and the first links back to the
  # cursor. Selective reads therefore chain over the delivered subsequence
  # rather than over the log's id space.
  defp chain(rows, cursor) do
    rows
    |> Enum.map_reduce(cursor, fn row, previous ->
      {%{row | previous_id: previous}, row.id}
    end)
    |> elem(0)
  end

  # The real adapter enforces this inside the query; a stub over a list does it
  # here, under the same rule — inclusive, so the total never exceeds the
  # budget, except that the first row always goes out however large it is.
  defp cap_bytes(rows, max_bytes) do
    rows
    |> Enum.reduce_while({[], 0}, fn row, {kept, prefix} ->
      if prefix == 0 or prefix + row_bytes(row) <= max_bytes do
        {:cont, {[row | kept], prefix + row_bytes(row)}}
      else
        {:halt, {kept, prefix}}
      end
    end)
    |> then(fn {kept, _prefix} -> Enum.reverse(kept) end)
  end

  defp row_bytes(row) do
    byte_size(row.data) + byte_size(row.metadata) + byte_size(row.message_type)
  end

  defp selected?(_row, :all), do: true
  defp selected?(row, types), do: row.message_type in types

  @impl DMEx.Storage
  def max_id(opts) do
    Agent.get(store(opts), fn state -> {:ok, max_id_of(state.rows)} end)
  end

  @impl DMEx.Storage
  def appended_since(opts, cursor) do
    Agent.get(store(opts), fn state ->
      case Enum.filter(state.rows, &(&1.id > cursor)) do
        [] -> {:ok, {nil, []}}
        rows -> {:ok, {max_id_of(rows), rows |> Enum.map(& &1.message_type) |> Enum.uniq()}}
      end
    end)
  end

  @impl DMEx.Storage
  def last_seen(opts, subscription) when is_binary(subscription) do
    Agent.get(store(opts), fn state -> {:ok, Map.get(state.cursors, subscription)} end)
  end

  @impl DMEx.Storage
  def cursor(opts, subscription) when is_binary(subscription) do
    Agent.get(store(opts), fn state ->
      case Map.get(state.cursors, subscription) do
        nil -> {:ok, nil}
        id -> {:ok, %{last_seen: id, updated_at: nil}}
      end
    end)
  end

  @impl DMEx.Storage
  def delete_cursor(opts, subscription) when is_binary(subscription) do
    update(opts, &%{&1 | cursors: Map.delete(&1.cursors, subscription)})
  end

  @impl DMEx.Storage
  def transaction(_opts, fun), do: {:ok, fun.()}

  @impl DMEx.Storage
  def advance(opts, subscription, id) when is_binary(subscription) do
    update(opts, &%{&1 | cursors: Map.put(&1.cursors, subscription, id)})
  end

  @impl DMEx.Storage
  def dead_message(opts, attrs), do: update(opts, &%{&1 | dead: [attrs | &1.dead]})

  @impl DMEx.Storage
  def dead_messages(opts, subscription) when is_binary(subscription) do
    Agent.get(store(opts), fn state ->
      rows =
        state.dead
        |> Enum.reverse()
        |> Enum.filter(&(&1.subscription == subscription))
        |> Enum.map(&Map.take(&1, [:id, :message_type, :data, :metadata, :inserted_at]))

      {:ok, rows}
    end)
  end

  @impl DMEx.Storage
  def delete_dead_message(opts, subscription, id) when is_binary(subscription) do
    update(opts, fn state ->
      %{
        state
        | dead: Enum.reject(state.dead, &(&1.subscription == subscription and &1.id == id))
      }
    end)
  end

  # An in-memory log has nothing to connect to and nothing to notify through.
  @impl DMEx.Storage
  def child_specs(_opts), do: []

  @impl DMEx.Storage
  def listen(_opts), do: :unsupported

  ## Internals

  # Read rather than matched on: every callback here is declared to take any
  # `t:DMEx.Storage.opts/0`, and a head matching `%{store: pid}` would narrow
  # this stub's functions to the opts *it* builds — which is exactly what
  # dialyzer reports as a behaviour mismatch.
  defp store(opts), do: Map.fetch!(opts, :store)

  defp get(opts), do: Agent.get(store(opts), & &1)

  defp maybe_hang(opts, fun) do
    if get(opts).hang == fun, do: Process.sleep(:infinity)

    :ok
  end

  defp update(opts, fun) do
    Agent.update(store(opts), fun)
  end

  defp max_id_of([]), do: 0
  defp max_id_of(rows), do: rows |> Enum.map(& &1.id) |> Enum.max()
end
