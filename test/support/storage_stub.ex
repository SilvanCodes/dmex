defmodule DMEx.Test.Storage do
  @moduledoc """
  An in-memory `DMEx.Storage` for tests that have no business touching Postgres.

  Everything on the log's HTTP path — header parsing, the send loop, frame
  shaping — is logic over what a read returned, so testing it against a real
  database buys nothing and costs `async: true`. This stub lets those tests run
  in parallel with no sandbox at all.

  It also records every read, which is how the loop tests assert things a real
  adapter could only hint at: that an empty read sends no frame, that the cursor
  advances to the last id sent, and that `max_batch_rows` really is the limit.
  """

  @behaviour DMEx.Storage

  defstruct rows: [],
            reads: [],
            read_bytes: [],
            cursors: %{},
            dead: [],
            fail: nil,
            fail_max_id: nil,
            hang: nil

  @doc """
  Starts a stub and returns the `t:DMEx.Storage.opts/0` map addressing it.

  `rows` may be a list of `t:DMEx.Storage.row/0` maps, or an integer count of
  generated rows.
  """
  @spec new(term()) :: DMEx.Storage.opts()
  def new(rows \\ []) do
    {:ok, pid} = Agent.start_link(fn -> %__MODULE__{rows: rows(rows)} end)

    %{repo: __MODULE__, store: pid, lock: {0, 0}, prefix: nil}
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
      type: Keyword.get(opts, :type, "order_placed"),
      data: Keyword.get(opts, :data, ~s({"order_id":#{id}})),
      metadata: Keyword.get(opts, :metadata, "{}"),
      inserted_at: Keyword.get(opts, :inserted_at, ~U[2026-07-27 10:31:04.221Z])
    }
  end

  @doc "Replaces the stub's rows."
  @spec put_rows([DMEx.Storage.row()] | non_neg_integer(), DMEx.Storage.opts()) :: :ok
  def put_rows(rows, opts), do: update(&%{&1 | rows: rows(rows)}, opts)

  @doc "Every `{cursor, max_rows, types}` triple `read/5` was called with, oldest first."
  @spec reads(DMEx.Storage.opts()) :: [
          {non_neg_integer(), pos_integer(), :all | [String.t()]}
        ]
  def reads(opts), do: opts |> get() |> Map.fetch!(:reads) |> Enum.reverse()

  @doc "The byte budget each read was given, oldest first."
  @spec read_bytes(DMEx.Storage.opts()) :: [pos_integer()]
  def read_bytes(opts), do: opts |> get() |> Map.fetch!(:read_bytes) |> Enum.reverse()

  @doc "Makes the next `read/5` return `{:error, reason}`."
  @spec fail(term(), DMEx.Storage.opts()) :: :ok
  def fail(reason \\ :boom, opts), do: update(&%{&1 | fail: reason}, opts)

  @doc """
  Makes every subsequent `read/5` block its caller forever.

  This is the failure no transport-level timeout can see: a query that never
  comes back wedges the send loop with a healthy socket underneath it.

  Both this and `fail/2` are scoped to `read/5` and take no callback name:
  reads are the only call these tests inject faults into, and an argument
  naming a callback that is never consulted is a silent no-op.
  """
  @spec hang(DMEx.Storage.opts()) :: :ok
  def hang(opts), do: update(&%{&1 | hang: true}, opts)

  @doc """
  Makes `max_id/1` return `{:error, reason}`, for as long as it is set.

  Its own switch rather than a callback argument to `fail/2`: this one is about
  the *connect* path, where the question is whether a failure to resolve the tip
  is fatal or silently becomes "from the beginning".
  """
  @spec fail_max_id(term(), DMEx.Storage.opts()) :: :ok
  def fail_max_id(reason \\ :boom, opts), do: update(&%{&1 | fail_max_id: reason}, opts)

  ## DMEx.Storage

  @impl DMEx.Storage
  def append(type, data, metadata, opts) do
    Agent.get_and_update(store(opts), fn state ->
      id = length(state.rows) + 1
      row = row(id, type: type, data: data, metadata: metadata)

      {{:ok, id}, %{state | rows: state.rows ++ [row]}}
    end)
  end

  @impl DMEx.Storage
  def read(cursor, max_rows, max_bytes, types, opts) do
    # Before the agent call, so the caller wedges rather than the stub.
    maybe_hang(opts)

    Agent.get_and_update(store(opts), fn state ->
      state = %{
        state
        | reads: [{cursor, max_rows, types} | state.reads],
          read_bytes: [max_bytes | state.read_bytes]
      }

      do_read(state, cursor, max_rows, max_bytes, types)
    end)
  end

  defp do_read(%{fail: reason} = state, _cursor, _limit, _max_bytes, _types) when reason != nil do
    {{:error, reason}, %{state | fail: nil}}
  end

  defp do_read(state, cursor, max_rows, max_bytes, types) do
    rows =
      state.rows
      |> Enum.filter(&(&1.id > cursor and selected?(&1, types)))
      |> Enum.take(max_rows)
      |> cap_bytes(max_bytes)

    {{:ok, rows}, state}
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
    byte_size(row.data) + byte_size(row.metadata) + byte_size(row.type)
  end

  defp selected?(_row, :all), do: true
  defp selected?(row, types), do: row.type in types

  @impl DMEx.Storage
  def max_id(opts) do
    Agent.get(store(opts), fn
      %{fail_max_id: reason} when reason != nil -> {:error, reason}
      state -> {:ok, max_id_of(state.rows)}
    end)
  end

  @impl DMEx.Storage
  def appended_since(cursor, opts) do
    Agent.get(store(opts), fn state ->
      case Enum.filter(state.rows, &(&1.id > cursor)) do
        [] -> {:ok, {nil, []}}
        rows -> {:ok, {max_id_of(rows), rows |> Enum.map(& &1.type) |> Enum.uniq()}}
      end
    end)
  end

  @impl DMEx.Storage
  def last_seen(subscription, opts) when is_binary(subscription) do
    Agent.get(store(opts), fn state -> {:ok, Map.get(state.cursors, subscription)} end)
  end

  @impl DMEx.Storage
  def cursor(subscription, opts) when is_binary(subscription) do
    Agent.get(store(opts), fn state ->
      case Map.get(state.cursors, subscription) do
        nil -> {:ok, nil}
        id -> {:ok, %{last_seen: id, updated_at: nil}}
      end
    end)
  end

  @impl DMEx.Storage
  def delete_cursor(subscription, opts) when is_binary(subscription) do
    update(&%{&1 | cursors: Map.delete(&1.cursors, subscription)}, opts)
  end

  @impl DMEx.Storage
  def transaction(fun, _opts), do: {:ok, fun.()}

  @impl DMEx.Storage
  def advance_cursor(subscription, last_seen, opts) when is_binary(subscription) do
    update(&%{&1 | cursors: Map.put(&1.cursors, subscription, last_seen)}, opts)
  end

  # A dead row arrives as the consumer held it, so this stores what a real
  # adapter's columns would: JSON text, plus the `:inserted_at` it stamps itself.
  @impl DMEx.Storage
  def record_dead_message(
        %{"subscription" => subscription, "id" => id, "type" => type, "data" => data, "metadata" => metadata},
        opts
      ) do
    dead = %{
      subscription: subscription,
      id: id,
      type: type,
      data: JSON.encode!(data),
      metadata: JSON.encode!(metadata),
      inserted_at: DateTime.utc_now()
    }

    update(&%{&1 | dead: [dead | &1.dead]}, opts)
  end

  @impl DMEx.Storage
  def list_dead_messages(subscription, opts) when is_binary(subscription) do
    Agent.get(store(opts), fn state ->
      rows =
        state.dead
        |> Enum.reverse()
        |> Enum.filter(&(&1.subscription == subscription))
        |> Enum.map(&Map.take(&1, [:id, :type, :data, :metadata, :inserted_at]))

      {:ok, rows}
    end)
  end

  @impl DMEx.Storage
  def delete_dead_message(subscription, id, opts) when is_binary(subscription) do
    update(
      fn state ->
        %{
          state
          | dead: Enum.reject(state.dead, &(&1.subscription == subscription and &1.id == id))
        }
      end,
      opts
    )
  end

  # An in-memory log has no database to notify through, so it takes the one that
  # needs none. Nothing starts it in these tests, which makes `notify/2` a no-op
  # and the dispatcher polling-only — exactly what a stub wants.
  @impl DMEx.Storage
  def default_notifier, do: DMEx.Notifier.Beam

  ## Internals

  # Read rather than matched on: every callback here is declared to take any
  # `t:DMEx.Storage.opts/0`, and a head matching `%{store: pid}` would narrow
  # this stub's functions to the opts *it* builds — which is exactly what
  # dialyzer reports as a behaviour mismatch.
  defp store(opts), do: Map.fetch!(opts, :store)

  defp get(opts), do: Agent.get(store(opts), & &1)

  defp maybe_hang(opts) do
    if get(opts).hang, do: Process.sleep(:infinity)

    :ok
  end

  defp update(fun, opts) do
    Agent.update(store(opts), fun)
  end

  defp max_id_of([]), do: 0
  defp max_id_of(rows), do: rows |> Enum.map(& &1.id) |> Enum.max()
end
