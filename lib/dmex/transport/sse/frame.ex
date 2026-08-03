defmodule DMEx.Transport.SSE.Frame do
  @moduledoc """
  The SSE wire format: one frame per log row.

  Three slots carry the row and there is no envelope:

      id: 1041/1042
      event: MyApp.Messages.OrderShipped
      data: {"data":{"order_id":7},"metadata":{"trace_id":"abc"}}

  Nothing DMEx owns rides at the top level of `data:`. A log wanting its own
  columns on the wire lifts them into `metadata` with `metadata:` on
  `use DMEx.Log`, which happens before a row reaches here — see
  `DMEx.Log.merge_metadata/2`.

  The column-to-slot table is in ARCHITECTURE.md's wire contract, along with why
  `id:` is a `previous/current` link rather than a number. `DMEx.Log.Params`
  validates the `Last-Event-ID` a client echoes it back as.

  ## Encoding

  Frames are iodata, and rows are **spliced, never re-encoded**: `data` and
  `metadata` go in as text, exactly as the read returned them. Decoding `jsonb`
  into a map and re-encoding would pay a decode plus an encode per row, once per
  connection for the same rows.

  Two invariants keep that safe, and they are the whole of the encoding
  contract: a frame is **never pretty-printed**, and **nothing is spliced in
  that did not come out of a JSON encoder, a JSON-typed column, or a column the
  schema constrains to a safe charset**. Together they guarantee no raw CR or LF
  reaches the wire, which is what makes it safe to put a whole payload on one
  `data:` line with no escaping pass.

  `message_type` is the constrained-charset case: it goes straight onto the
  `event:` line, and SSE field lines have no escaping. `dmex_messages` carries a
  CHECK restricting it to `[A-Za-z0-9_.]`, so a type that cannot be written on an
  `event:` line cannot be appended. See `DMEx.Storage.Migration`.
  """

  @typedoc "A log row on the log's side, as `c:DMEx.Storage.read/5` returns it."
  @type row :: DMEx.Storage.row()

  @typedoc """
  A log row on the reader's side, as `parse/1` yields it.

  String-keyed and one level decoded; turning `data` into a message struct is
  the message module's job, later, in the pipeline.
  """
  @type decoded_row :: DMEx.Handler.Pipeline.row()

  @typedoc """
  One parsed frame block.

  `:message` is the only kind that carries a row. `:retry` is the producer's
  advertised interval. `:ignore` is everything else: a heartbeat comment, a
  block with an empty `data:` buffer, a field vocabulary this does not know.
  """
  @type parsed :: {:message, decoded_row()} | {:retry, pos_integer()} | :ignore

  @type error :: {:invalid_json, term()} | {:invalid_data_shape, term()}

  @doc """
  Encodes one log row as one frame, as iodata.

  `:data` and `:metadata` are spliced verbatim and must therefore already be
  JSON text.

  `id:` carries `previous_id/id`; a `nil` predecessor renders as `0`.
  """
  @spec encode_row(row()) :: iodata()
  def encode_row(row) do
    [
      "id: ",
      Integer.to_string(row.previous_id || 0),
      ?/,
      Integer.to_string(row.id),
      "\nevent: ",
      row.message_type,
      "\ndata: {\"data\":",
      row.data,
      ",\"metadata\":",
      row.metadata,
      "}\n\n"
    ]
  end

  @doc """
  Encodes a lone `retry:` frame, sent at stream open.

  Dispatches nothing: an SSE parser applies `retry:` the moment it parses the
  line, while an event is dispatched only if the data buffer is non-empty when
  the blank line arrives.
  """
  @spec encode_retry(pos_integer()) :: iodata()
  def encode_retry(ms) when is_integer(ms) and ms > 0 do
    ["retry: ", Integer.to_string(ms), "\n\n"]
  end

  @doc """
  Encodes a heartbeat, as an SSE comment frame.

  Comment lines are ignored by every spec-compliant SSE client, so heartbeats
  never pollute the message stream or require consumer-side handling.
  """
  @spec encode_heartbeat() :: iodata()
  def encode_heartbeat, do: ": hb\n\n"

  @doc """
  Parses one complete frame block — the field lines between blank lines, without
  the terminating blank line itself.

  A block is a message only if it carries **all three** of `id:`, `event:` and a
  non-empty `data:`. Anything short of that is `:ignore` rather than an error:
  per the SSE specification an event is dispatched only when the data buffer is
  non-empty, and a producer is free to send comments and bare fields.

  `id:` parses in both forms — `1041/1042` yields `"previous_id" => 1041`, a
  bare `1042` yields `nil`, which a consumer refuses to accept.
  """
  @spec parse(binary()) :: {:ok, parsed()} | {:error, error()}
  def parse(block) when is_binary(block) do
    fields =
      block
      |> split_lines()
      |> Enum.reduce(%{data: []}, &collect_field/2)

    case fields do
      %{id: id, event: event, data: [_ | _] = lines} ->
        # SSE joins consecutive `data:` lines with LF. DMEx always emits one,
        # but a generic producer may not.
        lines |> Enum.reverse() |> Enum.join("\n") |> decode_row(id, event)

      %{retry: retry} ->
        {:ok, {:retry, retry}}

      _other ->
        {:ok, :ignore}
    end
  end

  defp decode_row(payload, {previous_id, id}, event) do
    case JSON.decode(payload) do
      {:ok, %{"data" => _} = decoded} ->
        row = %{"id" => id, "previous_id" => previous_id, "message_type" => event}

        {:ok, {:message, Map.merge(decoded, row)}}

      {:ok, other} ->
        {:error, {:invalid_data_shape, other}}

      {:error, reason} ->
        {:error, {:invalid_json, reason}}
    end
  end

  defp split_lines(block), do: String.split(block, ["\r\n", "\n", "\r"])

  defp collect_field(line, acc) do
    case parse_line(line) do
      {"data", value} -> Map.update!(acc, :data, &[value | &1])
      {"id", value} -> put_id(acc, value)
      {"retry", value} -> put_integer(acc, :retry, value)
      {"event", value} -> Map.put(acc, :event, value)
      _other -> acc
    end
  end

  # The value is everything after the colon, minus one optional leading space.
  defp parse_line(":" <> _comment), do: :comment

  defp parse_line(line) do
    case :binary.split(line, ":") do
      [field, " " <> value] -> {field, value}
      [field, value] -> {field, value}
      [""] -> :blank
      [field] -> {field, ""}
    end
  end

  defp put_id(acc, value) do
    case String.split(value, "/", parts: 2) do
      [current] ->
        put_pair(acc, nil, current)

      [previous, current] ->
        case Integer.parse(previous) do
          {previous, ""} -> put_pair(acc, previous, current)
          _ -> acc
        end
    end
  end

  defp put_pair(acc, previous, current) do
    case Integer.parse(current) do
      {id, ""} -> Map.put(acc, :id, {previous, id})
      _ -> acc
    end
  end

  defp put_integer(acc, key, value) do
    case Integer.parse(value) do
      {int, ""} -> Map.put(acc, key, int)
      _ -> acc
    end
  end
end
