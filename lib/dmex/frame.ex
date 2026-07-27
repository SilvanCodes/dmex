defmodule DMEx.Frame do
  @moduledoc """
  The SSE wire format: one struct per `text/event-stream` chunk.

  A frame models a *batch*, not a single message. Every data frame is a batch
  frame, including one carrying exactly a single message — it is simply a
  one-element `messages` list. That costs a couple of bytes on the wire and buys
  a single encode path, a single decode path, and no branch anywhere that could
  let batched and unbatched messages drift apart in behaviour.

      id: 1042
      event: batch
      data: {"metadata":{"max_id":1180},"messages":[{"id":1042,...}]}

  | field | SSE field | meaning |
  | --- | --- | --- |
  | `id` | `id:` | the **highest** message id in this frame |
  | `event` | `event:` | transport-level framing type, always `"batch"` for data frames |
  | `metadata` | inside `data:` | frame-level facts, currently `max_id` |
  | `messages` | inside `data:` | the frame's log rows, exactly as stored |
  | `retry` | `retry:` | the producer's effective heartbeat interval, in ms |

  SSE allows only one `id:` per frame, so the frame's `id:` is the highest
  message id it carries; per-message ids live inside `messages`. A DMEx consumer
  advances its cursor per message from that list, never from the frame's `id:`.
  The frame-level `id:` exists so a generic, non-DMEx SSE client resuming via
  `Last-Event-ID` lands on a correct position — never skipped, never re-read from
  the middle.

  ## Encoding

  Frames are built as iodata, and rows are **spliced, never re-encoded**: a row's
  `data` and `metadata` go in as text, exactly as the read returned them. The
  alternative — letting the driver decode `jsonb` into a map and re-encoding it —
  pays a decode plus an encode per row, and pays it once per connection for the
  same rows, since every connection issues its own read.

  Two invariants keep that safe, and they are the whole of the encoding contract:
  a frame is **never pretty-printed**, and **nothing is spliced into it that did
  not come out of a JSON encoder or a JSON-typed column**. Together they
  guarantee the payload contains no raw CR or LF — JSON's grammar requires
  U+0000–U+001F to be escaped inside strings — which is what makes it safe to put
  the whole payload on one `data:` line with no base64 and no escaping pass.
  """

  @type row :: %{
          required(:id) => pos_integer(),
          required(:message_type) => String.t(),
          required(:data) => iodata(),
          required(:metadata) => iodata(),
          required(:inserted_at) => DateTime.t() | NaiveDateTime.t()
        }

  @type t :: %__MODULE__{
          id: pos_integer() | nil,
          event: String.t() | nil,
          metadata: map() | nil,
          messages: [map()] | nil,
          retry: pos_integer() | nil
        }

  defstruct [:id, :event, :metadata, :messages, :retry]

  @batch_event "batch"

  @doc "The transport-level `event:` value carried by every data frame."
  @spec batch_event() :: String.t()
  def batch_event, do: @batch_event

  @doc """
  Encodes a batch frame as iodata.

  `rows` must be non-empty — a read returning nothing sends no frame at all, so
  encoding an empty batch is a bug rather than a valid degenerate case.

  Each row's `:data` and `:metadata` are spliced verbatim and must therefore
  already be JSON text. `:message_type` and `:inserted_at` are run through the
  JSON encoder, which is what keeps the "nothing unescaped is spliced" invariant
  true for them.
  """
  @spec encode_batch([row()], non_neg_integer()) :: iodata()
  def encode_batch([], _max_id) do
    raise ArgumentError, "cannot encode an empty batch frame; skip the frame instead"
  end

  def encode_batch(rows, max_id) when is_list(rows) and is_integer(max_id) do
    frame_id = rows |> Enum.map(& &1.id) |> Enum.max()

    [
      "id: ",
      Integer.to_string(frame_id),
      "\nevent: ",
      @batch_event,
      "\ndata: {\"metadata\":{\"max_id\":",
      Integer.to_string(max_id),
      "},\"messages\":[",
      rows |> Enum.map(&encode_row/1) |> Enum.intersperse(?,),
      "]}\n\n"
    ]
  end

  defp encode_row(row) do
    [
      "{\"id\":",
      Integer.to_string(row.id),
      ",\"message_type\":",
      JSON.encode_to_iodata!(row.message_type),
      ",\"data\":",
      row.data,
      ",\"metadata\":",
      row.metadata,
      ",\"inserted_at\":",
      row.inserted_at |> to_iso8601() |> JSON.encode_to_iodata!(),
      ?}
    ]
  end

  defp to_iso8601(%DateTime{} = at), do: DateTime.to_iso8601(at)
  defp to_iso8601(%NaiveDateTime{} = at), do: NaiveDateTime.to_iso8601(at)

  @doc """
  Encodes a lone `retry:` frame, sent at stream open.

  Well-defined and dispatches nothing: an SSE parser applies `retry:` the moment
  it parses the line, while an event is dispatched only if the data buffer is
  non-empty when the blank line arrives.
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

  Decoding goes exactly one level: the `data:` payload becomes frame metadata
  plus the list of rows, and no further. Turning a row's `data` into a struct is
  the message module's job and happens later, in the consumer's pipeline.

  Returns a frame whose `messages` is `nil` when the block carried no `data:`
  field. That is not an error — per the SSE specification an event is dispatched
  only if the data buffer is non-empty — and it is how a lone `retry:` frame
  arrives. Callers decide whether to dispatch.
  """
  @spec parse(binary()) ::
          {:ok, t()} | {:error, {:invalid_json, term()} | {:invalid_data_shape, term()}}
  def parse(block) when is_binary(block) do
    fields =
      block
      |> split_lines()
      |> Enum.reduce(%{data: []}, &collect_field/2)

    case fields.data do
      [] ->
        {:ok, build(fields, nil, nil)}

      lines ->
        # SSE joins consecutive `data:` lines with LF; DMEx always emits one, but
        # a generic producer may not, and joining costs nothing.
        payload = lines |> Enum.reverse() |> Enum.join("\n")
        decode_data(fields, payload)
    end
  end

  defp decode_data(fields, payload) do
    case JSON.decode(payload) do
      {:ok, %{"messages" => messages} = decoded} when is_list(messages) ->
        {:ok, build(fields, Map.get(decoded, "metadata", %{}), messages)}

      {:ok, other} ->
        {:error, {:invalid_data_shape, other}}

      {:error, reason} ->
        {:error, {:invalid_json, reason}}
    end
  end

  defp build(fields, metadata, messages) do
    %__MODULE__{
      id: fields[:id],
      event: fields[:event],
      metadata: metadata,
      messages: messages,
      retry: fields[:retry]
    }
  end

  # SSE line terminators are CR, LF and CRLF — and only those. Notably U+2028 and
  # U+2029 are not terminators here, despite being a perennial hazard in
  # JavaScript contexts, so they need no special handling.
  defp split_lines(block), do: String.split(block, ["\r\n", "\n", "\r"])

  defp collect_field(line, acc) do
    case parse_line(line) do
      {"data", value} -> Map.update!(acc, :data, &[value | &1])
      {"id", value} -> put_integer(acc, :id, value)
      {"retry", value} -> put_integer(acc, :retry, value)
      {"event", value} -> Map.put(acc, :event, value)
      _other -> acc
    end
  end

  # A line beginning with a colon is a comment and is ignored. A line with no
  # colon at all is a field with an empty value. Otherwise the value is
  # everything after the colon, minus a single optional leading space.
  defp parse_line(":" <> _comment), do: :comment

  defp parse_line(line) do
    case :binary.split(line, ":") do
      [field, " " <> value] -> {field, value}
      [field, value] -> {field, value}
      [""] -> :blank
      [field] -> {field, ""}
    end
  end

  defp put_integer(acc, key, value) do
    case Integer.parse(value) do
      {int, ""} -> Map.put(acc, key, int)
      _ -> acc
    end
  end
end
