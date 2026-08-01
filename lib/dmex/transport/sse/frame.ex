defmodule DMEx.Transport.SSE.Frame do
  @moduledoc """
  The SSE wire format: one frame per log row.

  A `dmex_messages` row has five columns and an SSE frame has five slots, so the
  mapping is direct and there is no envelope:

      id: 1041/1042
      event: MyApp.Messages.OrderPlaced
      data: {"data":{"order_id":7},"metadata":{"trace_id":"abc"},"inserted_at":"2026-07-27T10:31:04.221Z"}

  | column | goes to | why there |
  | --- | --- | --- |
  | `id` | `id:`, as `previous/current` | it *is* the resume position, so a generic client's `Last-Event-ID` works with no DMEx knowledge — and the predecessor makes a *missing* frame detectable |
  | `message_type` | `event:` | it *is* the event name, so `addEventListener("MyApp.Messages.OrderPlaced", …)` works in a browser |
  | `data` | inside `data:` | the publisher's payload, verbatim |
  | `metadata` | inside `data:` | the publisher's metadata, verbatim |
  | `inserted_at` | inside `data:` | append time, which is what the consumer's lag signal is measured against |

  There is no batch envelope and no frame-level `max_id`. Batching is still real
  — the producer writes a whole read's worth of frames in one `chunk/2` — but it
  is a *write* batch rather than a wire construct, so nothing on the consumer
  side has to know it happened. That is what lets `id:` be the row's own id
  rather than "the highest id in this frame", and what makes the frame's meaning
  identical whether one row or five hundred went out together.

  ## `id:` is a link, not a number

  `id: 1041/1042` reads "the row before me was 1041, I am 1042". The pair is
  what makes a **missing** frame detectable: a consumer holds the last id it
  saw and refuses a frame that does not claim it as its predecessor, so a row
  that never arrives breaks the chain instead of being silently stepped over.
  Without it a dropped frame is invisible — the next row's cursor advance
  commits straight past the gap and the message is gone for good.

  It has to be a stated link rather than `id - 1`, because a selective stream's
  ids are legitimately sparse: the producer skips the types this subscription
  did not ask for, and only the producer knows which gaps are deliberate.

  The first frame of a connection carries the cursor the read was issued
  against, so the chain also proves the producer resumed where it was asked to.

  **`Last-Event-ID` therefore carries the pair too.** A generic client stores
  whatever was in `id:` and echoes it back verbatim, so a producer must accept
  `1041/1042` and resume after `1042`. See `DMEx.Log.Params`.

  ## `event:` carries the message type, and the type is constrained for it

  SSE field lines have no escaping: a CR or LF inside `event:` would end the
  line and split the frame. JSON's grammar forbids raw control characters inside
  strings, which is what keeps the spliced `data:` payload safe — but
  `message_type` is not spliced through a JSON encoder here, it is written
  straight onto a header line.

  So the log constrains it instead: `dmex_messages` carries a CHECK constraint
  restricting `message_type` to `[A-Za-z0-9_.]`, which is exactly the charset an
  Elixir module name uses. A type that cannot be written on an `event:` line
  cannot be appended in the first place. See `DMEx.Migration`.

  ## Encoding

  Frames are built as iodata, and rows are **spliced, never re-encoded**: a
  row's `data` and `metadata` go in as text, exactly as the read returned them.
  The alternative — letting the driver decode `jsonb` into a map and re-encoding
  it — pays a decode plus an encode per row, and pays it once per connection for
  the same rows, since every connection issues its own read.

  Two invariants keep that safe, and they are the whole of the encoding
  contract: a frame is **never pretty-printed**, and **nothing is spliced into
  it that did not come out of a JSON encoder, a JSON-typed column, or a column
  the schema constrains to a safe charset**. Together they guarantee no raw CR
  or LF reaches the wire, which is what makes it safe to put a whole payload on
  one `data:` line with no base64 and no escaping pass.
  """

  @typedoc "A log row on the log's side, as `c:DMEx.Storage.read/5` returns it."
  @type row :: DMEx.Storage.row()

  @typedoc """
  A log row on the reader's side, as `parse/1` yields it.

  String-keyed and one level decoded — `data` and `metadata` are maps, and
  turning `data` into a message struct is the message module's job, later, in
  the pipeline.
  """
  @type decoded_row :: DMEx.Handler.Pipeline.row()

  @typedoc """
  One parsed frame block.

  `:message` is a data frame — the only kind that carries a row. `:retry` is the
  producer's advertised heartbeat interval, which an SSE parser applies the
  moment it reads the line. `:ignore` is everything else: a heartbeat comment, a
  frame whose `data:` buffer was empty, a field vocabulary this does not know.
  """
  @type parsed :: {:message, decoded_row()} | {:retry, pos_integer()} | :ignore

  @type error :: {:invalid_json, term()} | {:invalid_data_shape, term()}

  @doc """
  Encodes one log row as one frame, as iodata.

  `:data` and `:metadata` are spliced verbatim and must therefore already be
  JSON text. `:message_type` goes on the `event:` line unescaped, which the
  log's CHECK constraint is what makes safe; `:inserted_at` is run through the
  JSON encoder.

  `id:` carries `previous_id/id`. A `nil` predecessor renders as `0`, which is
  the same thing a read from the very beginning of a log says.
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
      ",\"inserted_at\":",
      row.inserted_at |> to_iso8601() |> JSON.encode_to_iodata!(),
      "}\n\n"
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

  A block is a message only if it carries **all three** of `id:`, `event:` and a
  non-empty `data:`. Anything short of that is `:ignore` rather than an error:
  per the SSE specification an event is dispatched only when the data buffer is
  non-empty, and a producer is free to send comments and bare fields.

  That `:ignore` is precisely why the id chain exists. A block this drops is a
  message the consumer never sees, and without the chain it would be
  indistinguishable from a message that was never sent — see the moduledoc.

  `id:` parses in both forms. `1041/1042` yields `"previous_id" => 1041`; a bare
  `1042` yields `nil`, which is what a producer that predates the chain sends
  and what a consumer refuses to accept.

  Decoding goes exactly one level. The `data:` payload becomes the row's `data`,
  `metadata` and `inserted_at`; turning `data` into a struct happens later, in
  the consumer's pipeline.
  """
  @spec parse(binary()) :: {:ok, parsed()} | {:error, error()}
  def parse(block) when is_binary(block) do
    fields =
      block
      |> split_lines()
      |> Enum.reduce(%{data: []}, &collect_field/2)

    case fields do
      %{id: id, event: event, data: [_ | _] = lines} ->
        # SSE joins consecutive `data:` lines with LF; DMEx always emits one, but
        # a generic producer may not, and joining costs nothing.
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

  # SSE line terminators are CR, LF and CRLF — and only those. Notably U+2028 and
  # U+2029 are not terminators here, despite being a perennial hazard in
  # JavaScript contexts, so they need no special handling.
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

  # `previous/current`, or a bare `current` from anything that does not chain.
  # Anything else is not an id, and a block with no id is not a message.
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
