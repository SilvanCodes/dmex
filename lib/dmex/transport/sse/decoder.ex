defmodule DMEx.Transport.SSE.Decoder do
  @moduledoc """
  Turns a stream of arbitrary byte runs into whole log rows.

  **Chunk boundaries are not frame boundaries.** Chunked transfer encoding and
  TCP both deliver whatever they happen to have, so a frame can span several
  reads and a read can contain several frames plus part of another. This
  accumulates and dispatches on the blank-line delimiter.

  **Blocks carrying no row are dropped**, every `:ignore` in
  `t:DMEx.Transport.SSE.Frame.decoded/0`. The `retry:` value is kept (see
  `retry/1`): it is how a consumer learns the interval to measure silence against.

  **The buffer is bounded.** A producer that never emits a blank line — broken,
  hostile, or simply mid-frame when it wedged — would grow it without limit.
  Exceeding `max_bytes` returns `{:error, :frame_too_large, decoder}`; the caller
  drops the connection and reconnects from `last_seen`, costing only a re-read.

  **It accumulates iodata.** `buffer <> chunk` would copy the whole buffer every
  chunk; chunks are consed instead, flattened only when one carries a line
  terminator — linear while terminators are rare inside a frame, quadratic again
  for a producer that splits `data:` over many lines.

  **A leading byte order mark is removed once, at the head of the stream** — the
  event-stream grammar's first step, and a property of the stream rather than of
  a block: the same bytes opening any later block are payload. Left in, it joins
  the first field's *name*, which then matches nothing, so the first frame — the
  `retry:` advertisement, against DMEx's own producer — disappears with no error
  and a consumer's liveness budget silently falls back to its assumed default.
  """

  alias DMEx.Transport.SSE.Frame

  # A blank line is any two line terminators back to back — every combination,
  # not just the matching pairs, since a producer may mix them. The exception is
  # a pair that is *itself* a terminator: CR followed by LF, which the grammar
  # reads as one CRLF rather than two line endings. DMEx's own producer is the
  # "\n\n" case.
  #
  # Derived from `Frame.line_terminators/0` rather than restated, so the
  # vocabulary has one home: these eight are that set crossed with itself, minus
  # the pairs the set already contains.
  #
  # `:binary.split/2` splits on the leftmost match and the longest of those, which
  # is what these need: "\r\n\r\n" wins over the "\r\n\r" starting at the same
  # byte, and "\r\n\n" wins over the "\n\n" one byte later — and it is why the
  # generated order does not matter.
  @delimiters for first <- Frame.line_terminators(),
                  second <- Frame.line_terminators(),
                  (first <> second) not in Frame.line_terminators(),
                  do: first <> second

  # CR and CRLF are tolerated, though DMEx's own encoder emits LF only, because
  # `guides/fingerprint-spec.md` is normative: a third-party producer may use any
  # terminator the SSE grammar allows. The same argument covers the BOM below.

  # Every terminator contains a CR or an LF, so the single-byte ones are the
  # cheap "could this chunk have completed a line?" test over the same set.
  @terminators Enum.filter(Frame.line_terminators(), &(byte_size(&1) == 1))

  defstruct buffer: [], size: 0, max_bytes: nil, retry: nil, bom: <<>>

  @type t :: %__MODULE__{
          buffer: iodata(),
          size: non_neg_integer(),
          max_bytes: pos_integer(),
          retry: pos_integer() | nil,
          bom: binary() | :done
        }

  @type error :: :frame_too_large | Frame.error()

  @doc "Builds a decoder buffering at most `max_bytes` of an unterminated frame."
  @spec new(pos_integer()) :: t()
  def new(max_bytes) when is_integer(max_bytes) and max_bytes > 0 do
    %__MODULE__{max_bytes: max_bytes}
  end

  @doc """
  Returns the producer's advertised interval in ms, or `nil` — the upper bound on
  silence a consumer measures against.
  """
  @spec retry(t()) :: pos_integer() | nil
  def retry(%__MODULE__{retry: retry}), do: retry

  @doc """
  Feeds a chunk in and returns every row it completed.

  Rows come back in arrival order; a chunk that completes nothing returns `[]`.
  """
  @spec push(t(), binary()) ::
          {:ok, [DMEx.Handler.Pipeline.decoded_row()], t()} | {:error, error(), t()}
  def push(%__MODULE__{} = decoder, chunk) when is_binary(chunk) do
    {chunk, bom} = strip_bom(decoder.bom, chunk)

    decoder = %{
      decoder
      | buffer: [chunk | decoder.buffer],
        size: decoder.size + byte_size(chunk),
        bom: bom
    }

    if terminator?(chunk) do
      decoder |> flatten() |> extract([])
    else
      check_size(decoder, [])
    end
  end

  ## Internals

  @bom <<0xEF, 0xBB, 0xBF>>

  # `:done` once the question is settled, so every chunk after the first costs one
  # clause head. Until then the bytes seen so far are held back rather than
  # buffered — at most two, and only before the first frame can be complete —
  # because deciding on a partial prefix would eat payload or leave a mark behind.
  defp strip_bom(:done, chunk), do: {chunk, :done}

  defp strip_bom(seen, chunk) do
    case seen <> chunk do
      @bom <> rest -> {rest, :done}
      <<>> -> {<<>>, <<>>}
      <<0xEF>> = partial -> {<<>>, partial}
      <<0xEF, 0xBB>> = partial -> {<<>>, partial}
      other -> {other, :done}
    end
  end

  defp flatten(%{buffer: [single]} = decoder) when is_binary(single), do: decoder

  defp flatten(decoder) do
    %{decoder | buffer: [decoder.buffer |> Enum.reverse() |> IO.iodata_to_binary()]}
  end

  defp extract(%{buffer: [binary]} = decoder, acc) do
    case :binary.split(binary, @delimiters) do
      [_unterminated] ->
        check_size(decoder, Enum.reverse(acc))

      [block, rest] ->
        decoder = %{decoder | buffer: [rest], size: byte_size(rest)}

        case Frame.decode(block) do
          {:ok, {:message, row}} -> extract(decoder, [row | acc])
          {:ok, {:retry, retry}} -> extract(%{decoder | retry: retry}, acc)
          {:ok, :ignore} -> extract(decoder, acc)
          {:error, reason} -> {:error, reason, decoder}
        end
    end
  end

  # Both error paths throw away the rows this `push/2` already decoded, where
  # `DMEx.Transport.SSE` hands rows over before exiting on a terminal response.
  # The difference: a malformed frame is malformed on every read, so a reconnect
  # from the durable cursor replays it and fails identically — returning the rows
  # ahead would shorten one re-read and never buy progress.
  defp check_size(%{size: size, max_bytes: max} = decoder, _frames) when size > max do
    {:error, :frame_too_large, decoder}
  end

  defp check_size(decoder, frames), do: {:ok, frames, decoder}

  defp terminator?(chunk), do: :binary.match(chunk, @terminators) != :nomatch
end
