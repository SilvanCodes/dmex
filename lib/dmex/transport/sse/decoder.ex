defmodule DMEx.Transport.SSE.Decoder do
  @moduledoc """
  Turns a stream of arbitrary byte runs into whole log rows.

  **Chunk boundaries are not frame boundaries.** Chunked transfer encoding and
  TCP both deliver whatever they happen to have, so a frame can span several
  reads and a read can contain several frames plus part of another. This
  accumulates and dispatches on the blank-line delimiter.

  ## Two things it drops, on purpose

  Heartbeat comments (`: hb`) and the lone `retry:` frame at stream open carry
  no row. Neither is a delivery event, so neither is returned — but the `retry:`
  value is kept (see `retry/1`), because it is how a consumer learns the
  interval to measure silence against.

  ## The buffer is bounded

  A producer that never emits a blank line — broken, hostile, or simply
  mid-frame when it wedged — would grow this buffer without limit. Exceeding
  `max_bytes` returns `{:error, :frame_too_large, decoder}`; the caller drops
  the connection and reconnects from `last_seen`, which costs nothing beyond a
  re-read since a drop is not a delivery event.

  ## Why it accumulates iodata

  The obvious implementation, `buffer <> chunk`, copies the whole buffer on
  every chunk — quadratic in the number of chunks, and real TCP reads are small,
  so a large run of frames arriving in 1 KB pieces would copy far more than it
  received. Instead chunks are consed onto a list and flattened only when one of
  them actually contains a line terminator.
  """

  alias DMEx.Transport.SSE.Frame

  # The SSE specification's line terminators are CR, LF and CRLF only, so a
  # blank line is one of exactly these three.
  @delimiters ["\r\n\r\n", "\n\n", "\r\r"]
  @terminators ["\r", "\n"]

  defstruct buffer: [], size: 0, max_bytes: 4_194_304, retry: nil

  @type t :: %__MODULE__{
          buffer: iodata(),
          size: non_neg_integer(),
          max_bytes: pos_integer(),
          retry: pos_integer() | nil
        }

  @type error :: :frame_too_large | {:invalid_json, term()} | {:invalid_data_shape, term()}

  @doc "A decoder that will not buffer more than `max_bytes` of an unterminated frame."
  @spec new(pos_integer()) :: t()
  def new(max_bytes \\ 4_194_304) when is_integer(max_bytes) and max_bytes > 0 do
    %__MODULE__{max_bytes: max_bytes}
  end

  @doc """
  The producer's advertised reconnect/heartbeat interval, in ms, or `nil`.

  Carries `heartbeat_ms` plus one batch interval — the real upper bound on
  silence — so a
  consumer reads it as "expect a frame or a comment within this window".
  """
  @spec retry(t()) :: pos_integer() | nil
  def retry(%__MODULE__{retry: retry}), do: retry

  @doc """
  Feeds a chunk in and returns every row it completed.

  Rows come back in arrival order. A chunk that completes nothing returns an
  empty list, which is the ordinary case for a frame still in flight.
  """
  @spec push(t(), binary()) :: {:ok, [Frame.decoded_row()], t()} | {:error, error(), t()}
  def push(%__MODULE__{} = decoder, chunk) when is_binary(chunk) do
    decoder = %{decoder | buffer: [chunk | decoder.buffer], size: decoder.size + byte_size(chunk)}

    if terminator?(chunk) do
      decoder |> flatten() |> extract([])
    else
      check_size(decoder, [])
    end
  end

  ## Internals

  defp flatten(%{buffer: [single]} = decoder) when is_binary(single), do: decoder

  defp flatten(decoder) do
    %{decoder | buffer: [decoder.buffer |> Enum.reverse() |> IO.iodata_to_binary()]}
  end

  defp extract(%{buffer: [binary]} = decoder, acc) do
    case :binary.match(binary, @delimiters) do
      :nomatch ->
        check_size(decoder, Enum.reverse(acc))

      {start, length} ->
        block = binary_part(binary, 0, start)
        rest = binary_part(binary, start + length, byte_size(binary) - start - length)
        decoder = %{decoder | buffer: [rest], size: byte_size(rest)}

        case Frame.parse(block) do
          {:ok, {:message, row}} -> extract(decoder, [row | acc])
          {:ok, {:retry, retry}} -> extract(%{decoder | retry: retry}, acc)
          {:ok, :ignore} -> extract(decoder, acc)
          {:error, reason} -> {:error, reason, decoder}
        end
    end
  end

  defp check_size(%{size: size, max_bytes: max} = decoder, _frames) when size > max do
    {:error, :frame_too_large, decoder}
  end

  defp check_size(decoder, frames), do: {:ok, frames, decoder}

  defp terminator?(chunk), do: :binary.match(chunk, @terminators) != :nomatch
end
