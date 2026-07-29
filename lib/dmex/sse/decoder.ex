defmodule DMEx.SSE.Decoder do
  @moduledoc """
  Turns a stream of arbitrary byte runs into whole `DMEx.SSE.Frame`s.

  **Chunk boundaries are not frame boundaries.** Chunked transfer encoding and
  TCP both deliver whatever they happen to have, so a frame can span several
  reads and a read can contain several frames plus part of another. This
  accumulates and dispatches on the blank-line delimiter.

  ## Two things it drops, on purpose

  Heartbeat comments (`: hb`) and the lone `retry:` frame at stream open both
  parse to a frame with no `messages`. Neither is a delivery event, so neither
  is returned — but the `retry:` value is kept (see `retry/1`), because it is
  how a consumer learns the interval to measure silence against.

  ## The buffer is bounded, and the queue bound does not cover it

  A producer that never emits a blank line — broken, hostile, or simply
  mid-frame when it wedged — would grow this buffer without limit, and it does
  so on the socket path *above* any queue, where a row-based bound has no reach.
  Exceeding `max_bytes` returns `{:error, :frame_too_large, decoder}`; the
  caller drops the connection and reconnects from `last_seen`, which costs
  nothing beyond a re-read since a drop is not a delivery event.

  ## Why it accumulates iodata

  The obvious implementation, `buffer <> chunk`, copies the whole buffer on
  every chunk — quadratic in the number of chunks, and real TCP reads are small,
  so a 512 KB frame arriving in 1 KB pieces would copy ~128 MB. Instead chunks
  are consed onto a list and flattened only when one of them actually contains a
  line terminator, which for DMEx frames means roughly once per frame: the
  payload is a single `data:` line, and JSON's grammar forbids raw control
  characters inside it, so a frame's bytes are newline-free until it ends.
  """

  alias DMEx.SSE.Frame

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

  Carries `heartbeat_ms + batch_window` — the real upper bound on silence — so a
  consumer reads it as "expect a frame or a comment within this window".
  """
  @spec retry(t()) :: pos_integer() | nil
  def retry(%__MODULE__{retry: retry}), do: retry

  @doc """
  Feeds a chunk in and returns every frame it completed.

  Frames come back in arrival order. A chunk that completes nothing returns an
  empty list, which is the ordinary case for a large frame in flight.
  """
  @spec push(t(), binary()) :: {:ok, [Frame.t()], t()} | {:error, error(), t()}
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
          {:ok, frame} -> extract(apply_retry(decoder, frame), collect(acc, frame))
          {:error, reason} -> {:error, reason, decoder}
        end
    end
  end

  # A frame with no `messages` is a heartbeat comment or the stream-open
  # `retry:` frame. Neither is a delivery event, so neither is handed on.
  defp collect(acc, %Frame{messages: nil}), do: acc
  defp collect(acc, frame), do: [frame | acc]

  defp apply_retry(decoder, %Frame{retry: nil}), do: decoder
  defp apply_retry(decoder, %Frame{retry: retry}), do: %{decoder | retry: retry}

  defp check_size(%{size: size, max_bytes: max} = decoder, _frames) when size > max do
    {:error, :frame_too_large, decoder}
  end

  defp check_size(decoder, frames), do: {:ok, frames, decoder}

  defp terminator?(chunk), do: :binary.match(chunk, @terminators) != :nomatch
end
