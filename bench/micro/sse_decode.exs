# The O(n^2) buffer-append trap, and the reason `DMEx.SSE.Decoder` accumulates
# iodata rather than concatenating binaries.
#
# Chunk boundaries are not frame boundaries, so a decoder has to hold a partial
# frame across reads. The obvious `buffer <> chunk` copies the whole buffer every
# time — and real TCP reads are small, so a 512 KB frame arriving in 1 KB pieces
# would copy on the order of 128 MB.
#
# The property to look for: cost should be flat in chunk size. If the 1 KB column
# is dramatically worse than the 64 KB one, the accumulation is quadratic.

alias DMEx.BenchHelper, as: Helper
alias DMEx.SSE.Decoder

Helper.section("sse_decode — is the accumulation linear in bytes?")

frame = Helper.frame_bytes(500, 1_024)

inputs =
  for size <- [512, 1_024, 8_192, 65_536], into: %{} do
    {"#{div(byte_size(frame), 1024)}KB frame in #{size}B chunks", Helper.chunks(frame, size)}
  end

inputs = Map.put(inputs, "whole frame in one chunk", [frame])

# The naive implementation, for contrast.
defmodule Bench.NaiveDecoder do
  @moduledoc false

  def run(chunks) do
    Enum.reduce(chunks, "", fn chunk, buffer ->
      buffer = buffer <> chunk

      case :binary.match(buffer, "\n\n") do
        :nomatch -> buffer
        {start, len} -> binary_part(buffer, start + len, byte_size(buffer) - start - len)
      end
    end)
  end
end

Benchee.run(
  %{
    "DMEx.SSE.Decoder (iodata)" => fn chunks ->
      Enum.reduce(chunks, Decoder.new(), fn chunk, decoder ->
        {:ok, _frames, decoder} = Decoder.push(decoder, chunk)
        decoder
      end)
    end,
    "naive buffer <> chunk" => fn chunks -> Bench.NaiveDecoder.run(chunks) end
  },
  Helper.opts(inputs: inputs)
)
