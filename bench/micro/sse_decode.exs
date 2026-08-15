# The O(n^2) buffer-append trap, and the reason `DMEx.Transport.SSE.Decoder` accumulates
# iodata rather than concatenating binaries.
#
# Chunk boundaries are not frame boundaries, so a decoder has to hold a partial
# frame across reads. The obvious `buffer <> chunk` copies the whole buffer every
# time — and real TCP reads are small, so a 512 KB run of frames arriving in 1 KB
# pieces would copy far more than it received.
#
# The property to look for: cost should be flat in chunk size. If the 1 KB column
# is dramatically worse than the 64 KB one, the accumulation is quadratic.
#
# Last measured — 2026-08-15, 28-core i7-14700K, Elixir 1.20.2/OTP 29, over
# 531 KB in 500 frames:
#
#   * the decoder is flat in chunk size: 5.31 ms at 512 B, 4.76 ms at 1 KB,
#     3.30 ms at 8 KB, 3.25 ms at 64 KB, 3.56 ms in one chunk — a 1.6x spread
#     across a 128x range of chunk sizes, so ~100-160 MB/s whatever the socket
#     hands it;
#   * the naive control swings 180x over the same range — 0.0144 ms in one
#     chunk, 0.0905 ms at 64 KB, 2.64 ms at 8 KB, 1.23 ms at 1 KB, 1.89 ms at
#     512 B, so not even monotonic in chunk size.
#
# The property holds. The control's absolute numbers are not comparable: it
# finds one boundary per chunk and decodes nothing, so what it measures is the
# copying alone — which is exactly the cost that grows as chunks shrink, and the
# cost the iodata accumulation does not pay.

alias DMEx.BenchHelper, as: Helper
alias DMEx.Config
alias DMEx.Transport.SSE.Decoder

Helper.section("sse_decode — is the accumulation linear in bytes?")

frames = Helper.frame_bytes(500, 1_024)

# The bound the transport hands the decoder at connect, so this measures the
# shipped path rather than a hand-picked limit.
max_bytes = Config.handler!([]).max_frame_bytes

inputs =
  for size <- [512, 1_024, 8_192, 65_536], into: %{} do
    {"#{div(byte_size(frames), 1024)}KB in #{size}B chunks", Helper.chunks(frames, size)}
  end

inputs = Map.put(inputs, "everything in one chunk", [frames])

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
    "DMEx.Transport.SSE.Decoder (iodata)" => fn chunks ->
      Enum.reduce(chunks, Decoder.new(max_bytes), fn chunk, decoder ->
        {:ok, _frames, decoder} = Decoder.push(decoder, chunk)
        decoder
      end)
    end,
    "naive buffer <> chunk" => fn chunks -> Bench.NaiveDecoder.run(chunks) end
  },
  Helper.opts(inputs: inputs)
)
