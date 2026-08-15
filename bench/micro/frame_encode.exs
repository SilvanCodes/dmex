# Is `DMEx.Transport.SSE.Frame.encode_row/1` O(bytes) — a memcpy over iodata — or does it
# carry meaningful per-row overhead?
#
# Two things happen per row that are not copying: an `Integer.to_string/1` for
# the id, and a `JSON.encode_to_iodata!/1` for the ISO8601 timestamp. If those
# dominate at small payloads, precomputing the timestamp string is worth doing;
# if they vanish beside the copy, the module is done.
#
# Last measured — 2026-08-15, 28-core i7-14700K, Elixir 1.20.2/OTP 29:
#
#   * one row costs 57-60 ns to frame at 128 B, 1 KB and 16 KB alike, and 1000
#     rows cost 34-86 us at every one of those payload sizes — flat in payload
#     bytes, so the id and the timestamp are the whole per-row cost;
#   * flattening is where the bytes are paid: 1000 x 16 KB is 1.45 ms against
#     0.0525 ms for the same encode left as iodata, and 1000 x 128 B is 89 us
#     against 34 us.
#
# So the copy dominates even at 128 B and the per-row constants vanish beside
# it: precomputing the timestamp string would buy nothing, and returning iodata
# keeps the payload bytes untouched until the socket asks for them.

alias DMEx.BenchHelper, as: Helper
alias DMEx.Transport.SSE.Frame

Helper.section("frame_encode — log-side framing cost")

inputs =
  for batch <- [1, 100, 1_000],
      payload <- [128, 1_024, 16_384],
      into: %{} do
    {"#{batch} rows x #{payload}B", Helper.rows(batch, payload)}
  end

Benchee.run(
  %{
    "encode_row per row" => fn rows -> Enum.map(rows, &Frame.encode_row/1) end,

    # Same work plus flattening, which is what the socket ultimately does. The
    # gap between the two is what returning iodata buys.
    "encode_row + iodata_to_binary" => fn rows ->
      rows |> Enum.map(&Frame.encode_row/1) |> IO.iodata_to_binary()
    end
  },
  Helper.opts(inputs: inputs)
)
