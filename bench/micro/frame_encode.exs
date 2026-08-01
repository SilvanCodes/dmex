# Is `DMEx.Transport.SSE.Frame.encode_row/1` O(bytes) — a memcpy over iodata — or does it
# carry meaningful per-row overhead?
#
# Two things happen per row that are not copying: an `Integer.to_string/1` for
# the id, and a `JSON.encode_to_iodata!/1` for the ISO8601 timestamp. If those
# dominate at small payloads, precomputing the timestamp string is worth doing;
# if they vanish beside the copy, the module is done.
#


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
