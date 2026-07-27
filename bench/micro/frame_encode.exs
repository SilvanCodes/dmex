# Is `DMEx.Frame.encode_batch/2` O(bytes) — a memcpy over iodata — or does it
# carry meaningful per-row overhead?
#
# Three things happen per row that are not copying: an `Integer.to_string/1` for
# the id, two `JSON.encode_to_iodata!/1` calls (message_type and the ISO8601
# timestamp), and the `Enum.intersperse/2` that joins rows with commas. If those
# dominate at small payloads, precomputing the timestamp string or hand-rolling
# the join is worth doing; if they vanish beside the copy, the module is done.
#
# The ns/row figure this produces is also what sizes the frame byte cap: it says
# how long the producer spends building the largest frame it will ever send.

alias DMEx.BenchHelper, as: Helper
alias DMEx.Frame

Helper.section("frame_encode — producer-side framing cost")

inputs =
  for batch <- [1, 100, 1_000],
      payload <- [128, 1_024, 16_384],
      into: %{} do
    {"#{batch} rows x #{payload}B", Helper.rows(batch, payload)}
  end

Benchee.run(
  %{
    "encode_batch" => fn rows -> Frame.encode_batch(rows, 1_000_000) end,

    # Same work plus flattening, which is what the socket ultimately does. The
    # gap between the two is what returning iodata buys.
    "encode_batch + iodata_to_binary" => fn rows ->
      rows |> Frame.encode_batch(1_000_000) |> IO.iodata_to_binary()
    end
  },
  Helper.opts(inputs: inputs)
)
