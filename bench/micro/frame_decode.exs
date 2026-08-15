# The ceiling on consumer drain rate, before any handler runs.
#
# A consumer's per-row cost is dominated by one `JSON.decode/1` over that row's
# payload. Whatever that decodes at is the fastest a subscription could possibly
# go with a free handler — so it is the number to check first when a consumer
# cannot keep up.
#
# It also checks the claim that stopping the parse one level deep is cheaper
# than decoding all the way into structs: `Frame.decode/1` yields a string-keyed
# row whose `data` is still a plain map, and casting happens later, per row, in
# the pipeline.
#
# Last measured — 2026-08-15, 28-core i7-14700K, Elixir 1.20.2/OTP 29:
#
#   * `Frame.decode/1` runs at 2.17 us per 128 B row, 3.92 us at 1 KB and
#     29.3 us at 16 KB — a ceiling of 460 K, 255 K and 34 K rows/s per
#     connection with a free handler;
#   * the row's own `JSON.decode!/1` is 0.83, 1.19 and 7.42 us, so framing
#     costs 1.6-2.9x the JSON and grows with the block because `split_lines/1`
#     scans it.
#
# The ceiling is three orders of magnitude above the ~265 rows/s a handled row
# commits at (`bench/e2e/catchup_drain.exs`), so a consumer that cannot keep up
# is not being held there by the decode. The one-level-deep parse is not what
# would have to give if it ever were: the framing scan costs more than the
# payload's JSON at every size measured.

alias DMEx.BenchHelper, as: Helper
alias DMEx.Transport.SSE.Frame

Helper.section("frame_decode — consumer-side decode ceiling")

# One frame per row: a block is a row.
inputs =
  for payload <- [128, 1_024, 16_384], into: %{} do
    {"1 row x #{payload}B", 1 |> Helper.frame_bytes(payload) |> String.trim_trailing("\n\n")}
  end

Benchee.run(
  %{
    "Frame.decode (one level deep)" => fn bytes -> {:ok, _} = Frame.decode(bytes) end,

    # The whole payload through JSON.decode!, which is the bulk of the work
    # above — so the gap between these two is the framing overhead, not the JSON.
    "JSON.decode! of the data payload alone" => fn bytes ->
      bytes |> String.split("data: ", parts: 2) |> List.last() |> JSON.decode!()
    end
  },
  Helper.opts(inputs: inputs)
)
