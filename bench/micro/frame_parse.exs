# The ceiling on consumer drain rate, before any handler runs.
#
# A consumer's per-row cost is dominated by one `JSON.decode/1` over that row's
# payload. Whatever that decodes at is the fastest a subscription could possibly
# go with a free handler — so it is the number to check first when a consumer
# cannot keep up.
#
# It also checks the claim that stopping the parse one level deep is cheaper
# than decoding all the way into structs: `Frame.parse/1` yields a string-keyed
# row whose `data` is still a plain map, and casting happens later, per row, in
# the pipeline.

alias DMEx.BenchHelper, as: Helper
alias DMEx.SSE.Frame

Helper.section("frame_parse — consumer-side decode ceiling")

# One frame per row now, so there is no batch dimension left: a block is a row.
inputs =
  for payload <- [128, 1_024, 16_384], into: %{} do
    {"1 row x #{payload}B", 1 |> Helper.frame_bytes(payload) |> String.trim_trailing("\n\n")}
  end

Benchee.run(
  %{
    "Frame.parse (one level deep)" => fn bytes -> {:ok, _} = Frame.parse(bytes) end,

    # The whole payload through JSON.decode!, which is the bulk of the work
    # above — so the gap between these two is the framing overhead, not the JSON.
    "JSON.decode! of the data payload alone" => fn bytes ->
      bytes |> String.split("data: ", parts: 2) |> List.last() |> JSON.decode!()
    end
  },
  Helper.opts(inputs: inputs)
)
