# The ceiling on consumer drain rate, before any handler runs.
#
# A consumer's per-frame cost is dominated by one `JSON.decode/1` over the whole
# payload. Whatever that decodes at is the fastest a subscription could possibly
# go with a free handler — so it is the number the adaptive controller's
# service-time estimate has to sit under, and the number to check first when a
# consumer cannot keep up.
#
# It also checks the claim that stopping the parse one level deep is cheaper
# than decoding all the way into structs: `Frame.parse/1` yields frame metadata
# plus a list of still-raw rows, and casting happens later, per row, in the
# pipeline.

alias DMEx.BenchHelper, as: Helper
alias DMEx.Frame

Helper.section("frame_parse — consumer-side decode ceiling")

inputs =
  for batch <- [1, 100, 1_000],
      payload <- [128, 1_024, 16_384],
      into: %{} do
    {"#{batch} rows x #{payload}B", Helper.frame_bytes(batch, payload)}
  end

Benchee.run(
  %{
    "Frame.parse (one level deep)" => fn bytes -> {:ok, _} = Frame.parse(bytes) end,

    # The whole payload through JSON.decode!, which is the bulk of the work
    # above — so the gap between these two is the framing overhead, not the JSON.
    "JSON.decode! of the data payload alone" => fn bytes ->
      bytes
      |> String.split("data: ", parts: 2)
      |> List.last()
      |> String.trim_trailing("\n\n")
      |> JSON.decode!()
    end
  },
  Helper.opts(inputs: inputs)
)
