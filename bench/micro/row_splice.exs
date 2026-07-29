# The load-bearing design comparison.
#
# `DMEx.SSE.Frame` splices each row's stored `data` and `metadata` into the frame as
# text, exactly as the read returned them, rather than letting the driver decode
# `jsonb` into a map and re-encoding it. The claim is that this makes the
# producer's per-row work "a memcpy" — and since every connection issues its own
# read, the alternative would pay a decode plus an encode once per connection
# for the same rows.
#
# This measures that claim against a control that does the decode-and-re-encode.
# Multiply the gap by C connections to get what fan-out would have cost.
#
# If the gap turns out to be small, the invariant is buying less than the design
# says it does — which is worth knowing, because it constrains the storage
# adapter contract (`data::text`) in a way that is otherwise inconvenient.

alias DMEx.BenchHelper, as: Helper
alias DMEx.SSE.Frame

Helper.section("row_splice — splicing stored JSON vs. decoding and re-encoding")

inputs =
  for payload <- [256, 1_024, 16_384], into: %{} do
    {"500 rows x #{payload}B", Helper.rows(500, payload)}
  end

# What DMEx would have to do if `read/3` returned driver-decoded maps: parse each
# row's payload back out of storage and re-serialize the whole frame.
reencode = fn rows, max_id ->
  messages =
    Enum.map(rows, fn row ->
      %{
        "id" => row.id,
        "message_type" => row.message_type,
        "data" => JSON.decode!(row.data),
        "metadata" => JSON.decode!(row.metadata),
        "inserted_at" => DateTime.to_iso8601(row.inserted_at)
      }
    end)

  JSON.encode_to_iodata!(%{"metadata" => %{"max_id" => max_id}, "messages" => messages})
end

Benchee.run(
  %{
    "splice (what DMEx does)" => fn rows -> Frame.encode_batch(rows, 1_000_000) end,
    "decode + re-encode (the control)" => fn rows -> reencode.(rows, 1_000_000) end
  },
  Helper.opts(inputs: inputs)
)
