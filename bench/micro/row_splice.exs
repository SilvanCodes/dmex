# Is splicing stored JSON text really cheaper than decode-and-re-encode, and by
# how much per connection?
#
# `DMEx.Transport.SSE.Frame` splices each row's stored `data` and `metadata` into the frame as
# text, exactly as the read returned them, rather than letting the driver decode
# `jsonb` into a map and re-encoding it. The claim is that this makes the
# log's per-row work "a memcpy" — and since every connection issues its own
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
alias DMEx.Transport.SSE.Frame

Helper.section("row_splice — splicing stored JSON vs. decoding and re-encoding")

inputs =
  for payload <- [256, 1_024, 16_384], into: %{} do
    {"500 rows x #{payload}B", Helper.rows(500, payload)}
  end

# What DMEx would have to do if `read/5` returned driver-decoded maps: parse each
# row's payload back out of storage and re-serialize every frame.
reencode = fn rows ->
  messages =
    Enum.map(rows, fn row ->
      %{
        "id" => row.id,
        "message_type" => row.message_type,
        "data" => JSON.decode!(row.data),
        "metadata" => JSON.decode!(row.metadata)
      }
    end)

  Enum.map(messages, &JSON.encode_to_iodata!/1)
end

# `metadata: [:id, :inserted_at]` is the same splice one level in: the lifted
# pairs go on the front of the stored metadata text rather than being merged into
# a decoded map. It flattens the result, because a row's fields are binaries — so
# the gap against the plain splice above is the copy that option costs a
# connection, and the gap against the control is what it still saves.
lift = fn rows ->
  rows
  |> Enum.map(&DMEx.Log.Metadata.merge(&1, [:id, :inserted_at]))
  |> Enum.map(&Frame.encode_row/1)
end

Benchee.run(
  %{
    "splice (what DMEx does)" => fn rows -> Enum.map(rows, &Frame.encode_row/1) end,
    "splice + lift two columns" => fn rows -> lift.(rows) end,
    "decode + re-encode (the control)" => fn rows -> reencode.(rows) end
  },
  Helper.opts(inputs: inputs)
)
