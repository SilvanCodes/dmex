defmodule DMEx.Storage.SQL do
  @moduledoc """
  The windowed read statement, rendered once for both shipped adapters.

  Internal to DMEx: an adapter calls this and nothing else does.

  `c:DMEx.Storage.read/5` is a batch bounded by two budgets at once, and the
  four-level prefix-sum query below is the whole of how that bound is computed.
  Every other statement the two adapters duplicate — the inserts, the cursor
  upsert, the dead-message pair — is short enough that a divergence shows up as
  a failing conformance test or an error out of the driver. A divergence *here*
  does not: both engines would still return rows, in order, and merely cut the
  batch in a different place. That is a data bug — a subscription wedged behind
  one oversized row on one engine only, or a frame over the byte budget — and it
  is the only reason this module exists.

  Everything else that looks duplicated between the two adapters stays
  duplicated on purpose. Two statements each readable on its own beat one
  statement with an engine switch inside it, and none of them can fail quietly.

  ## The four tokens

  All the two engines actually disagree about, and nothing else:

    * `:table` — the message table, rendered as that engine renders it. Postgres
      quotes it and qualifies it with the `:prefix` schema, through
      `DMEx.Storage.Postgres.__table__/2`; SQLite has no schemas and interpolates
      the bare name.
    * `:param` — the sigil a numbered placeholder starts with, `"$"` or `"?"`.
      Both engines number from 1 and bind one value to every occurrence of a
      number, so the sigil is the whole difference.
    * `:cast` — how the innermost SELECT projects one payload column. Postgres
      stores `jsonb` and `c:DMEx.Storage.read/5` hands back text, so it casts
      once, there; SQLite's columns are already text.
    * `:bytes` — the byte length of one column. `octet_length(x)` on Postgres,
      `length(CAST(x AS BLOB))` on SQLite, which is the spelling that does not
      need SQLite 3.43.

  `:cast` and `:bytes` are per-column functions rather than finished
  expressions, and that is the point of the split: *which* columns are measured,
  and that the same ones are projected, is the shared invariant — an engine gets
  to spell "the byte length of" and nothing more. A finished expression would
  hand back the ability to measure a different set of columns on one engine,
  which is the bug this module exists to make unrepresentable.

  `:predicate` is not one of the four. It is the type selection for this one
  call, rendered by the adapter because the engines spell set membership
  differently — one `= ANY($4)` against N placeholders — so the numbering of
  everything after the three fixed parameters is the adapter's business rather
  than this module's. Empty string for a read of every type.
  """

  @typedoc """
  The four engine tokens, plus this call's already-rendered type predicate.

  `:table` takes an atom too, because `DMEx.Storage.__table__/1` names the tables
  as atoms and every unprefixed site spells one by interpolating it.
  """
  @type opts :: %{
          table: String.t() | atom(),
          param: String.t(),
          cast: (String.t() -> String.t()),
          bytes: (String.t() -> String.t()),
          predicate: String.t()
        }

  @doc """
  Renders the read statement. See the module documentation for the tokens.

  Four nested levels, each load-bearing.

  **`page`** applies the row limit against the index, so everything above it
  works on at most `LIMIT` rows. Not stylistic: within one SELECT both engines
  compute window functions *before* LIMIT, so a window at the same level as the
  LIMIT would run the sum over every matching row in the log — and neither
  flattens a subquery that contains one, so the nesting holds. Any `:cast`
  happens here, once.

  **`sized`** gives each row its byte count, off the already-cast columns.

  **`measured`** adds the *exclusive* prefix sum over the same ordering — bytes
  of the rows strictly before this one; `sum()` over an empty frame is NULL,
  hence the coalesce, and that NULL-to-zero is exactly the first row.

  **The outer filter** applies the byte bound — inclusive, plus
  `prefix_bytes = 0` so one oversized row cannot wedge the subscription. That
  identifies the first row exactly: `type` is `NOT NULL` and at least one
  character, so every row contributes at least one byte. See
  `c:DMEx.Storage.read/5`.

  The ORDER BY is repeated at the top because a filter over a subquery does not
  contractually preserve its ordering, and ordering here is a delivery
  guarantee.
  """
  @spec read_sql(opts()) :: String.t()
  def read_sql(%{table: table, param: param, cast: cast, bytes: bytes, predicate: predicate}) do
    """
    SELECT id, type, data, metadata, inserted_at
      FROM (
        SELECT id, type, data, metadata, inserted_at, bytes,
               coalesce(
                 sum(bytes) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                 0
               ) AS prefix_bytes
          FROM (
            SELECT id, type, data, metadata, inserted_at,
                   #{bytes.("data")} +
                   #{bytes.("metadata")} +
                   #{bytes.("type")} AS bytes
              FROM (
                SELECT id, type, #{cast.("data")}, #{cast.("metadata")},
                       inserted_at
                  FROM #{table}
                 WHERE id > #{param}1 #{predicate}
                 ORDER BY id
                 LIMIT #{param}2
              ) page
          ) sized
      ) measured
     WHERE prefix_bytes + bytes <= #{param}3 OR prefix_bytes = 0
     ORDER BY id
    """
  end
end
