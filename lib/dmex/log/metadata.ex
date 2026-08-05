defmodule DMEx.Log.Metadata do
  @moduledoc """
  Merges a log's own columns into the metadata a handler sees.

  A handler receives the publisher's metadata map and nothing else, so a
  subscription that wants the row's id or its append time has no way to ask for
  it. `metadata:` on `use DMEx.Log` is that ask, answered on the producing side:
  the log names which of its columns get merged, and they arrive as ordinary
  metadata keys.

      use DMEx.Log, repo: MyApp.Repo, metadata: [:id, :inserted_at]

      def handle_message(%OrderPlaced{} = order, %{"id" => id}), do: ...

  **Nothing the publisher wrote is decoded or re-encoded.** `metadata` is JSON
  object text and stays that way: the pairs go in at the front and the object is
  flattened once, so an opted-in log pays one copy of its metadata field — never a
  decode plus an encode of the publisher's JSON, which is the invariant
  `DMEx.Transport.SSE.Frame`'s splice-verbatim contract exists to protect. A log
  that lifts nothing pays nothing, since `merge/2` is then the identity.

  The front is also what decides a collision on DMEx's own consumer path: Elixir's
  `JSON` takes the first of two duplicate keys, so a lifted column beats a
  publisher key of the same name. `metadata:` therefore makes the names it lists
  reserved for that log — the one place DMEx writes into a space it otherwise
  leaves alone. Reserved is not merely a convention: a publisher that reuses a
  lifted name puts a duplicate key on the wire, and a generic client whose parser
  is last-wins will disagree with DMEx about which value it means.
  """

  @typedoc """
  A column that may be lifted.

  The `dmex_messages` columns less `data`, which is the message itself, and
  `metadata`, which is where these are going.
  """
  @type field :: :id | :message_type | :inserted_at

  @doc "Every liftable column, for `DMEx.Config` to validate `metadata:` against."
  @spec fields() :: [field()]
  def fields, do: [:id, :message_type, :inserted_at]

  @doc """
  Merges `fields` into `row`'s metadata, as text, and returns the row.

  `row.metadata` comes back a binary because it went in as one, so nothing
  downstream has to know a merge happened.
  """
  @spec merge(DMEx.Storage.row(), [field()]) :: DMEx.Storage.row()
  def merge(row, []), do: row

  def merge(row, fields) do
    %{row | metadata: IO.iodata_to_binary(splice(row.metadata, pairs(row, fields)))}
  end

  defp pairs(row, fields) do
    fields
    |> Enum.map(&pair(row, &1))
    |> Enum.intersperse(?,)
  end

  defp pair(row, :id), do: [~s("id":), Integer.to_string(row.id)]

  defp pair(row, :message_type) do
    [~s("message_type":), JSON.encode_to_iodata!(row.message_type)]
  end

  # A `DateTime` and only a `DateTime`: `t:DMEx.Storage.row/0` narrowed
  # `:inserted_at` to one precisely so a frame cannot lose its trailing `Z`, and
  # accepting a `NaiveDateTime` here would be this module quietly undoing that.
  defp pair(row, :inserted_at) do
    [~s("inserted_at":), row.inserted_at |> DateTime.to_iso8601() |> JSON.encode_to_iodata!()]
  end

  defp splice(<<?{, rest::binary>>, pairs) do
    # An empty object has nothing for the pairs to precede, so it is the one case
    # without a comma. `trim_leading/1` returns a sub-binary rather than a copy
    # and the comparison checks size first, so this stays O(1) per row.
    if String.trim_leading(rest) == "}" do
      [?{, pairs, rest]
    else
      [?{, pairs, ?,, rest]
    end
  end
end
