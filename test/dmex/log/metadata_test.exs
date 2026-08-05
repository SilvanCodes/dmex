defmodule DMEx.Log.MetadataTest do
  use DMEx.Case, async: true

  alias DMEx.Log

  @at ~U[2026-07-27 10:31:04.221Z]

  defp row(opts) do
    %{
      id: 1042,
      message_type: "MyApp.Messages.OrderPlaced",
      data: ~s({"order_id":7}),
      metadata: Keyword.get(opts, :metadata, "{}"),
      inserted_at: Keyword.get(opts, :inserted_at, @at)
    }
  end

  defp merged(fields, opts \\ []) do
    opts |> row() |> Log.merge_metadata(fields) |> Map.fetch!(:metadata)
  end

  defp decoded(fields, opts \\ []) do
    fields |> merged(opts) |> JSON.decode!()
  end

  describe "merge_metadata/2" do
    test "lifting nothing returns the row untouched" do
      assert Log.merge_metadata(row(metadata: ~s({"a":1})), []) == row(metadata: ~s({"a":1}))
    end

    test "renders each column as its own key" do
      assert decoded([:id, :message_type, :inserted_at]) == %{
               "id" => 1042,
               "message_type" => "MyApp.Messages.OrderPlaced",
               "inserted_at" => "2026-07-27T10:31:04.221Z"
             }
    end

    test "an empty metadata object gains the pairs and stays valid JSON" do
      assert merged([:id]) == ~s({"id":1042})

      # Not only the canonical spelling: an adapter's `{ }` must not become
      # `{"id":1042, }`.
      assert decoded([:id], metadata: "{ }") == %{"id" => 1042}
    end

    test "keeps what the publisher wrote, byte for byte and in order" do
      # Spliced, not decoded and re-encoded: key order survives, which a round
      # trip through a map would not guarantee.
      assert merged([:id], metadata: ~s({"z":1,"a":2})) == ~s({"id":1042,"z":1,"a":2})
    end

    test "the lifted column wins a collision with a publisher key" do
      # The pairs go in at the front, and Elixir's JSON takes the first of two
      # duplicate keys — which is what makes `metadata:` reserve the names it
      # lists on the path DMEx's own consumer reads.
      assert decoded([:id], metadata: ~s({"id":"publishers"})) == %{"id" => 1042}
    end

    test "accepts a NaiveDateTime as well as a DateTime" do
      assert decoded([:inserted_at], inserted_at: ~N[2026-07-27 10:31:04]) == %{
               "inserted_at" => "2026-07-27T10:31:04"
             }
    end

    # A row's fields are binaries — `t:DMEx.Storage.row/0` — so a merged row is
    # still an ordinary row and nothing downstream has to know one happened.
    # Iodata belongs to the frame, not here.
    test "returns a binary, because it received one" do
      assert is_binary(merged([:id]))
    end
  end

  describe "metadata_fields/0" do
    test "is the message columns less the payload and metadata itself" do
      assert Log.metadata_fields() == [:id, :message_type, :inserted_at]
    end
  end
end
