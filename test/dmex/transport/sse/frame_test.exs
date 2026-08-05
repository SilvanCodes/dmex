defmodule DMEx.Transport.SSE.FrameTest do
  use DMEx.Case, async: true
  use ExUnitProperties

  alias DMEx.Transport.SSE.Frame

  @at ~U[2026-07-27 10:31:04.221Z]

  defp row(id, opts \\ []) do
    %{
      id: id,
      message_type: Keyword.get(opts, :message_type, "MyApp.Messages.OrderPlaced"),
      data: Keyword.get(opts, :data, ~s({"order_id":7})),
      metadata: Keyword.get(opts, :metadata, "{}"),
      inserted_at: Keyword.get(opts, :inserted_at, @at)
    }
  end

  defp encode(row), do: row |> Frame.encode_row() |> IO.iodata_to_binary()

  describe "encode_row/1" do
    test "produces exactly the documented wire shape" do
      assert encode(row(1042)) ==
               "id: 1042\n" <>
                 "event: MyApp.Messages.OrderPlaced\n" <>
                 ~s(data: {"data":{"order_id":7},"metadata":{}}\n\n)
    end

    # Nothing DMEx owns rides at the top level of `data:` — a log wanting its own
    # columns there lifts them into `metadata` first, which happens before a row
    # reaches the frame.
    test "carries the payload and the metadata, and nothing else" do
      assert {:message, message} = reparse(encode(row(1042)))

      assert Map.keys(message) |> Enum.sort() == ~w(data id message_type metadata)
    end

    # Three slots carry the row and two of them are SSE's own — which is what
    # makes a generic client's `Last-Event-ID` and `addEventListener` work with no
    # DMEx knowledge at all.
    test "the id is the row's own id, and the event is its message type" do
      assert {:message, message} = reparse(encode(row(1042)))

      assert message["id"] == 1042
      assert message["message_type"] == "MyApp.Messages.OrderPlaced"
    end

    test "splices row data verbatim rather than re-encoding it" do
      # Key order is preserved exactly, which a decode/re-encode round trip
      # through a map would not guarantee.
      data = ~s({"z":1,"a":2,"m":{"nested":[1,2,3]}})

      assert encode(row(1, data: data)) =~ ~s("data":#{data},)
    end

    test "splices row metadata verbatim" do
      assert encode(row(1, metadata: ~s({"trace_id":"abc"}))) =~
               ~s("metadata":{"trace_id":"abc"}})
    end

    test "emits exactly one data: line" do
      body = encode(row(1))

      assert body |> String.split("\n") |> Enum.count(&String.starts_with?(&1, "data: ")) == 1
    end

    test "is never pretty-printed and contains no stray newlines" do
      body = encode(row(1))

      # Three field lines plus the terminating blank line.
      assert body |> String.split("\n") |> length() == 5
      refute body =~ "\r"
    end

    test "a payload containing control characters cannot break framing" do
      # JSON escapes U+0000..U+001F inside strings by grammar, so a newline in a
      # value can never reach the wire as a literal newline.
      data = JSON.encode!(%{"note" => "line one\nline two\r\ttabbed"})
      body = encode(row(1, data: data))

      assert body |> String.split("\n") |> length() == 5

      assert {:message, %{"data" => %{"note" => "line one\nline two\r\ttabbed"}}} = reparse(body)
    end

    test "returns iodata, not a flattened binary" do
      assert row(1) |> Frame.encode_row() |> is_list()
    end
  end

  describe "encode_retry/1 and encode_heartbeat/0" do
    test "a retry frame carries no data and so dispatches nothing" do
      assert IO.iodata_to_binary(Frame.encode_retry(4200)) == "retry: 4200\n\n"
      assert reparse(Frame.encode_retry(4200)) == {:retry, 4200}
    end

    test "a heartbeat is a comment frame" do
      assert IO.iodata_to_binary(Frame.encode_heartbeat()) == ": hb\n\n"
    end

    test "a heartbeat parses to nothing at all" do
      assert reparse(Frame.encode_heartbeat()) == :ignore
    end
  end

  describe "parse/1" do
    test "decodes exactly one level — rows stay as maps, not structs" do
      assert {:ok, {:message, message}} = Frame.parse(block(encode(row(1042))))

      assert message["message_type"] == "MyApp.Messages.OrderPlaced"
      assert message["data"] == %{"order_id" => 7}
      assert message["metadata"] == %{}
    end

    test "tolerates CRLF and CR line terminators" do
      crlf = ~s(id: 5\r\nevent: T\r\ndata: {"data":{}})
      cr = ~s(id: 5\revent: T\rdata: {"data":{}})

      assert {:ok, {:message, %{"id" => 5, "message_type" => "T"}}} = Frame.parse(crlf)
      assert {:ok, {:message, %{"id" => 5, "message_type" => "T"}}} = Frame.parse(cr)
    end

    test "joins consecutive data: lines with LF, as the SSE spec requires" do
      block = ~s(id: 5\nevent: T\ndata: {"data":\ndata: {"a":1}})

      assert {:ok, {:message, %{"data" => %{"a" => 1}}}} = Frame.parse(block)
    end

    test "accepts a field with no space after the colon" do
      assert {:ok, {:message, %{"id" => 5}}} = Frame.parse(~s(id:5\nevent:T\ndata:{"data":{}}))
    end

    test "preserves further leading spaces, stripping only the first" do
      assert {:ok, {:message, %{"message_type" => " padded"}}} =
               Frame.parse(~s(id: 1\nevent:  padded\ndata: {"data":{}}))
    end

    test "ignores comment lines and unknown fields" do
      block = ~s(: a comment\nid: 5\nevent: T\nunknown: whatever\ndata: {"data":{}})

      assert {:ok, {:message, %{"id" => 5}}} = Frame.parse(block)
    end

    # A message needs all three, so a block missing any of them is not one. That
    # is not an error: per the SSE specification an event is dispatched only when
    # the data buffer is non-empty, and a log may send whatever else it
    # likes.
    test "a block short of id, event or data is ignored rather than rejected" do
      assert {:ok, :ignore} = Frame.parse(~s(event: T\ndata: {"data":{}}))
      assert {:ok, :ignore} = Frame.parse(~s(id: 5\ndata: {"data":{}}))
      assert {:ok, :ignore} = Frame.parse(~s(id: 5\nevent: T))
      assert {:ok, :ignore} = Frame.parse("")
    end

    # Including the `previous/current` pair the wire used to carry: a position
    # is one integer now, so the old spelling is simply not an id.
    test "a non-integer id is not an id, so the block is not a message" do
      assert {:ok, :ignore} = Frame.parse(~s(id: abc\nevent: T\ndata: {"data":{}}))
      assert {:ok, :ignore} = Frame.parse(~s(id: 4/5\nevent: T\ndata: {"data":{}}))
    end

    test "rejects malformed JSON" do
      assert {:error, {:invalid_json, _}} = Frame.parse(~s(id: 1\nevent: T\ndata: {not json}))
    end

    test "rejects a data payload that does not carry the row's data" do
      assert {:error, {:invalid_data_shape, _}} =
               Frame.parse(~s(id: 1\nevent: T\ndata: [1,2,3]))

      assert {:error, {:invalid_data_shape, _}} =
               Frame.parse(~s(id: 1\nevent: T\ndata: {"metadata":{}}))
    end
  end

  describe "round trip" do
    property "any row survives encode -> parse unchanged" do
      check all(
              id <- integer(1..1_000_000),
              type <- string(:alphanumeric),
              payload <- map_of(string(:alphanumeric), integer()),
              metadata <- map_of(string(:alphanumeric), integer())
            ) do
        row =
          row(id,
            message_type: "T" <> type,
            data: JSON.encode!(payload),
            metadata: JSON.encode!(metadata)
          )

        assert {:ok, {:message, message}} = Frame.parse(block(encode(row)))

        assert message["id"] == id
        assert message["message_type"] == "T" <> type
        assert message["data"] == payload
        assert message["metadata"] == metadata
      end
    end
  end

  # Strips the terminating blank line, which is the buffer's delimiter rather
  # than part of the frame block itself.
  defp block(encoded) do
    encoded |> IO.iodata_to_binary() |> String.trim_trailing("\n\n")
  end

  defp reparse(encoded) do
    {:ok, parsed} = encoded |> block() |> Frame.parse()
    parsed
  end
end
