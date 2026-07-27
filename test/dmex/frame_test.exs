defmodule DMEx.FrameTest do
  use DMEx.Case, async: true
  use ExUnitProperties

  alias DMEx.Frame

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

  defp encode(rows, max_id), do: rows |> Frame.encode_batch(max_id) |> IO.iodata_to_binary()

  describe "encode_batch/2" do
    test "produces exactly the documented wire shape" do
      assert encode([row(1042)], 1180) ==
               "id: 1042\n" <>
                 "event: batch\n" <>
                 ~s(data: {"metadata":{"max_id":1180},"messages":[) <>
                 ~s({"id":1042,"message_type":"MyApp.Messages.OrderPlaced",) <>
                 ~s("data":{"order_id":7},"metadata":{},) <>
                 ~s("inserted_at":"2026-07-27T10:31:04.221Z"}]}\n\n)
    end

    test "the frame id is the highest message id, not the first or last" do
      assert %Frame{id: 1042} = reparse(encode([row(1041), row(1042), row(7)], 1180))
    end

    test "a single message is still a one-element batch" do
      assert %Frame{event: "batch", messages: [_only]} = reparse(encode([row(1)], 1))
    end

    test "splices row data verbatim rather than re-encoding it" do
      # Key order is preserved exactly, which a decode/re-encode round trip
      # through a map would not guarantee.
      data = ~s({"z":1,"a":2,"m":{"nested":[1,2,3]}})

      assert encode([row(1, data: data)], 1) =~ ~s("data":#{data},)
    end

    test "splices row metadata verbatim" do
      assert encode([row(1, metadata: ~s({"trace_id":"abc"}))], 1) =~
               ~s("metadata":{"trace_id":"abc"},)
    end

    test "escapes the message type through the JSON encoder" do
      assert encode([row(1, message_type: ~s(Weird"Type))], 1) =~
               ~s("message_type":"Weird\\"Type")
    end

    test "emits exactly one data: line, however many messages" do
      body = encode(Enum.map(1..50, &row/1), 100)

      assert body |> String.split("\n") |> Enum.count(&String.starts_with?(&1, "data: ")) == 1
    end

    test "is never pretty-printed and contains no stray newlines" do
      body = encode(Enum.map(1..20, &row/1), 100)

      # Three field lines plus the terminating blank line.
      assert body |> String.split("\n") |> length() == 5
      refute body =~ "\r"
    end

    test "a payload containing control characters cannot break framing" do
      # JSON escapes U+0000..U+001F inside strings by grammar, so a newline in a
      # value can never reach the wire as a literal newline.
      data = JSON.encode!(%{"note" => "line one\nline two\r\ttabbed"})
      body = encode([row(1, data: data)], 1)

      assert body |> String.split("\n") |> length() == 5

      assert %Frame{messages: [%{"data" => %{"note" => "line one\nline two\r\ttabbed"}}]} =
               reparse(body)
    end

    test "carries max_id in frame metadata, separate from any row" do
      assert %Frame{metadata: %{"max_id" => 9999}} = reparse(encode([row(1)], 9999))
    end

    test "accepts a NaiveDateTime as well as a DateTime" do
      assert encode([row(1, inserted_at: ~N[2026-07-27 10:31:04])], 1) =~
               ~s("inserted_at":"2026-07-27T10:31:04")
    end

    test "refuses to encode an empty batch" do
      assert_raise ArgumentError, ~r/empty batch/, fn -> Frame.encode_batch([], 5) end
    end

    test "returns iodata, not a flattened binary" do
      assert Frame.encode_batch([row(1)], 1) |> is_list()
    end
  end

  describe "encode_retry/1 and encode_heartbeat/0" do
    test "a retry frame carries no data and so dispatches nothing" do
      assert IO.iodata_to_binary(Frame.encode_retry(4200)) == "retry: 4200\n\n"
      assert %Frame{retry: 4200, messages: nil} = reparse(Frame.encode_retry(4200))
    end

    test "a heartbeat is a comment frame" do
      assert IO.iodata_to_binary(Frame.encode_heartbeat()) == ": hb\n\n"
    end

    test "a heartbeat parses to a frame carrying nothing at all" do
      assert %Frame{messages: nil, retry: nil, id: nil, event: nil} =
               reparse(Frame.encode_heartbeat())
    end
  end

  describe "parse/1" do
    test "decodes exactly one level — rows stay as maps, not structs" do
      assert {:ok, %Frame{messages: [message]}} =
               Frame.parse(block(encode([row(1042)], 1180)))

      assert message["message_type"] == "MyApp.Messages.OrderPlaced"
      assert message["data"] == %{"order_id" => 7}
      assert is_map(message["data"])
    end

    test "tolerates CRLF and CR line terminators" do
      crlf = "id: 5\r\nevent: batch\r\ndata: {\"metadata\":{},\"messages\":[]}"
      cr = "id: 5\revent: batch\rdata: {\"metadata\":{},\"messages\":[]}"

      assert {:ok, %Frame{id: 5, event: "batch", messages: []}} = Frame.parse(crlf)
      assert {:ok, %Frame{id: 5, event: "batch", messages: []}} = Frame.parse(cr)
    end

    test "joins consecutive data: lines with LF, as the SSE spec requires" do
      block = ~s(event: batch\ndata: {"metadata":{},\ndata: "messages":[]})

      assert {:ok, %Frame{messages: []}} = Frame.parse(block)
    end

    test "accepts a field with no space after the colon" do
      assert {:ok, %Frame{id: 5}} = Frame.parse(~s(id:5\ndata: {"messages":[]}))
    end

    test "preserves further leading spaces, stripping only the first" do
      assert {:ok, %Frame{event: " padded"}} =
               Frame.parse(~s(event:  padded\ndata: {"messages":[]}))
    end

    test "ignores comment lines and unknown fields" do
      block = ~s(: a comment\nid: 5\nunknown: whatever\ndata: {"messages":[]})

      assert {:ok, %Frame{id: 5, messages: []}} = Frame.parse(block)
    end

    test "ignores a non-integer id or retry rather than failing the frame" do
      assert {:ok, %Frame{id: nil, retry: nil, messages: []}} =
               Frame.parse(~s(id: abc\nretry: later\ndata: {"messages":[]}))
    end

    test "defaults absent frame metadata to an empty map" do
      assert {:ok, %Frame{metadata: %{}}} = Frame.parse(~s(data: {"messages":[]}))
    end

    test "rejects malformed JSON" do
      assert {:error, {:invalid_json, _}} = Frame.parse(~s(data: {not json}))
    end

    test "rejects a data payload that is not an object carrying messages" do
      assert {:error, {:invalid_data_shape, _}} = Frame.parse(~s(data: [1,2,3]))
      assert {:error, {:invalid_data_shape, _}} = Frame.parse(~s(data: {"messages":"nope"}))
    end
  end

  describe "round trip" do
    property "any batch survives encode -> parse unchanged" do
      check all(
              messages <-
                list_of(
                  tuple(
                    {integer(1..1_000_000), string(:alphanumeric),
                     map_of(string(:alphanumeric), integer())}
                  ),
                  min_length: 1,
                  max_length: 20
                ),
              max_id <- integer(1..2_000_000)
            ) do
        rows =
          messages
          |> Enum.uniq_by(fn {id, _, _} -> id end)
          |> Enum.map(fn {id, type, payload} ->
            row(id, message_type: "T" <> type, data: JSON.encode!(payload))
          end)

        encoded = encode(rows, max_id)
        assert {:ok, %Frame{} = frame} = Frame.parse(block(encoded))

        assert frame.event == "batch"
        assert frame.id == rows |> Enum.map(& &1.id) |> Enum.max()
        assert frame.metadata == %{"max_id" => max_id}
        assert Enum.map(frame.messages, & &1["id"]) == Enum.map(rows, & &1.id)

        assert Enum.map(frame.messages, & &1["data"]) ==
                 Enum.map(rows, &JSON.decode!(&1.data))
      end
    end
  end

  # Strips the terminating blank line, which is the buffer's delimiter rather
  # than part of the frame block itself.
  defp block(encoded) do
    encoded |> IO.iodata_to_binary() |> String.trim_trailing("\n\n")
  end

  defp reparse(encoded) do
    {:ok, frame} = encoded |> block() |> Frame.parse()
    frame
  end
end
