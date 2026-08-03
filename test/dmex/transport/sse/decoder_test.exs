defmodule DMEx.Transport.SSE.DecoderTest do
  use DMEx.Case, async: true
  use ExUnitProperties

  alias DMEx.Test.Storage, as: Stub
  alias DMEx.Transport.SSE.Decoder
  alias DMEx.Transport.SSE.Frame

  describe "whole rows" do
    test "a frame arriving in one chunk yields one row" do
      {:ok, [row], _} = push(Decoder.new(), frame_bytes(1..1))

      assert row["id"] == 1
      assert row["message_type"] == "order_placed"
      assert is_map(row["data"])
    end

    test "several frames in one chunk yield all of them, in order" do
      {:ok, rows, _} = push(Decoder.new(), frame_bytes(1..3))

      assert Enum.map(rows, & &1["id"]) == [1, 2, 3]
    end
  end

  describe "chunk boundaries are not frame boundaries" do
    test "a frame split across chunks yields its row when it completes" do
      bytes = frame_bytes(1..1)
      {head, tail} = String.split_at(bytes, div(byte_size(bytes), 2))

      {:ok, [], decoder} = push(Decoder.new(), head)
      {:ok, [row], _} = push(decoder, tail)

      assert row["id"] == 1
    end

    test "a frame split one byte at a time still yields exactly one row" do
      {rows, _decoder} =
        1..1
        |> frame_bytes()
        |> chunks_of(1)
        |> Enum.reduce({[], Decoder.new()}, fn chunk, {acc, decoder} ->
          {:ok, rows, decoder} = push(decoder, chunk)
          {acc ++ rows, decoder}
        end)

      assert length(rows) == 1
    end

    test "a trailing partial frame is retained and completed by the next chunk" do
      bytes = frame_bytes(1..2)
      cut = byte_size(frame_bytes(1..1)) + 10
      {head, tail} = String.split_at(bytes, cut)

      {:ok, [first], decoder} = push(Decoder.new(), head)
      assert first["id"] == 1

      {:ok, [second], _} = push(decoder, tail)
      assert second["id"] == 2
    end
  end

  describe "line terminators" do
    test "accepts LF, CRLF and CR blank lines" do
      body = ~s(id: 1\nevent: T\ndata: {"data":{}})

      for delimiter <- ["\n\n", "\r\n\r\n", "\r\r"] do
        assert {:ok, [%{"id" => 1}], _} = push(Decoder.new(), body <> delimiter)
      end
    end
  end

  describe "frames that are not delivery events" do
    test "a lone retry: frame yields nothing but is remembered" do
      {:ok, rows, decoder} =
        push(Decoder.new(), IO.iodata_to_binary(Frame.encode_retry(15_200)))

      assert rows == []
      assert Decoder.retry(decoder) == 15_200
    end

    test "a heartbeat comment yields nothing" do
      {:ok, rows, _} = push(Decoder.new(), IO.iodata_to_binary(Frame.encode_heartbeat()))

      assert rows == []
    end

    test "a retry: frame ahead of data does not swallow the data" do
      bytes = IO.iodata_to_binary(Frame.encode_retry(15_200)) <> frame_bytes(1..1)

      {:ok, [row], decoder} = push(Decoder.new(), bytes)

      assert Decoder.retry(decoder) == 15_200
      assert row["id"] == 1
    end

    test "heartbeats interleaved with frames are dropped without losing rows" do
      hb = IO.iodata_to_binary(Frame.encode_heartbeat())
      bytes = hb <> frame_bytes(1..1) <> hb <> hb <> frame_bytes(2..2) <> hb

      {:ok, rows, _} = push(Decoder.new(), bytes)

      assert Enum.map(rows, & &1["id"]) == [1, 2]
    end
  end

  describe "the buffer bound" do
    # A log that never emits a blank line — broken, hostile, or simply
    # mid-frame when it wedged — would otherwise grow this without limit.
    test "an unterminated frame past max_bytes is refused" do
      decoder = Decoder.new(100)

      assert {:error, :frame_too_large, _} = push(decoder, String.duplicate("x", 101))
    end

    test "accumulates across chunks before refusing" do
      decoder = Decoder.new(100)

      {:ok, [], decoder} = push(decoder, String.duplicate("x", 60))

      assert {:error, :frame_too_large, _} = push(decoder, String.duplicate("x", 60))
    end

    test "the bound applies to the unterminated remainder, not to traffic seen" do
      # Ten complete frames well past the cap in total, but never more than one
      # frame outstanding, so nothing is refused.
      decoder = Decoder.new(2_000)

      Enum.reduce(1..10, decoder, fn id, decoder ->
        assert {:ok, [_row], decoder} = push(decoder, frame_bytes(id..id))
        decoder
      end)
    end

    test "a complete frame exactly at the bound is accepted" do
      bytes = frame_bytes(1..1)

      assert {:ok, [_], _} = push(Decoder.new(byte_size(bytes)), bytes)
    end
  end

  describe "malformed input" do
    test "invalid JSON in a data field is reported" do
      assert {:error, {:invalid_json, _}, _} =
               push(Decoder.new(), "id: 1\nevent: T\ndata: {not json}\n\n")
    end

    test "a data payload that is not a row is reported" do
      assert {:error, {:invalid_data_shape, _}, _} =
               push(Decoder.new(), "id: 1\nevent: T\ndata: [1,2,3]\n\n")
    end

    test "an error leaves the decoder returnable rather than raising" do
      assert {:error, _reason, %Decoder{}} =
               push(Decoder.new(), "id: 1\nevent: T\ndata: {oops\n\n")
    end
  end

  # Chunk-boundary bugs hide nowhere else: any framing that works for one split
  # can fail for another, so the split is what gets randomised.
  property "any sequence of frames survives any chunking" do
    check all(
            count <- integer(1..12),
            chunk_size <- integer(1..64)
          ) do
      {rows, _decoder} =
        1..count
        |> frame_bytes()
        |> chunks_of(chunk_size)
        |> Enum.reduce({[], Decoder.new()}, fn chunk, {acc, decoder} ->
          {:ok, rows, decoder} = push(decoder, chunk)
          {acc ++ rows, decoder}
        end)

      assert Enum.map(rows, & &1["id"]) == Enum.to_list(1..count)
    end
  end

  ## Helpers

  defp push(decoder, chunk), do: Decoder.push(decoder, chunk)

  # One frame per row, exactly as the log's send loop writes them.
  defp frame_bytes(range) do
    range
    |> Enum.map(&Frame.encode_row(Stub.row(&1)))
    |> IO.iodata_to_binary()
  end

  defp chunks_of(binary, size) do
    Stream.unfold(binary, fn
      "" -> nil
      rest -> String.split_at(rest, min(size, byte_size(rest)))
    end)
  end
end
