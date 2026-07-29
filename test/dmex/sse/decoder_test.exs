defmodule DMEx.SSE.DecoderTest do
  use DMEx.Case, async: true
  use ExUnitProperties

  alias DMEx.SSE.Decoder
  alias DMEx.SSE.Frame
  alias DMEx.Test.Storage, as: Stub

  describe "whole frames" do
    test "a frame arriving in one chunk yields one frame" do
      {:ok, [frame], _} = push(Decoder.new(), frame_bytes(1..2))

      assert %Frame{} = frame
      assert Enum.map(frame.messages, & &1["id"]) == [1, 2]
    end

    test "several frames in one chunk yield all of them, in order" do
      bytes = frame_bytes(1..1) <> frame_bytes(2..2) <> frame_bytes(3..3)

      {:ok, frames, _} = push(Decoder.new(), bytes)

      assert Enum.map(frames, &hd(&1.messages)["id"]) == [1, 2, 3]
    end

    test "carries frame metadata through" do
      {:ok, [frame], _} = push(Decoder.new(), frame_bytes(1..2))

      assert frame.metadata == %{"max_id" => 2}
      assert frame.id == 2
    end
  end

  describe "chunk boundaries are not frame boundaries" do
    test "a frame split across chunks yields one frame when it completes" do
      bytes = frame_bytes(1..3)
      {head, tail} = String.split_at(bytes, div(byte_size(bytes), 2))

      {:ok, [], decoder} = push(Decoder.new(), head)
      {:ok, [frame], _} = push(decoder, tail)

      assert Enum.map(frame.messages, & &1["id"]) == [1, 2, 3]
    end

    test "a frame split one byte at a time still yields exactly one frame" do
      bytes = frame_bytes(1..2)

      {frames, _decoder} =
        bytes
        |> chunks_of(1)
        |> Enum.reduce({[], Decoder.new()}, fn chunk, {acc, decoder} ->
          {:ok, frames, decoder} = push(decoder, chunk)
          {acc ++ frames, decoder}
        end)

      assert length(frames) == 1
    end

    test "a trailing partial frame is retained and completed by the next chunk" do
      bytes = frame_bytes(1..1) <> frame_bytes(2..2)
      cut = byte_size(frame_bytes(1..1)) + 10
      {head, tail} = String.split_at(bytes, cut)

      {:ok, [first], decoder} = push(Decoder.new(), head)
      assert hd(first.messages)["id"] == 1

      {:ok, [second], _} = push(decoder, tail)
      assert hd(second.messages)["id"] == 2
    end
  end

  describe "line terminators" do
    test "accepts LF, CRLF and CR blank lines" do
      body = ~s(data: {"metadata":{},"messages":[]})

      for delimiter <- ["\n\n", "\r\n\r\n", "\r\r"] do
        assert {:ok, [%Frame{messages: []}], _} = push(Decoder.new(), body <> delimiter)
      end
    end
  end

  describe "frames that are not delivery events" do
    test "a lone retry: frame yields nothing but is remembered" do
      {:ok, frames, decoder} =
        push(Decoder.new(), IO.iodata_to_binary(Frame.encode_retry(15_200)))

      assert frames == []
      assert Decoder.retry(decoder) == 15_200
    end

    test "a heartbeat comment yields nothing" do
      {:ok, frames, _} = push(Decoder.new(), IO.iodata_to_binary(Frame.encode_heartbeat()))

      assert frames == []
    end

    test "a retry: frame ahead of data does not swallow the data" do
      bytes = IO.iodata_to_binary(Frame.encode_retry(15_200)) <> frame_bytes(1..1)

      {:ok, [frame], decoder} = push(Decoder.new(), bytes)

      assert Decoder.retry(decoder) == 15_200
      assert hd(frame.messages)["id"] == 1
    end

    test "heartbeats interleaved with frames are dropped without losing frames" do
      hb = IO.iodata_to_binary(Frame.encode_heartbeat())
      bytes = hb <> frame_bytes(1..1) <> hb <> hb <> frame_bytes(2..2) <> hb

      {:ok, frames, _} = push(Decoder.new(), bytes)

      assert Enum.map(frames, &hd(&1.messages)["id"]) == [1, 2]
    end
  end

  describe "the buffer bound" do
    # A producer that never emits a blank line — broken, hostile, or simply
    # mid-frame when it wedged — would otherwise grow this without limit, and it
    # does so above any row-based queue bound, where that bound has no reach.
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
        assert {:ok, [_frame], decoder} = push(decoder, frame_bytes(id..id))
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
      assert {:error, {:invalid_json, _}, _} = push(Decoder.new(), "data: {not json}\n\n")
    end

    test "a data payload that is not a frame envelope is reported" do
      assert {:error, {:invalid_data_shape, _}, _} = push(Decoder.new(), "data: [1,2,3]\n\n")
    end

    test "an error leaves the decoder returnable rather than raising" do
      assert {:error, _reason, %Decoder{}} = push(Decoder.new(), "data: {oops\n\n")
    end
  end

  # Chunk-boundary bugs hide nowhere else: any framing that works for one split
  # can fail for another, so the split is what gets randomised.
  property "any sequence of frames survives any chunking" do
    check all(
            counts <- list_of(integer(1..4), min_length: 1, max_length: 5),
            chunk_size <- integer(1..64)
          ) do
      {bytes, expected} =
        Enum.reduce(counts, {"", []}, fn count, {bytes, ids} ->
          next = length(ids) + 1
          range = next..(next + count - 1)
          {bytes <> frame_bytes(range), ids ++ [Enum.to_list(range)]}
        end)

      {frames, _decoder} =
        bytes
        |> chunks_of(chunk_size)
        |> Enum.reduce({[], Decoder.new()}, fn chunk, {acc, decoder} ->
          {:ok, frames, decoder} = push(decoder, chunk)
          {acc ++ frames, decoder}
        end)

      assert Enum.map(frames, fn frame -> Enum.map(frame.messages, & &1["id"]) end) == expected
    end
  end

  ## Helpers

  defp push(decoder, chunk), do: Decoder.push(decoder, chunk)

  defp frame_bytes(range) do
    rows = Enum.map(range, &Stub.row/1)

    rows
    |> Frame.encode_batch(Enum.max(range))
    |> IO.iodata_to_binary()
  end

  defp chunks_of(binary, size) do
    Stream.unfold(binary, fn
      "" -> nil
      rest -> String.split_at(rest, min(size, byte_size(rest)))
    end)
  end
end
