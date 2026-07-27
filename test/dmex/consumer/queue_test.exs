defmodule DMEx.Consumer.QueueTest do
  use DMEx.Case, async: true

  alias DMEx.Consumer.Queue

  defp rows(range), do: Enum.map(range, &%{"id" => &1})

  describe "FIFO" do
    test "hands rows back in arrival order" do
      queue = Queue.new(100, 10) |> Queue.push(rows(1..3))

      assert {:ok, %{"id" => 1}, queue} = Queue.pop(queue)
      assert {:ok, %{"id" => 2}, queue} = Queue.pop(queue)
      assert {:ok, %{"id" => 3}, queue} = Queue.pop(queue)
      assert :empty = Queue.pop(queue)
    end

    test "keeps order across several frames" do
      queue =
        Queue.new(100, 10)
        |> Queue.push(rows(1..2))
        |> Queue.push(rows(3..4))

      assert drain(queue) == [1, 2, 3, 4]
    end

    test "an empty queue pops nothing" do
      assert :empty = Queue.pop(Queue.new(100, 10))
    end

    test "pushing nothing changes nothing" do
      queue = Queue.new(100, 10) |> Queue.push([])

      assert Queue.empty?(queue)
      assert Queue.depth(queue) == 0
    end
  end

  describe "depth" do
    test "tracks what is waiting" do
      queue = Queue.new(100, 10) |> Queue.push(rows(1..7))

      assert Queue.depth(queue) == 7

      {:ok, _row, queue} = Queue.pop(queue)

      assert Queue.depth(queue) == 6
    end

    test "counts every row, including ones that will be skipped" do
      # The lookup happens in the pipeline, not at the door — filtering here
      # would leave depth counting handled rows while max_batch_size counts wire
      # rows, and every use of one to compute the other would need a mix ratio.
      queue = Queue.new(100, 10) |> Queue.push(rows(1..5))

      assert Queue.depth(queue) == 5
    end
  end

  describe "the re-arm gate" do
    # This is what makes queue_limit a true bound rather than an approximate
    # one: rows arrive a frame at a time, so a gate of "below the limit" would
    # let a full frame land on a nearly-full queue and overshoot.
    test "re-arms only while a whole frame would still fit" do
      queue = Queue.new(100, 40)

      assert Queue.room?(queue)
      assert Queue.room?(Queue.push(queue, rows(1..60)))
      refute Queue.room?(Queue.push(queue, rows(1..61)))
    end

    test "peak depth is exactly the limit" do
      queue = Queue.new(100, 50)

      # Re-arm at the last moment the gate allows, then take a whole frame.
      filled = Queue.push(queue, rows(1..50))
      assert Queue.room?(filled)

      peaked = Queue.push(filled, rows(51..100))
      assert Queue.depth(peaked) == 100
      refute Queue.room?(peaked)
    end

    test "at the 2x default the gate is one frame's worth of depth" do
      queue = Queue.new(1_000, 500)

      assert Queue.room?(Queue.push(queue, rows(1..500)))
      refute Queue.room?(Queue.push(queue, rows(1..501)))
    end
  end

  describe "water marks" do
    # Convenient rather than coincidental: "depth above the high-water mark" and
    # "the socket is being held off" become one condition rather than two that
    # have to be kept consistent.
    test "the high-water mark is the same number as the re-arm headroom" do
      queue = Queue.new(1_000, 500)

      assert Queue.high_water(queue) == 500

      at_mark = Queue.push(queue, rows(1..500))
      assert Queue.room?(at_mark)
      assert Queue.depth(at_mark) == Queue.high_water(queue)
    end

    test "the low-water mark sits strictly below it, for hysteresis" do
      queue = Queue.new(1_000, 500)

      assert Queue.low_water(queue) < Queue.high_water(queue)
    end
  end

  describe "resize/2" do
    test "re-sizes the arrival quantum without disturbing what is queued" do
      queue = Queue.new(1_000, 500) |> Queue.push(rows(1..600))

      resized = Queue.resize(queue, 100)

      assert Queue.depth(resized) == 600
      # 600 + 100 <= 1_000, where 600 + 500 was not.
      refute Queue.room?(queue)
      assert Queue.room?(resized)
    end

    test "adaptation only shrinks the quantum, so the ratio can only widen" do
      queue = Queue.new(1_000, 500)

      assert Queue.high_water(Queue.resize(queue, 250)) > Queue.high_water(queue)
    end
  end

  defp drain(queue, acc \\ []) do
    case Queue.pop(queue) do
      {:ok, row, queue} -> drain(queue, acc ++ [row["id"]])
      :empty -> acc
    end
  end
end
