defmodule DMEx.CaseTest do
  @moduledoc """
  Exercises the test infrastructure itself, so a broken harness fails loudly
  here rather than as confusing failures in every other suite.
  """

  use DMEx.Case, async: true

  describe "eventually/3" do
    test "returns as soon as the condition holds" do
      counter = :counters.new(1, [])

      result =
        eventually(fn ->
          :counters.add(counter, 1, 1)
          if :counters.get(counter, 1) >= 3, do: :done
        end)

      assert result == :done
      assert :counters.get(counter, 1) == 3
    end

    test "raises once the timeout elapses" do
      assert_raise ExUnit.AssertionError, fn ->
        eventually(fn -> false end, 60, 10)
      end
    end

    test "treats nil and false as not-yet, anything else as satisfied" do
      assert eventually(fn -> 0 end) == 0
      assert eventually(fn -> "" end) == ""
    end
  end
end
