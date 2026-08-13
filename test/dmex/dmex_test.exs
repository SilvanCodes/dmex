defmodule DMExTest do
  @moduledoc """
  Message-module discovery, and the application key it caches under.

  Both are load-bearing and neither was covered. `DMEx.Message.message?/1`
  decides what `use DMEx.Message` means at all — a handler's compile-time
  derivation and `append/2` both ask it — and it once answered `false` for every
  message module, which surfaced as a compile error in a fixture file rather
  than as a test. `DMEx.Message.types/0` is what a selective connect resolves a
  filter against.
  """

  use DMEx.Case, async: true

  alias DMEx.Test.Messages.OrderPlaced

  describe "DMEx.Message.message?/1" do
    test "is true for a module built with use DMEx.Message" do
      # The positive direction, which nothing else asserts: the negative one
      # holds just as well when the predicate is broken for everything.
      assert DMEx.Message.message?(OrderPlaced)
    end

    test "is false for a look-alike exporting type/0 and cast/1" do
      # The reason the predicate tests a persisted attribute rather than the
      # module's exports: this shape is one a struct can have by coincidence.
      defmodule NotAMessage do
        @moduledoc false
        defstruct [:order_id]
        def type, do: "order_placed"
        def cast(data), do: data
      end

      refute DMEx.Message.message?(NotAMessage)
    end

    test "is false for a module that does not exist" do
      refute DMEx.Message.message?(No.Such.Module)
    end
  end

  describe "DMEx.Message.discover/1" do
    test "finds the suite's message modules under :dmex" do
      discovered = DMEx.Message.discover(:dmex)

      assert OrderPlaced in discovered
      assert DMEx.Test.Messages.OrderShipped in discovered
      refute DMEx.Test.Handlers.Echo in discovered
    end

    test "is sorted, so a collision is reported against the same pair everywhere" do
      discovered = DMEx.Message.discover(:dmex)

      assert discovered == Enum.sort(discovered)
    end
  end

  describe "DMEx.Message.types/0" do
    test "holds the suite's types, whichever log a caller came from" do
      types = DMEx.Message.types()

      assert MapSet.member?(types, "order_placed")
      assert MapSet.member?(types, "order_shipped")
    end
  end

  describe "DMEx.backoff/3" do
    test "doubles from the base and never exceeds the cap" do
      for attempt <- 1..4 do
        assert DMEx.backoff(attempt, 100, 5_000) <= 100 * 2 ** (attempt - 1)
      end

      for attempt <- 1..64 do
        assert DMEx.backoff(attempt, 100, 5_000) in 1..5_000
      end
    end

    # A subscription whose origin is down counts `attempt` up for as long as it
    # runs — only a successful connect resets it. The closed form evaluated
    # `2 ** (attempt - 1)` before capping it, so this allocated a bignum with
    # thousands of digits per reconnect to answer `max`.
    test "costs nothing at an attempt count no cap will ever be reached from" do
      assert DMEx.backoff(1_000_000, 100, 5_000) in 1..5_000
    end
  end
end
