defmodule DMEx.Producer.PendingTest do
  use DMEx.Case, async: true

  alias DMEx.Producer.Pending

  test "a fresh flag is clear" do
    refute Pending.pending?(Pending.new())
  end

  test "the first flip says send, the second says nothing more" do
    ref = Pending.new()

    assert Pending.flip(ref)
    refute Pending.flip(ref)
    refute Pending.flip(ref)
  end

  # This is the whole point: a stalled connection is blocked in chunk/2 and
  # cannot service its mailbox, so without the flag `:wakeup` messages would
  # pile up at the publish rate for the duration of the stall.
  test "a stalled connection accumulates one wakeup, not one per publish" do
    ref = Pending.new()

    sent = Enum.count(1..1_000, fn _ -> Pending.flip(ref) end)

    assert sent == 1
  end

  test "clearing re-arms it" do
    ref = Pending.new()

    assert Pending.flip(ref)
    :ok = Pending.clear(ref)

    assert Pending.flip(ref)
  end

  test "clearing something already clear is fine" do
    ref = Pending.new()

    assert :ok = Pending.clear(ref)
    refute Pending.pending?(ref)
  end

  test "a loop with no flag at all still runs" do
    # The send loop is testable without a registry, and must not care.
    assert :ok = Pending.clear(nil)
  end

  test "flags are per connection" do
    one = Pending.new()
    two = Pending.new()

    assert Pending.flip(one)
    assert Pending.flip(two)
    refute Pending.flip(one)
  end

  # `:atomics.exchange/3` is a test-and-set rather than a read then a write, so
  # concurrent dispatchers cannot both conclude they should send.
  test "concurrent flips elect exactly one sender" do
    ref = Pending.new()

    winners =
      1..50
      |> Task.async_stream(fn _ -> Pending.flip(ref) end, max_concurrency: 50)
      |> Enum.count(fn {:ok, flipped?} -> flipped? end)

    assert winners == 1
  end

  describe "in a registry entry" do
    defmodule FakeProducer do
      @moduledoc false
    end

    setup do
      start_supervised!(DMEx.Registry.child_spec(FakeProducer))

      :ok
    end

    test "registration hands back the connection's own flag" do
      assert {:ok, ref} = DMEx.Registry.register(FakeProducer)

      refute Pending.pending?(ref)
      assert Pending.flip(ref)
    end

    test "the dispatcher reaches it through the entry" do
      {:ok, mine} = DMEx.Registry.register(FakeProducer)

      DMEx.Registry.each(FakeProducer, fn _pid, pending -> Pending.flip(pending) end)

      # Same flag, reached from another code path — which is what lets the
      # dispatcher coalesce without any shared table.
      assert Pending.pending?(mine)
    end

    # No monitor, no sweep, no table: a connection cannot outlive its own flag
    # because the flag *is* part of its registry entry.
    test "the flag dies with the connection, with nothing watching for it" do
      parent = self()

      pid =
        spawn(fn ->
          {:ok, _ref} = DMEx.Registry.register(FakeProducer)
          send(parent, :registered)
          Process.sleep(:infinity)
        end)

      assert_receive :registered
      assert DMEx.Registry.count(FakeProducer) == 1

      Process.exit(pid, :kill)

      eventually(fn -> DMEx.Registry.count(FakeProducer) == 0 end)

      # Nothing left to iterate, so nothing left to leak.
      DMEx.Registry.each(FakeProducer, fn _pid, _pending -> flunk("entry outlived process") end)
    end
  end
end
