defmodule DMEx.Log.StatusTest do
  use DMEx.Case, async: true

  alias DMEx.Log.Status

  test "a fresh flag is clear" do
    refute Status.pending?(Status.new(5_000))
  end

  test "the first flip says send, the second says nothing more" do
    ref = Status.new(5_000)

    assert Status.flip(ref)
    refute Status.flip(ref)
    refute Status.flip(ref)
  end

  # This is the whole point: a stalled connection is blocked in chunk/2 and
  # cannot service its mailbox, so without the flag `:wakeup` messages would
  # pile up at the append rate for the duration of the stall.
  test "a stalled connection accumulates one wakeup, not one per append" do
    ref = Status.new(5_000)

    sent = Enum.count(1..1_000, fn _ -> Status.flip(ref) end)

    assert sent == 1
  end

  test "clearing re-arms it" do
    ref = Status.new(5_000)

    assert Status.flip(ref)
    :ok = Status.clear(ref)

    assert Status.flip(ref)
  end

  test "clearing something already clear is fine" do
    ref = Status.new(5_000)

    assert :ok = Status.clear(ref)
    refute Status.pending?(ref)
  end

  test "a loop with no flag at all still runs" do
    # The send loop is testable without a registry, and must not care.
    assert :ok = Status.clear(nil)
  end

  test "flags are per connection" do
    one = Status.new(5_000)
    two = Status.new(5_000)

    assert Status.flip(one)
    assert Status.flip(two)
    refute Status.flip(one)
  end

  # `:atomics.exchange/3` is a test-and-set rather than a read then a write, so
  # concurrent dispatchers cannot both conclude they should send.
  test "concurrent flips elect exactly one sender" do
    ref = Status.new(5_000)

    winners =
      1..50
      |> Task.async_stream(fn _ -> Status.flip(ref) end, max_concurrency: 50)
      |> Enum.count(fn {:ok, flipped?} -> flipped? end)

    assert winners == 1
  end

  describe "deadlines" do
    # Armed at creation, because the connect path writes headers and the retry:
    # frame before the send loop ever runs, and those writes stall like any
    # other. A status that started blank would leave that window unwatched.
    test "a fresh status is already armed" do
      now = System.monotonic_time(:millisecond)
      ref = Status.new(5_000)

      refute Status.overdue?(ref, now)
      assert Status.deadline(ref) >= now + 5_000
    end

    test "arming moves the deadline out by the budget" do
      ref = Status.new(1)

      Process.sleep(5)
      assert Status.overdue?(ref, System.monotonic_time(:millisecond))

      :ok = Status.arm(ref, 5_000)
      refute Status.overdue?(ref, System.monotonic_time(:millisecond))
    end

    # The watcher compares against a clock reading of its own, so the boundary
    # is worth pinning: a deadline exactly at `now` has not yet passed.
    test "overdue? is strict" do
      ref = Status.new(1_000)
      deadline = Status.deadline(ref)

      refute Status.overdue?(ref, deadline)
      assert Status.overdue?(ref, deadline + 1)
    end

    test ":infinity arms no deadline at all" do
      ref = Status.new(:infinity)

      refute Status.overdue?(ref, System.monotonic_time(:millisecond))

      # The far future rather than a special case every reader has to know
      # about, so `overdue?/2` stays one comparison.
      assert Status.deadline(ref) == 9_223_372_036_854_775_807
    end

    # Killing is asynchronous, so an entry outlives the signal by however long
    # the registry takes to process the :DOWN. Without this, overlapping sweeps
    # would reclaim — and report — the same connection twice.
    test "disarming makes an already-reclaimed connection stop looking overdue" do
      ref = Status.new(1)

      Process.sleep(5)
      assert Status.overdue?(ref, System.monotonic_time(:millisecond))

      :ok = Status.disarm(ref)

      refute Status.overdue?(ref, System.monotonic_time(:millisecond))
    end

    test "a loop with no status at all still runs" do
      # The send loop is testable without a registry, and must not care.
      assert :ok = Status.arm(nil, 5_000)
    end

    # Different slots of the same reference: the dispatcher writes one, the
    # stream writes the other, and neither may disturb the other's.
    test "the flag and the deadline do not disturb each other" do
      ref = Status.new(5_000)
      deadline = Status.deadline(ref)

      assert Status.flip(ref)
      assert Status.deadline(ref) == deadline

      :ok = Status.arm(ref, 10_000)
      assert Status.pending?(ref)
    end
  end

  describe "in a registry entry" do
    defmodule FakeLog do
      @moduledoc false
    end

    setup do
      start_supervised!(DMEx.Log.Registry.child_spec(FakeLog))

      :ok
    end

    test "registration hands back the connection's own status, armed" do
      now = System.monotonic_time(:millisecond)

      assert {:ok, ref} = DMEx.Log.Registry.register(FakeLog, 5_000)

      refute Status.pending?(ref)
      assert Status.flip(ref)
      refute Status.overdue?(ref, now)
    end

    test "the dispatcher reaches it through the entry" do
      {:ok, mine} = DMEx.Log.Registry.register(FakeLog, 5_000)

      DMEx.Log.Registry.each(FakeLog, fn _pid, status, _selection ->
        Status.flip(status)
      end)

      # Same flag, reached from another code path — which is what lets the
      # dispatcher coalesce without any shared table.
      assert Status.pending?(mine)
    end

    # No monitor, no sweep, no table: a connection cannot outlive its own status
    # because the status *is* part of its registry entry.
    test "the status dies with the connection, with nothing watching for it" do
      parent = self()

      pid =
        spawn(fn ->
          {:ok, _ref} = DMEx.Log.Registry.register(FakeLog, 5_000)
          send(parent, :registered)
          Process.sleep(:infinity)
        end)

      assert_receive :registered
      assert DMEx.Log.Registry.count(FakeLog) == 1

      Process.exit(pid, :kill)

      eventually(fn -> DMEx.Log.Registry.count(FakeLog) == 0 end)

      # Nothing left to iterate, so nothing left to leak.
      DMEx.Log.Registry.each(FakeLog, fn _pid, _status, _selection ->
        flunk("entry outlived process")
      end)
    end
  end
end
