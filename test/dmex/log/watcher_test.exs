defmodule DMEx.Log.WatcherTest do
  use DMEx.Case, async: true

  alias DMEx.Log.Status
  alias DMEx.Log.Stream, as: SendLoop
  alias DMEx.Log.Streams
  alias DMEx.Log.Watcher
  alias DMEx.Test.ClosingAdapter
  alias DMEx.Test.ImpatientLog, as: Log
  alias DMEx.Test.Storage, as: Stub

  @moduletag :capture_log

  setup do
    start_supervised!(Streams.child_spec(Log))
    start_supervised!({Watcher, log: Log})

    :ok
  end

  describe "reclaiming" do
    test "kills a connection that missed its deadline" do
      pid = stream(1)

      assert_receive :registered

      eventually(fn -> not Process.alive?(pid) end)
    end

    test "leaves a connection that keeps arming alone" do
      pid = stream(:renewing)

      assert_receive :registered

      Process.sleep(100)

      assert Process.alive?(pid)
      assert Streams.count(Log) == 1
    end

    # The whole point of budgeting each step separately: a stream parked in its
    # idle wait is not making progress and must not be mistaken for one that is
    # stuck.
    test "leaves a connection parked on a long but budgeted wait alone" do
      pid = stream(60_000)

      assert_receive :registered

      Process.sleep(100)

      assert Process.alive?(pid)
    end

    test "reports each reclaim once, with how late it was" do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :stream, :reclaim]])

      stream(1)
      assert_receive :registered

      assert_receive {[:dmex, :stream, :reclaim], ^ref, %{overdue_by: overdue_by}, %{log: Log}}

      assert overdue_by >= 0

      # A kill is asynchronous, so the entry outlives the signal. Disarming as
      # part of reclaiming is what keeps the next sweep from reporting it again.
      refute_receive {[:dmex, :stream, :reclaim], ^ref, _, _}, 100
    end

    test "an empty log sweeps to nothing" do
      assert Watcher.sweep(Log) == 0
    end
  end

  describe "the send loop under it" do
    # This is the failure the socket layer structurally cannot see: the peer is
    # fine, the socket is fine, and the loop is wedged in a query that never
    # returns. `send_timeout` would never fire here.
    test "a loop wedged in a read is reclaimed" do
      opts = Stub.new()
      Stub.hang(opts)

      parent = self()

      pid =
        spawn(fn ->
          {:ok, status} = Streams.register(Log, 20, :all)
          send(parent, :registered)

          SendLoop.loop(ClosingAdapter.conn(parent, 100, state: :chunked), state(opts, status))
        end)

      assert_receive :registered

      eventually(fn -> not Process.alive?(pid) end)
    end
  end

  ## Harness

  defp state(opts, status) do
    %{
      SendLoop.connect(Log, transport: DMEx.Transport.SSE, status: status)
      | storage: Stub,
        storage_opts: opts
    }
  end

  # A stand-in for a stream process: it registers, arms, and then blocks exactly
  # as a wedged loop does — the watcher's whole input is the registry entry, so
  # it cannot tell this from the real thing.
  defp stream(budget) do
    parent = self()

    pid =
      spawn(fn ->
        {:ok, status} = Streams.register(Log, budget(budget), :all)
        send(parent, :registered)

        block(status, budget)
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)

    pid
  end

  defp budget(:renewing), do: 1_000
  defp budget(budget), do: budget

  defp block(status, :renewing) do
    Status.arm(status, 1_000)
    Process.sleep(5)

    block(status, :renewing)
  end

  defp block(_status, _budget), do: Process.sleep(:infinity)
end
