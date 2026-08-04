defmodule DMEx.Log.RestartTest do
  @moduledoc """
  A crash inside one log's subtree must stay inside it.

  Both kills here are the same fault from OTP's point of view — a named
  subtree dies and is restarted at once — and the thing that used to break
  restart is that the *old* subtree's registered names outlive the exit signal
  that announced its death.
  """

  # Not async: the dispatcher's own process reads the log through the shared
  # sandbox, and these tests kill globally named processes.
  use DMEx.Case, async: false, database: true

  alias DMEx.Log.Registry, as: LogRegistry
  alias DMEx.Test.FastPollLog, as: Log
  alias DMEx.Test.Messages.OrderPlaced

  # Every dispatcher here logs its documented "falling back to polling" warning,
  # and a contained crash is a crash report either way.
  @moduletag :capture_log

  @sup DMEx.Log.RestartTest.Supervisor

  setup do
    start_supervised!(
      {DMEx.Supervisor, name: @sup, otp_app: nil, repo: DMEx.Test.Repo, logs: [test: Log]},
      restart: :temporary
    )

    :ok
  end

  describe "containment" do
    test "a killed log registry does not escalate, and the log serves again" do
      sup = Process.whereis(@sup)
      registry = Process.whereis(LogRegistry.name(Log))
      stream = live_stream()

      Process.exit(registry, :kill)

      assert_restarted(LogRegistry.name(Log), registry, sup)

      assert Process.whereis(@sup) == sup
      assert Process.alive?(sup)

      # A live stream is linked to the registry it registered in, so a registry
      # crash takes it down rather than leaving it registered nowhere.
      refute Process.alive?(stream)

      assert serving?()
    end

    test "a killed log supervisor does not escalate, and the log serves again" do
      sup = Process.whereis(@sup)
      log_sup = Process.whereis(Module.concat(Log, "Supervisor"))
      stream = live_stream()

      Process.exit(log_sup, :kill)

      assert_restarted(Module.concat(Log, "Supervisor"), log_sup, sup)

      assert Process.whereis(@sup) == sup
      assert Process.alive?(sup)
      refute Process.alive?(stream)

      assert serving?()
    end

    test "the registry survives a run of crashes rather than only the first" do
      sup = Process.whereis(@sup)

      # Three, because a fourth in the same five seconds is the log supervisor's
      # restart intensity doing its job rather than the bug.
      for _ <- 1..3 do
        registry = Process.whereis(LogRegistry.name(Log))
        Process.exit(registry, :kill)
        assert_restarted(LogRegistry.name(Log), registry, sup)
        assert Process.alive?(sup)
      end

      assert serving?()
    end

    test "a registry whose predecessor is still dying restarts once it is gone" do
      # The race, made deterministic. `Registry.Partition` traps exits, so it
      # only releases its registered name once it has been scheduled to handle
      # the kill — suspending it holds the name for exactly as long as we like,
      # which is what a loaded scheduler does by accident.
      sup = Process.whereis(@sup)
      registry = Process.whereis(LogRegistry.name(Log))
      partition = Process.whereis(Module.concat(LogRegistry.name(Log), "PIDPartition0"))

      :erlang.suspend_process(partition)
      Process.exit(registry, :kill)

      # Long enough for the log's supervisor to have exhausted its restart
      # intensity on a tree that cannot recover.
      Process.sleep(100)
      :erlang.resume_process(partition)

      assert_restarted(LogRegistry.name(Log), registry, sup)
      assert Process.alive?(sup)
      assert serving?()
    end
  end

  ## Helpers

  # Waits for `name` to be held by someone other than `was`, and says which of
  # the two things went wrong when it is not.
  defp assert_restarted(name, was, sup) do
    ok? =
      eventually(
        fn ->
          case Process.whereis(name) do
            nil -> not Process.alive?(sup) and :escalated
            ^was -> false
            pid when is_pid(pid) -> true
          end
        end,
        3_000
      )

    if ok? == :escalated do
      flunk("#{inspect(name)} never came back and DMEx.Supervisor died with it")
    end

    :ok
  end

  # Registers a process as a stream would, and leaves it registered.
  defp live_stream do
    parent = self()

    pid =
      spawn(fn ->
        {:ok, _status} = LogRegistry.register(Log, 5_000, :all)
        send(parent, :stream_ready)
        Process.sleep(:infinity)
      end)

    assert_receive :stream_ready, 2_000

    pid
  end

  # End to end through the parts that were killed: a fresh stream registers in
  # the new registry and the new dispatcher wakes it for a real append.
  defp serving? do
    parent = self()
    ref = make_ref()

    pid =
      spawn(fn ->
        {:ok, _status} = LogRegistry.register(Log, 5_000, :all)
        send(parent, {ref, :registered})

        receive do
          :wakeup -> send(parent, {ref, :woken})
        after
          3_000 -> :ok
        end
      end)

    assert_receive {^ref, :registered}, 2_000

    {:ok, _id} = Log.append(%OrderPlaced{order_id: System.unique_integer([:positive])})

    woken? =
      receive do
        {^ref, :woken} -> true
      after
        3_000 -> false
      end

    Process.exit(pid, :kill)

    woken?
  end
end
