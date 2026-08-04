defmodule DMEx.Log.RegistryTest do
  use DMEx.Case, async: true

  alias DMEx.Log.Registry, as: LogRegistry

  defmodule FakeLog do
    @moduledoc false
    def __dmex__, do: %{storage: DMEx.Storage.Postgres}
  end

  setup do
    start_supervised!(LogRegistry.child_spec(FakeLog))

    :ok
  end

  test "names the registry after the log" do
    assert LogRegistry.name(FakeLog) == DMEx.Log.RegistryTest.FakeLog.Registry
  end

  test "each/2 visits a registered process with its status and selection" do
    {:ok, status} = LogRegistry.register(FakeLog, 5_000, :all)

    LogRegistry.each(FakeLog, fn pid, entry_status, selection ->
      send(pid, {:visited, entry_status, selection})
    end)

    assert_received {:visited, ^status, :all}
  end

  test "each/2 visits every registered stream" do
    parent = self()

    pids =
      for _ <- 1..3 do
        spawn_link(fn ->
          {:ok, _status} = LogRegistry.register(FakeLog, 5_000, :all)
          send(parent, {:registered, self()})

          receive do
            :wakeup -> send(parent, {:woke, self()})
          end
        end)
      end

    for pid <- pids, do: assert_receive({:registered, ^pid})

    LogRegistry.each(FakeLog, fn pid, _status, _selection -> send(pid, :wakeup) end)

    for pid <- pids, do: assert_receive({:woke, ^pid})
  end

  test "counts live streams" do
    assert LogRegistry.count(FakeLog) == 0

    {:ok, _status} = LogRegistry.register(FakeLog, 5_000, :all)

    assert LogRegistry.count(FakeLog) == 1
  end

  # On the log side the request process *is* the stream, so a dead
  # connection takes its registry entry with it — there is nothing to clean up
  # and no way for a stale entry to outlive its connection.
  test "an entry dies with its process" do
    parent = self()

    pid =
      spawn(fn ->
        {:ok, _status} = LogRegistry.register(FakeLog, 5_000, :all)
        send(parent, :registered)

        receive do
          :stop -> :ok
        end
      end)

    assert_receive :registered
    assert LogRegistry.count(FakeLog) == 1

    ref = Process.monitor(pid)
    send(pid, :stop)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}

    eventually(fn -> LogRegistry.count(FakeLog) == 0 end)
  end

  # For a connect that registers and then fails before becoming a stream: its
  # process is the keep-alive connection process and does not die with it.
  test "unregister/1 drops the caller's entry while it is still alive" do
    {:ok, _status} = LogRegistry.register(FakeLog, 5_000, :all)
    assert LogRegistry.count(FakeLog) == 1

    assert :ok = LogRegistry.unregister(FakeLog)

    assert LogRegistry.count(FakeLog) == 0
  end

  test "each/2 over nobody is fine" do
    assert :ok =
             LogRegistry.each(FakeLog, fn pid, _status, _selection ->
               send(pid, :wakeup)
             end)
  end
end
