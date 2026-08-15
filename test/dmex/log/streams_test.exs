defmodule DMEx.Log.StreamsTest do
  use DMEx.Case, async: true

  alias DMEx.Log.Status
  alias DMEx.Log.Streams

  defmodule FakeLog do
    @moduledoc false
    def __dmex__, do: %{storage: %{adapter: DMEx.Storage.Postgres}}
  end

  setup do
    start_supervised!(Streams.child_spec(FakeLog))

    :ok
  end

  # The suffix is `Registry`, not `Streams`: it names the running Elixir
  # `Registry`, not this module. See `DMEx.Log.Streams`.
  test "names the registry after the log" do
    assert Streams.name(FakeLog) == DMEx.Log.StreamsTest.FakeLog.Registry
  end

  test "all/1 hands back a registered process with its status and selection" do
    {:ok, status} = Streams.register(FakeLog, 5_000, :all)
    me = self()

    assert [{^me, ^status, :all}] = Streams.all(FakeLog)

    # Registration hands back a status already armed against the connect-path
    # writes, which happen before the send loop exists.
    refute Status.overdue?(status, System.monotonic_time(:millisecond))
  end

  test "all/1 hands back every registered stream" do
    parent = self()

    pids =
      for _ <- 1..3 do
        spawn_link(fn ->
          {:ok, _status} = Streams.register(FakeLog, 5_000, :all)
          send(parent, {:registered, self()})

          receive do
            :wakeup -> send(parent, {:woke, self()})
          end
        end)
      end

    for pid <- pids, do: assert_receive({:registered, ^pid})

    for {pid, _status, _selection} <- Streams.all(FakeLog), do: send(pid, :wakeup)

    for pid <- pids, do: assert_receive({:woke, ^pid})
  end

  test "counts live streams" do
    assert Streams.count(FakeLog) == 0

    {:ok, _status} = Streams.register(FakeLog, 5_000, :all)

    assert Streams.count(FakeLog) == 1
  end

  # On the log side the request process *is* the stream, so a dead
  # connection takes its registry entry with it — there is nothing to clean up
  # and no way for a stale entry to outlive its connection.
  test "an entry dies with its process" do
    parent = self()

    pid =
      spawn(fn ->
        {:ok, _status} = Streams.register(FakeLog, 5_000, :all)
        send(parent, :registered)

        receive do
          :stop -> :ok
        end
      end)

    assert_receive :registered
    assert Streams.count(FakeLog) == 1

    ref = Process.monitor(pid)
    send(pid, :stop)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}

    eventually(fn -> Streams.count(FakeLog) == 0 end)
  end

  # For a connect that registers and then fails before becoming a stream: its
  # process is the keep-alive connection process and does not die with it.
  test "unregister/1 drops the caller's entry while it is still alive" do
    {:ok, _status} = Streams.register(FakeLog, 5_000, :all)
    assert Streams.count(FakeLog) == 1

    assert :ok = Streams.unregister(FakeLog)

    assert Streams.count(FakeLog) == 0
  end

  test "all/1 over nobody is an empty list" do
    assert Streams.all(FakeLog) == []
  end
end
