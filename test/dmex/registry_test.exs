defmodule DMEx.RegistryTest do
  use DMEx.Case, async: true

  alias DMEx.Registry, as: DMExRegistry

  defmodule FakeProducer do
    @moduledoc false
    def __dmex__, do: %{storage: DMEx.Storage.Postgres}
  end

  setup do
    start_supervised!(DMExRegistry.child_spec(FakeProducer))

    :ok
  end

  test "names the registry after the producer" do
    assert DMExRegistry.name(FakeProducer) == DMEx.RegistryTest.FakeProducer.Registry
  end

  test "a registered process receives broadcasts" do
    {:ok, _status} = DMExRegistry.register(FakeProducer, 5_000)

    DMExRegistry.broadcast(FakeProducer, :wakeup)

    assert_received :wakeup
  end

  test "every registered stream receives the same broadcast" do
    parent = self()

    pids =
      for _ <- 1..3 do
        spawn_link(fn ->
          {:ok, _status} = DMExRegistry.register(FakeProducer, 5_000)
          send(parent, {:registered, self()})

          receive do
            :wakeup -> send(parent, {:woke, self()})
          end
        end)
      end

    for pid <- pids, do: assert_receive({:registered, ^pid})

    DMExRegistry.broadcast(FakeProducer, :wakeup)

    for pid <- pids, do: assert_receive({:woke, ^pid})
  end

  test "counts live streams" do
    assert DMExRegistry.count(FakeProducer) == 0

    {:ok, _status} = DMExRegistry.register(FakeProducer, 5_000)

    assert DMExRegistry.count(FakeProducer) == 1
  end

  # On the producer side the request process *is* the stream, so a dead
  # connection takes its registry entry with it — there is nothing to clean up
  # and no way for a stale entry to outlive its connection.
  test "an entry dies with its process" do
    parent = self()

    pid =
      spawn(fn ->
        {:ok, _status} = DMExRegistry.register(FakeProducer, 5_000)
        send(parent, :registered)

        receive do
          :stop -> :ok
        end
      end)

    assert_receive :registered
    assert DMExRegistry.count(FakeProducer) == 1

    ref = Process.monitor(pid)
    send(pid, :stop)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}

    eventually(fn -> DMExRegistry.count(FakeProducer) == 0 end)
  end

  test "broadcasting to nobody is fine" do
    assert :ok = DMExRegistry.broadcast(FakeProducer, :wakeup)
  end
end
