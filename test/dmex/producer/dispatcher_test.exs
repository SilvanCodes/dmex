defmodule DMEx.Producer.DispatcherTest do
  # Not async: the sandbox has to be in shared mode so the dispatcher's own
  # process sees the rows this test writes.
  use DMEx.Case, async: false, database: true

  alias DMEx.Producer.Dispatcher
  alias DMEx.Producer.Registry, as: ProducerRegistry
  alias DMEx.Producer.Status
  alias DMEx.Test.Messages.OrderPlaced

  # No notifier runs in this suite, so every dispatcher logs its documented
  # "falling back to polling" warning. That is the behaviour under test, not
  # noise worth reading nine times.
  @moduletag :capture_log

  # Fast enough that a test does not wait on it, slow enough that "one poll" is
  # still distinguishable from "none".
  @poll 50

  defmodule FastProducer do
    @moduledoc false
    use DMEx.Producer, repo: DMEx.Test.Repo, poll_interval: 50
  end

  setup do
    start_supervised!(ProducerRegistry.child_spec(FastProducer))

    :ok
  end

  describe "polling" do
    # LISTEN/NOTIFY only fires at COMMIT and the sandbox never commits, so this
    # suite exercises the polling half deliberately. The notification half needs
    # real commits and lives in test/integration.
    test "wakes registered streams after a publish" do
      start_dispatcher()
      {:ok, _status} = ProducerRegistry.register(FastProducer, 5_000)

      {:ok, _} = FastProducer.publish(%OrderPlaced{order_id: 1})

      assert_receive :wakeup, 1_000
    end

    # A poll that broadcasts unconditionally is a thundering herd: at a thousand
    # connections and a one-second interval, a thousand reads per second on a
    # completely idle log. The guard is that max(id) has to have moved.
    test "an idle log produces no wakeups at all" do
      start_dispatcher()
      {:ok, _status} = ProducerRegistry.register(FastProducer, 5_000)

      refute_receive :wakeup, @poll * 5
    end

    test "stops waking once the log stops moving" do
      start_dispatcher()
      {:ok, _status} = ProducerRegistry.register(FastProducer, 5_000)

      {:ok, _} = FastProducer.publish(%OrderPlaced{order_id: 1})
      assert_receive :wakeup, 1_000

      drain_wakeups()

      refute_receive :wakeup, @poll * 5
    end

    test "a log with history does not announce that history on startup" do
      # Seeded before the dispatcher starts, so its first max(id) read is the
      # tip rather than 0.
      {:ok, _} = FastProducer.publish(%OrderPlaced{order_id: 1})

      start_dispatcher()
      {:ok, _status} = ProducerRegistry.register(FastProducer, 5_000)

      refute_receive :wakeup, @poll * 5
    end

    test "coalesces a burst into far fewer broadcasts than publishes" do
      start_dispatcher()
      {:ok, _status} = ProducerRegistry.register(FastProducer, 5_000)

      for n <- 1..20, do: {:ok, _} = FastProducer.publish(%OrderPlaced{order_id: n})

      assert_receive :wakeup, 1_000

      # The poll broadcasts at most once per interval however many rows landed
      # in it, so twenty publishes cannot cost twenty passes.
      Process.sleep(@poll * 3)
      assert count_wakeups() < 20
    end

    test "wakes every registered stream, not just one" do
      start_dispatcher()

      parent = self()

      pids =
        for _ <- 1..3 do
          spawn_link(fn ->
            {:ok, _status} = ProducerRegistry.register(FastProducer, 5_000)
            send(parent, {:registered, self()})

            receive do
              :wakeup -> send(parent, {:woke, self()})
            end
          end)
        end

      for pid <- pids, do: assert_receive({:registered, ^pid})

      {:ok, _} = FastProducer.publish(%OrderPlaced{order_id: 1})

      for pid <- pids, do: assert_receive({:woke, ^pid}, 1_000)
    end
  end

  describe "wakeup coalescing" do
    # A stalled connection cannot service its mailbox, so without the flag its
    # wakeups would accumulate at the publish rate for the whole stall. The
    # registered process here never clears, which is exactly that situation.
    test "a connection that has not cleared its flag is told nothing further" do
      start_dispatcher()
      {:ok, _status} = ProducerRegistry.register(FastProducer, 5_000)

      for n <- 1..10 do
        {:ok, _} = FastProducer.publish(%OrderPlaced{order_id: n})
        Process.sleep(@poll * 2)
      end

      # Ten publishes across ten poll intervals, and one queued wakeup.
      assert count_wakeups() == 1
    end

    test "clearing the flag re-arms the connection for the next publish" do
      start_dispatcher()
      {:ok, status} = ProducerRegistry.register(FastProducer, 5_000)

      {:ok, _} = FastProducer.publish(%OrderPlaced{order_id: 1})
      assert_receive :wakeup, 1_000

      # This is what the send loop does at the top of every iteration.
      :ok = Status.clear(status)

      {:ok, _} = FastProducer.publish(%OrderPlaced{order_id: 2})
      assert_receive :wakeup, 1_000
    end
  end

  describe "telemetry" do
    test "reports how many streams a broadcast reached" do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :dispatcher, :broadcast]])

      start_dispatcher()
      {:ok, _status} = ProducerRegistry.register(FastProducer, 5_000)

      {:ok, _} = FastProducer.publish(%OrderPlaced{order_id: 1})

      assert_receive {[:dmex, :dispatcher, :broadcast], ^ref, measurements, metadata}, 1_000
      assert measurements.streams == 1
      assert measurements.woken == 1
      assert metadata.producer == FastProducer
    end
  end

  describe "resilience" do
    # This is the property that makes polling the belt to NOTIFY's suspenders:
    # with no notifier process running at all, dispatch still works. It is also
    # the SQLite path, where no notification mechanism exists in the first place.
    test "runs polling-only when no notifier is reachable" do
      start_dispatcher()
      {:ok, _status} = ProducerRegistry.register(FastProducer, 5_000)

      {:ok, _} = FastProducer.publish(%OrderPlaced{order_id: 1})

      assert_receive :wakeup, 1_000
      assert Process.alive?(Process.whereis(Dispatcher.name(FastProducer)))
    end

    # The producer's send loop reads on every iteration, including its receive
    # timeout, so a dispatcher that is down or late costs latency and never
    # correctness. That is what makes it safe to start one after connections
    # already exist.
    test "streams registered before the dispatcher starts still get woken" do
      {:ok, _status} = ProducerRegistry.register(FastProducer, 5_000)

      start_dispatcher()

      {:ok, _} = FastProducer.publish(%OrderPlaced{order_id: 1})

      assert_receive :wakeup, 1_000
    end
  end

  ## Helpers

  defp start_dispatcher do
    start_supervised!({Dispatcher, producer: FastProducer})
  end

  defp drain_wakeups do
    receive do
      :wakeup -> drain_wakeups()
    after
      0 -> :ok
    end
  end

  defp count_wakeups(acc \\ 0) do
    receive do
      :wakeup -> count_wakeups(acc + 1)
    after
      0 -> acc
    end
  end
end
