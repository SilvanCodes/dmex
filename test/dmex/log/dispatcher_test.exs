defmodule DMEx.Log.DispatcherTest do
  # Not async: the sandbox has to be in shared mode so the dispatcher's own
  # process sees the rows this test writes.
  use DMEx.Case, async: false, database: true

  alias DMEx.Log.Dispatcher
  alias DMEx.Log.Status
  alias DMEx.Log.Streams
  alias DMEx.Test.BeamNotifiedLog, as: BeamLog
  alias DMEx.Test.FastPollLog, as: FastLog
  alias DMEx.Test.Messages.OrderPlaced

  # No notifier runs in this suite, so every dispatcher logs its documented
  # "falling back to polling" warning. That is the behaviour under test, not
  # noise worth reading nine times.
  @moduletag :capture_log

  # Fast enough that a test does not wait on it, slow enough that "one poll" is
  # still distinguishable from "none".
  @poll 50

  setup do
    start_supervised!(Streams.child_spec(FastLog))

    :ok
  end

  describe "polling" do
    # LISTEN/NOTIFY only fires at COMMIT and the sandbox never commits, so this
    # suite exercises the polling half deliberately. The notification half needs
    # real commits and lives in test/integration.
    test "wakes registered streams after an append" do
      start_dispatcher()
      {:ok, _status} = Streams.register(FastLog, 5_000, :all)

      {:ok, _} = FastLog.append(%OrderPlaced{order_id: 1})

      assert_receive :wakeup, 1_000
    end

    # A poll that broadcasts unconditionally is a thundering herd: at a thousand
    # connections and a one-second interval, a thousand reads per second on a
    # completely idle log. The guard is that max(id) has to have moved.
    test "an idle log produces no wakeups at all" do
      start_dispatcher()
      {:ok, _status} = Streams.register(FastLog, 5_000, :all)

      refute_receive :wakeup, @poll * 5
    end

    test "stops waking once the log stops moving" do
      start_dispatcher()
      {:ok, _status} = Streams.register(FastLog, 5_000, :all)

      {:ok, _} = FastLog.append(%OrderPlaced{order_id: 1})
      assert_receive :wakeup, 1_000

      drain_wakeups()

      refute_receive :wakeup, @poll * 5
    end

    test "a log with history does not announce that history on startup" do
      # Seeded before the dispatcher starts, so its first max(id) read is the
      # tip rather than 0.
      {:ok, _} = FastLog.append(%OrderPlaced{order_id: 1})

      start_dispatcher()
      {:ok, _status} = Streams.register(FastLog, 5_000, :all)

      refute_receive :wakeup, @poll * 5
    end

    test "coalesces a burst into far fewer broadcasts than appends" do
      start_dispatcher()
      {:ok, _status} = Streams.register(FastLog, 5_000, :all)

      for n <- 1..20, do: {:ok, _} = FastLog.append(%OrderPlaced{order_id: n})

      assert_receive :wakeup, 1_000

      # The poll broadcasts at most once per interval however many rows landed
      # in it, so twenty appends cannot cost twenty passes.
      Process.sleep(@poll * 3)
      assert count_wakeups() < 20
    end

    test "wakes every registered stream, not just one" do
      start_dispatcher()

      parent = self()

      pids =
        for _ <- 1..3 do
          spawn_link(fn ->
            {:ok, _status} = Streams.register(FastLog, 5_000, :all)
            send(parent, {:registered, self()})

            receive do
              :wakeup -> send(parent, {:woke, self()})
            end
          end)
        end

      for pid <- pids, do: assert_receive({:registered, ^pid})

      {:ok, _} = FastLog.append(%OrderPlaced{order_id: 1})

      for pid <- pids, do: assert_receive({:woke, ^pid}, 1_000)
    end
  end

  describe "notifications" do
    test "wake a stream without waiting for the poll" do
      %{notifier: notifier, notifier_opts: notifier_opts} = BeamLog.__dmex__()

      start_supervised!(Streams.child_spec(BeamLog))
      for spec <- notifier.child_specs(notifier_opts), do: start_supervised!(spec)

      pid = start_supervised!({Dispatcher, log: BeamLog})

      # The subscription happens in `handle_continue/2`, so sync on it before
      # appending — otherwise this races the very thing it is testing.
      :sys.get_state(pid)

      {:ok, _status} = Streams.register(BeamLog, 5_000, :all)

      {:ok, _} = BeamLog.append(%OrderPlaced{order_id: 1})

      assert_receive :wakeup, 1_000
    end
  end

  describe "wakeup coalescing" do
    # A stalled connection cannot service its mailbox, so without the flag its
    # wakeups would accumulate at the append rate for the whole stall. The
    # registered process here never clears, which is exactly that situation.
    test "a connection that has not cleared its flag is told nothing further" do
      start_dispatcher()
      {:ok, _status} = Streams.register(FastLog, 5_000, :all)

      for n <- 1..10 do
        {:ok, _} = FastLog.append(%OrderPlaced{order_id: n})
        Process.sleep(@poll * 2)
      end

      # Ten appends across ten poll intervals, and one queued wakeup.
      assert count_wakeups() == 1
    end

    test "clearing the flag re-arms the connection for the next append" do
      start_dispatcher()
      {:ok, status} = Streams.register(FastLog, 5_000, :all)

      {:ok, _} = FastLog.append(%OrderPlaced{order_id: 1})
      assert_receive :wakeup, 1_000

      # This is what the send loop does at the top of every iteration.
      :ok = Status.clear(status)

      {:ok, _} = FastLog.append(%OrderPlaced{order_id: 2})
      assert_receive :wakeup, 1_000
    end
  end

  describe "telemetry" do
    test "reports how many streams a broadcast reached" do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :dispatcher, :broadcast]])

      start_dispatcher()
      {:ok, _status} = Streams.register(FastLog, 5_000, :all)

      {:ok, _} = FastLog.append(%OrderPlaced{order_id: 1})

      assert_receive {[:dmex, :dispatcher, :broadcast], ^ref, measurements, metadata}, 1_000
      assert measurements.streams == 1
      assert measurements.woken == 1
      assert metadata.log == FastLog
    end

    # Both numbers come out of the one walk now, so this pins that they still
    # answer different questions: everyone registered, versus everyone told.
    test "counts every registered stream, not only the woken ones" do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :dispatcher, :broadcast]])

      start_dispatcher()

      spawn_stream(MapSet.new(["order_placed"]))
      spawn_stream(MapSet.new(["order_shipped"]))

      {:ok, _} = FastLog.append(%OrderPlaced{order_id: 1})

      assert_receive {[:dmex, :dispatcher, :broadcast], ^ref, measurements, _metadata}, 1_000
      assert measurements.streams == 2
      assert measurements.woken == 1
    end
  end

  describe "resilience" do
    # This is the property that makes polling the belt to NOTIFY's suspenders:
    # with no notifier process running at all, dispatch still works. It is also
    # the SQLite path, where no notification mechanism exists in the first place.
    test "runs polling-only when no notifier is reachable" do
      start_dispatcher()
      {:ok, _status} = Streams.register(FastLog, 5_000, :all)

      {:ok, _} = FastLog.append(%OrderPlaced{order_id: 1})

      assert_receive :wakeup, 1_000
      assert Process.alive?(Process.whereis(Dispatcher.name(FastLog)))
    end

    # The log's send loop reads on every iteration, including its receive
    # timeout, so a dispatcher that is down or late costs latency and never
    # correctness. That is what makes it safe to start one after connections
    # already exist.
    test "streams registered before the dispatcher starts still get woken" do
      {:ok, _status} = Streams.register(FastLog, 5_000, :all)

      start_dispatcher()

      {:ok, _} = FastLog.append(%OrderPlaced{order_id: 1})

      assert_receive :wakeup, 1_000
    end

    # A notifier whose message is not `DMEx.Notifier`'s tuple leaves this
    # dispatcher polling-only forever. `listen/1` succeeded, so the LISTEN
    # warning never fires and the failure is otherwise completely silent.
    test "says so once when it is sent something it does not understand" do
      pid = start_dispatcher()

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          send(pid, {:notification, self(), make_ref(), "wrong", "shape", :extra})
          send(pid, :also_not_a_notification)
          :sys.get_state(pid)
        end)

      assert log =~ "unexpected message"
      assert [_once] = Regex.scan(~r/unexpected message/, log)
    end
  end

  # The read-amplification lever. Without routing, every append wakes every
  # connection and each one issues a read that finds nothing — so a log with a
  # hundred types costs a selective consumer a hundred times the reads it needs.
  #
  # These exercise the *poll* path, which is what this suite can reach: the
  # sandbox never commits, so LISTEN/NOTIFY never fires. That makes them the
  # important half to pin here, because the poll is the path that had no way to
  # name a type at all until `appended_since/2` — and an unrouted poll would
  # silently undo the routing on any log busier than one append per interval.
  describe "routing by selection" do
    test "an append wakes only the connections that selected its type" do
      start_dispatcher()

      wanted = spawn_stream(MapSet.new(["order_placed"]))
      unwanted = spawn_stream(MapSet.new(["order_shipped"]))

      {:ok, _} = FastLog.append(%OrderPlaced{order_id: 1})

      assert_receive {:woken, ^wanted}, 1_000
      refute_receive {:woken, ^unwanted}, @poll * 4
    end

    test "a whole-log subscription is woken by everything" do
      # Which is what keeps a bare EventSource working exactly as it did before
      # routing existed.
      start_dispatcher()

      everything = spawn_stream(:all)

      {:ok, _} = FastLog.append(%OrderPlaced{order_id: 1})

      assert_receive {:woken, ^everything}, 1_000
    end

    test "a selection matching nothing appended is never woken at all" do
      start_dispatcher()

      nobody = spawn_stream(MapSet.new(["Some.Other.App.Message"]))

      {:ok, _} = FastLog.append(%OrderPlaced{order_id: 1})

      refute_receive {:woken, ^nobody}, @poll * 5
    end
  end

  ## Helpers

  # A stand-in for a request process: registers with a selection and reports
  # each wakeup back, so a test can tell *which* connections were woken rather
  # than only that something was.
  defp spawn_stream(selection) do
    parent = self()

    pid =
      spawn_link(fn ->
        {:ok, _status} = Streams.register(FastLog, 5_000, selection)
        send(parent, :registered)

        forward(parent)
      end)

    assert_receive :registered, 1_000

    pid
  end

  defp forward(parent) do
    receive do
      :wakeup ->
        send(parent, {:woken, self()})
        forward(parent)
    end
  end

  defp start_dispatcher do
    start_supervised!({Dispatcher, log: FastLog})
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
