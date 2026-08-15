defmodule DMEx.Handler.SubscriptionTest.SleepingTransport do
  @moduledoc """
  A reader that connects and then does nothing.

  Enough to give the subscription a live linked process to stop, which is what
  `terminate/2` has to reach after flushing.
  """

  @behaviour DMEx.Transport

  @impl DMEx.Transport
  def start_reader(opts) do
    owner = Keyword.fetch!(opts, :owner)
    test = Keyword.fetch!(opts, :test)

    spawn_link(fn ->
      DMEx.Transport.connected(owner)
      send(test, {:reading, self()})

      Process.sleep(:infinity)
    end)
  end

  @impl DMEx.Transport
  def write(sink, _rows), do: {:ok, sink, %{}}

  @impl DMEx.Transport
  def heartbeat(sink), do: {:ok, sink}
end

defmodule DMEx.Handler.SubscriptionTest.DyingTransport do
  @moduledoc """
  A reader that dies without ever reporting a connection.

  `DMEx.Transport.connected/1` is what resets the counter, so every one of these
  is the failed attempt it was and the reconnect schedule is all that paces them.
  """

  @behaviour DMEx.Transport

  @impl DMEx.Transport
  def start_reader(_opts), do: spawn_link(fn -> exit(:econnrefused) end)

  @impl DMEx.Transport
  def write(sink, _rows), do: {:ok, sink, %{}}

  @impl DMEx.Transport
  def heartbeat(sink), do: {:ok, sink}
end

defmodule DMEx.Handler.SubscriptionTest.HandlesThenRaises do
  @moduledoc """
  Handles `order_placed` and raises on `order_shipped`.

  The batch shape the stale-flush regression needs: a handled row commits its own
  cursor advance, and a later row in the same batch kills the process.
  """

  use DMEx.Handler, logs: [:test], name: :handles_then_raises

  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Messages.OrderShipped

  @impl true
  def handle_message(%OrderPlaced{}, _metadata), do: :ok
  def handle_message(%OrderShipped{}, _metadata), do: raise("handler exploded")
end

defmodule DMEx.Handler.SubscriptionTest do
  @moduledoc """
  What a stop does with work in hand.

  A subscription defers a skipped row's cursor advance, so stopping is the one
  moment that pending run has to be written or deliberately given up on.
  """

  use DMEx.Case, async: true

  import ExUnit.CaptureLog

  alias DMEx.Config
  alias DMEx.Handler.Subscription
  alias DMEx.Handler.SubscriptionTest.DyingTransport
  alias DMEx.Handler.SubscriptionTest.HandlesThenRaises
  alias DMEx.Handler.SubscriptionTest.SleepingTransport
  alias DMEx.Test.Handlers
  alias DMEx.Test.Storage

  @url "https://orders.internal/stream"
  @registry __MODULE__.Registry

  # `init/1` registers before it connects, so every subscription started here
  # needs a live registry to register in.
  setup do
    start_supervised!({Registry, keys: :unique, name: @registry})

    :ok
  end

  describe "terminate/2" do
    test "flushes a pending run of skips" do
      storage = Storage.new()
      {:ok, pid} = start(storage, Storage)

      skip(pid)

      assert :ok = GenServer.stop(pid)
      assert {:ok, 1} = Storage.last_seen("test/echo", storage)
    end

    # The pool already on its way down. `Ecto.Repo.transaction/1` calls it, so an
    # unavailable one **exits** rather than raising, which a `rescue` alone does
    # not catch — and `terminate/2` still has to stop the reader.
    test "gives up the flush and still stops the reader when the pool is gone" do
      storage = Storage.new()
      Storage.fail_transaction([:down], storage)

      {:ok, pid} = start(storage, Storage)

      assert_receive {:reading, reader}
      ref = Process.monitor(reader)

      skip(pid)

      assert :ok = GenServer.stop(pid)
      assert_receive {:DOWN, ^ref, :process, ^reader, _reason}
    end

    # The crash path, where `state` is whatever the last *returned* callback left:
    # a batch runs inside one `handle_call`, so a row it handled has committed an
    # advance the stale state knows nothing about. `c:DMEx.Storage.advance_cursor/3`
    # is absolute, so flushing the pending skip below it moved the durable cursor
    # backwards and made the restart re-run side effects that had succeeded.
    test "does not flush a stale pending run over an advance the same batch committed" do
      storage = Storage.new()

      Process.flag(:trap_exit, true)

      {:ok, pid} =
        start(storage, Storage,
          handler: HandlesThenRaises,
          config:
            Config.handler!(
              checkpoint_interval: 60_000,
              max_attempts: 1,
              on_failure: :crash,
              reconnect_backoff_base: 1,
              reconnect_backoff_max: 1
            )
        )

      skip(pid)

      capture_log(fn ->
        catch_exit(
          DMEx.Transport.deliver(pid, [
            %{"id" => 2, "type" => "order_placed", "data" => %{"order_id" => 2}, "metadata" => %{}},
            %{"id" => 3, "type" => "order_shipped", "data" => %{"order_id" => 3}, "metadata" => %{}}
          ])
        )

        assert_receive {:EXIT, ^pid, _reason}
      end)

      assert {:ok, 2} = Storage.last_seen("test/echo", storage)
    end
  end

  # The counter is what `DMEx.backoff/3` raises 2 to, so it is bounded here
  # rather than clamped there: past the cap the delay is
  # `reconnect_backoff_max` whatever the count says.
  describe "reconnect backoff" do
    test "counts failed connects up to a cap, holds there, and says so" do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :handler, :reconnect]])

      log =
        capture_log(fn ->
          {:ok, pid} =
            start(Storage.new(), Storage,
              transport: DyingTransport,
              config:
                Config.handler!(
                  checkpoint_interval: 60_000,
                  reconnect_backoff_base: 1,
                  reconnect_backoff_max: 2
                )
            )

          assert attempts(ref, 13) == Enum.to_list(1..10) ++ [10, 10, 10]

          GenServer.stop(pid)
        end)

      assert log =~ "has failed 10 consecutive connects"
    end
  end

  describe "an unexpected message" do
    test "is logged, and does not take the subscription down" do
      {:ok, pid} = start(Storage.new(), Storage)

      log =
        capture_log(fn ->
          send(pid, :nonsense)

          # Answered only after the message ahead of it, so the warning is out
          # before the capture ends.
          :sys.get_state(pid)
        end)

      assert log =~ "DMEx test/echo (DMEx.Test.Handlers.Echo) ignored an unexpected message: :nonsense"
      assert Process.alive?(pid)
    end

    # A reader this subscription already gave up on, signalling after the fact.
    # Frequent, and there is nothing to say about it.
    test "from an exit that is not the current reader's says nothing at all" do
      {:ok, pid} = start(Storage.new(), Storage)
      gone = spawn(fn -> :ok end)

      log =
        capture_log(fn ->
          send(pid, {:EXIT, gone, :killed})
          :sys.get_state(pid)
        end)

      assert log == ""
    end
  end

  ## Helpers

  defp start(storage, adapter, opts \\ []) do
    Subscription.start_link(
      Keyword.merge(
        [
          subscription: "test/echo",
          handler: Handlers.Echo,
          origin: @url,
          transport: SleepingTransport,
          test: self(),
          storage: Map.put(storage, :adapter, adapter),
          config: Config.handler!(checkpoint_interval: 60_000),
          registry: @registry
        ],
        opts
      )
    )
  end

  defp attempts(ref, count) do
    Enum.map(1..count, fn _ ->
      assert_receive {[:dmex, :handler, :reconnect], ^ref, %{attempt: attempt}, _metadata}, 1_000

      attempt
    end)
  end

  # A type no clause of `Handlers.Echo` names, so the row is skipped — and a skip
  # is what defers a cursor advance to the flush.
  defp skip(pid) do
    DMEx.Transport.deliver(pid, [
      %{"id" => 1, "type" => "nothing.declares.this", "data" => %{}, "metadata" => %{}}
    ])
  end
end
