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

defmodule DMEx.Handler.SubscriptionTest.PoolDown do
  @moduledoc """
  The in-memory stub with a repo that is already gone.

  `Ecto.Repo.transaction/1` calls the pool, so an unavailable one **exits**
  rather than raising — the one shape a `rescue` cannot see.
  """

  @behaviour DMEx.Storage

  alias DMEx.Test.Storage

  @impl DMEx.Storage
  def transaction(_fun, _opts), do: exit(:noproc)

  @impl DMEx.Storage
  defdelegate append(type, data, metadata, opts), to: Storage
  @impl DMEx.Storage
  defdelegate read(cursor, limit, max_bytes, types, opts), to: Storage
  @impl DMEx.Storage
  defdelegate max_id(opts), to: Storage
  @impl DMEx.Storage
  defdelegate appended_since(cursor, opts), to: Storage
  @impl DMEx.Storage
  defdelegate last_seen(subscription, opts), to: Storage
  @impl DMEx.Storage
  defdelegate cursor(subscription, opts), to: Storage
  @impl DMEx.Storage
  defdelegate delete_cursor(subscription, opts), to: Storage
  @impl DMEx.Storage
  defdelegate advance_cursor(subscription, last_seen, opts), to: Storage
  @impl DMEx.Storage
  defdelegate record_dead_message(row, opts), to: Storage
  @impl DMEx.Storage
  defdelegate list_dead_messages(subscription, opts), to: Storage
  @impl DMEx.Storage
  defdelegate delete_dead_message(subscription, id, opts), to: Storage
end

defmodule DMEx.Handler.SubscriptionTest do
  @moduledoc """
  What a stop does with work in hand.

  A subscription defers a skipped row's cursor advance, so stopping is the one
  moment that pending run has to be written or deliberately given up on.
  """

  use DMEx.Case, async: true

  alias DMEx.Config
  alias DMEx.Handler.Subscription
  alias DMEx.Handler.SubscriptionTest.PoolDown
  alias DMEx.Handler.SubscriptionTest.SleepingTransport
  alias DMEx.Test.Handlers
  alias DMEx.Test.Storage

  @url "https://orders.internal/stream"

  describe "terminate/2" do
    test "flushes a pending run of skips" do
      storage = Storage.new()
      {:ok, pid} = start(storage, Storage)

      skip(pid)

      assert :ok = GenServer.stop(pid)
      assert {:ok, 1} = Storage.last_seen("test/echo", storage)
    end

    # The pool already on its way down. `Ecto.Repo.transaction/1` calls it, so an
    # unavailable one **exits** rather than raising — which `rescue` alone did not
    # catch, so `terminate/2` died on the flush and never stopped the reader.
    test "gives up the flush and still stops the reader when the pool is gone" do
      {:ok, pid} = start(Storage.new(), PoolDown)

      assert_receive {:reading, reader}
      ref = Process.monitor(reader)

      skip(pid)

      assert :ok = GenServer.stop(pid)
      assert_receive {:DOWN, ^ref, :process, ^reader, _reason}
    end
  end

  ## Helpers

  defp start(storage, adapter) do
    Subscription.start_link(
      subscription: "test/echo",
      handler: Handlers.Echo,
      origin: @url,
      transport: SleepingTransport,
      test: self(),
      storage: Map.put(storage, :adapter, adapter),
      config: Config.handler!(checkpoint_interval: 60_000),
      registry: nil
    )
  end

  # A type no clause of `Handlers.Echo` names, so the row is skipped — and a skip
  # is what defers a cursor advance to the flush.
  defp skip(pid) do
    DMEx.Transport.deliver(pid, [
      %{"id" => 1, "type" => "nothing.declares.this", "data" => %{}, "metadata" => %{}}
    ])
  end
end
