defmodule DMEx.ConsumerTest do
  # Not async: subscriptions run in their own processes and need the shared
  # sandbox to read their cursors.
  use DMEx.Case, async: false, database: true

  alias DMEx.Consumer
  alias DMEx.Test.Handlers
  alias DMEx.Test.Messages.OrderPlaced

  @moduletag :capture_log

  # Nothing listens here. A subscription that cannot connect backs off and
  # retries, which is exactly the behaviour under test for everything except
  # actual delivery — that lives in the integration suite.
  @url "http://127.0.0.1:1/stream"

  defmodule OtherHandler do
    @moduledoc false
    use DMEx.Consumer.Handler

    @impl true
    def handle_message(_message, _metadata), do: :ok
  end

  describe "the child spec" do
    test "starts a registry, a subscription supervisor, and the configured subscriptions" do
      start_consumer(
        subscriptions: [
          [url: @url, handler: Handlers.Echo, message_modules: [OrderPlaced]]
        ]
      )

      assert Process.whereis(Consumer.registry(TestConsumer))
      assert Process.whereis(Consumer.subscriptions(TestConsumer))

      eventually(fn -> subscribed?(Handlers.Echo) end)
    end

    test "starts with no subscriptions at all" do
      start_consumer([])

      assert Process.whereis(Consumer.subscriptions(TestConsumer))
    end
  end

  describe "subscribe/2" do
    setup do
      start_consumer([])

      :ok
    end

    test "starts a subscription and registers it under its handler" do
      assert {:ok, pid} = subscribe(handler: Handlers.Echo)

      assert Process.alive?(pid)
      assert subscribed?(Handlers.Echo)
    end

    # Subscribing the same handler twice would have both sharing and clobbering
    # one cursor, so the second is refused rather than debugged later.
    test "refuses a second subscription for the same handler" do
      {:ok, first} = subscribe(handler: Handlers.Echo)

      assert {:error, {:already_subscribed, Handlers.Echo}} = subscribe(handler: Handlers.Echo)

      assert Process.alive?(first)
    end

    test "different handlers may read the same producer" do
      assert {:ok, _} = subscribe(handler: Handlers.Echo)
      assert {:ok, _} = subscribe(handler: OtherHandler)

      assert subscribed?(Handlers.Echo)
      assert subscribed?(OtherHandler)
    end

    test "rejects a module that is not a handler" do
      assert {:error, _} = subscribe(handler: Handlers.NotAHandler)
    end

    test "rejects an unknown option, and says there is no filter" do
      assert_raise ArgumentError, ~r/unknown subscription option.*no filter option/s, fn ->
        subscribe(handler: Handlers.Echo, filter: ["Some.Type"])
      end
    end

    test "validates pacing options at subscribe time" do
      assert_raise ArgumentError, ~r/:on_failure must be one of/, fn ->
        subscribe(handler: Handlers.Echo, on_failure: :explode)
      end
    end
  end

  describe "unsubscribe/2" do
    setup do
      start_consumer([])

      :ok
    end

    test "stops the subscription and frees the handler" do
      {:ok, pid} = subscribe(handler: Handlers.Echo)
      ref = Process.monitor(pid)

      assert :ok = Consumer.unsubscribe(Handlers.Echo, consumer: TestConsumer)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}

      eventually(fn -> not subscribed?(Handlers.Echo) end)

      assert {:ok, _} = subscribe(handler: Handlers.Echo)
    end

    test "reports an unknown handler" do
      assert {:error, :not_found} = Consumer.unsubscribe(OtherHandler, consumer: TestConsumer)
    end
  end

  describe "resolving where the cursor is written" do
    test "a bare subscribe/2 with no repo and no consumer names both fixes" do
      assert_raise ArgumentError, ~r/nowhere to write.*repo: MyApp.Repo.*DMEx.Consumer,/s, fn ->
        Consumer.subscribe(@url,
          handler: Handlers.Echo,
          message_modules: [OrderPlaced],
          consumer: NotRunningConsumer
        )
      end
    end

    test "the child spec's repo becomes the default for every subscription" do
      start_consumer([])

      {:ok, pid} = subscribe(handler: Handlers.Echo)

      assert :sys.get_state(pid).storage_opts.repo == DMEx.Test.Repo
    end

    test "a subscription may override the consumer's repo" do
      # For an application whose bookkeeping lives in a second repo.
      start_consumer([])

      {:ok, pid} = subscribe(handler: Handlers.Echo, repo: DMEx.Test.IntegrationRepo)

      assert :sys.get_state(pid).storage_opts.repo == DMEx.Test.IntegrationRepo
    end
  end

  describe "naming" do
    test "derives the registry and supervisor from the consumer name" do
      assert Consumer.registry(MyApp.Consumer) == MyApp.Consumer.Registry
      assert Consumer.subscriptions(MyApp.Consumer) == MyApp.Consumer.Subscriptions
    end

    test "defaults to DMEx.Consumer" do
      assert Consumer.registry() == DMEx.Consumer.Registry
      assert Consumer.subscriptions() == DMEx.Consumer.Subscriptions
    end
  end

  ## Helpers

  defp start_consumer(opts) do
    start_supervised!({Consumer, Keyword.merge([name: TestConsumer, repo: DMEx.Test.Repo], opts)})
  end

  defp subscribe(opts) do
    Consumer.subscribe(
      @url,
      Keyword.merge([message_modules: [OrderPlaced], consumer: TestConsumer], opts)
    )
  end

  defp subscribed?(handler) do
    Registry.lookup(Consumer.registry(TestConsumer), {:handler, handler}) != []
  end
end
