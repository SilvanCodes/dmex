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
  @producers %{test: @url}

  defmodule OtherHandler do
    @moduledoc false
    use DMEx.Consumer.Handler, producer: :test, id: :other

    @impl true
    def handle_message(%OrderPlaced{}, _metadata), do: :ok
  end

  defmodule EchoTwin do
    @moduledoc "Declares Handlers.Echo's identity, for the duplicate check."
    use DMEx.Consumer.Handler, producer: :test, id: :echo

    @impl true
    def handle_message(%OrderPlaced{}, _metadata), do: :ok
  end

  defmodule ElsewhereHandler do
    @moduledoc "Names a producer nothing binds."
    use DMEx.Consumer.Handler, producer: :elsewhere, id: :elsewhere

    @impl true
    def handle_message(%OrderPlaced{}, _metadata), do: :ok
  end

  describe "the child spec" do
    test "starts a registry, a subscription supervisor, and the configured handlers" do
      start_consumer(handlers: [Handlers.Echo])

      assert Process.whereis(Consumer.registry(TestConsumer))
      assert Process.whereis(Consumer.subscriptions(TestConsumer))

      eventually(fn -> subscribed?("test/echo") end)
    end

    test "accepts a {module, opts} entry" do
      start_consumer(handlers: [{Handlers.Echo, on_failure: :skip}])

      eventually(fn -> subscribed?("test/echo") end)
    end

    test "starts with no handlers at all" do
      start_consumer([])

      assert Process.whereis(Consumer.subscriptions(TestConsumer))
    end

    # In init/1 rather than in the Task that starts the children: that Task is
    # restart: :temporary, so a raise inside it dies silently and the supervisor
    # never notices, which is not the loud failure this needs to be. An id is a
    # pure function of a declaration, so nothing has to have started for it to
    # be answerable — and failing here means the consumer does not start at all.
    test "refuses two handlers declaring one subscription id, naming both" do
      assert start_error(handlers: [Handlers.Echo, EchoTwin]) =~
               ~r/Handlers.Echo and DMEx.ConsumerTest.EchoTwin both declare the subscription/

      assert start_error(handlers: [Handlers.Echo, EchoTwin]) =~ ~s("test/echo")
    end

    test "rejects a handlers entry that is neither a module nor a pair" do
      assert start_error(handlers: ["nope"]) =~
               "must be a handler module or a {module, keyword} pair"
    end
  end

  # Validated where the map is declared rather than at first connect, because a
  # typo in a key and a typo in a handler's `producer:` are the same bug and
  # only one of them shows up in the "unknown producer" message.
  describe "the producer map" do
    test "rejects a name that is not a legal identifier" do
      assert start_error(producer: %{"Orders" => @url}) =~
               ~s(producer name "Orders" is not a legal identifier)
    end

    test "rejects a URL with no scheme, naming the entry" do
      assert start_error(producer: %{test: "orders.internal/stream"}) =~
               ~r/URL for "test".*no scheme/s
    end

    test "rejects a non-http scheme" do
      assert start_error(producer: %{test: "ftp://orders.internal/stream"}) =~
               ~r/URL for "test".*not http or https/s
    end

    test "rejects something that is not a URL at all" do
      assert start_error(producer: %{test: nil}) =~
               ~r/URL for "test".*not a non-empty string/s
    end

    test "rejects a producer option that is not a map" do
      assert start_error(producer: [test: @url]) =~ "`producer:` must be a map"
    end
  end

  describe "subscribe/2" do
    setup do
      start_consumer([])

      :ok
    end

    test "starts a subscription and registers it under its declared id" do
      assert {:ok, pid} = subscribe(Handlers.Echo)

      assert Process.alive?(pid)
      assert subscribed?("test/echo")
    end

    test "resolves the producer name against the consumer's map" do
      {:ok, pid} = subscribe(Handlers.Echo)

      assert :sys.get_state(pid).url == @url
    end

    # Two streams on one identity would share and clobber a single cursor.
    test "refuses a second subscription for the same id" do
      {:ok, first} = subscribe(Handlers.Echo)

      assert {:error, {:already_subscribed, "test/echo"}} = subscribe(Handlers.Echo)

      assert Process.alive?(first)
    end

    # Which is the point of keying on the declaration: the same identity from a
    # differently-named module is the same subscription.
    test "refuses a differently-named module declaring the same id" do
      {:ok, _} = subscribe(Handlers.Echo)

      assert {:error, {:already_subscribed, "test/echo"}} = subscribe(EchoTwin)
    end

    test "different ids may read the same producer" do
      assert {:ok, _} = subscribe(Handlers.Echo)
      assert {:ok, _} = subscribe(OtherHandler)

      assert subscribed?("test/echo")
      assert subscribed?("test/other")
    end

    test "rejects a module that is not a handler" do
      assert_raise ArgumentError, ~r/is not a DMEx handler/, fn ->
        subscribe(Handlers.NotAHandler)
      end
    end

    test "raises on an unknown producer name, listing the known ones" do
      assert_raise ArgumentError, ~r/`producer: "elsewhere"`.*Known producers: test/s, fn ->
        subscribe(ElsewhereHandler)
      end
    end

    test "rejects an unknown option, and says there is no filter" do
      assert_raise ArgumentError,
                   ~r/unknown subscription option.*no filter or selection option here/s,
                   fn -> subscribe(Handlers.Echo, filter: ["Some.Type"]) end
    end

    # Both belong to the handler module: it is the complete declaration of what
    # the subscription *is*, and a call site able to vary that could disagree
    # with it.
    test "rejects `selective:` here, since it belongs on the handler" do
      assert_raise ArgumentError, ~r/\[:selective\].*belong to the handler module/s, fn ->
        subscribe(Handlers.Echo, selective: false)
      end
    end

    # What is consumed and from whom belongs to the handler module, so naming
    # either here is a mistake rather than an override.
    test "rejects an attempt to name the producer or the message types here" do
      assert_raise ArgumentError, ~r/unknown subscription option\(s\) \[:url\]/, fn ->
        subscribe(Handlers.Echo, url: @url)
      end

      assert_raise ArgumentError, ~r/belong to the handler module, not here/, fn ->
        subscribe(Handlers.Echo, messages: [OrderPlaced])
      end
    end

    test "validates pacing options at subscribe time" do
      assert_raise ArgumentError, ~r/:on_failure must be one of/, fn ->
        subscribe(Handlers.Echo, on_failure: :explode)
      end
    end
  end

  describe "unsubscribe/2" do
    setup do
      start_consumer([])

      :ok
    end

    test "stops the subscription and frees the id" do
      {:ok, pid} = subscribe(Handlers.Echo)
      ref = Process.monitor(pid)

      assert :ok = Consumer.unsubscribe(Handlers.Echo, consumer: TestConsumer)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}

      eventually(fn -> not subscribed?("test/echo") end)

      assert {:ok, _} = subscribe(Handlers.Echo)
    end

    test "reports a subscription that is not running" do
      assert {:error, :not_found} = Consumer.unsubscribe(OtherHandler, consumer: TestConsumer)
    end
  end

  describe "resolving where the cursor is written" do
    test "a bare subscribe/2 with no repo and no consumer names both fixes" do
      assert_raise ArgumentError, ~r/nowhere to write.*repo: MyApp.Repo.*DMEx.Consumer,/s, fn ->
        Consumer.subscribe(Handlers.Echo, producer: @producers, consumer: NotRunningConsumer)
      end
    end

    test "the child spec's repo becomes the default for every subscription" do
      start_consumer([])

      {:ok, pid} = subscribe(Handlers.Echo)

      assert :sys.get_state(pid).storage_opts.repo == DMEx.Test.Repo
    end

    test "a subscription may override the consumer's repo" do
      # For an application whose bookkeeping lives in a second repo.
      start_consumer([])

      {:ok, pid} = subscribe(Handlers.Echo, repo: DMEx.Test.IntegrationRepo)

      assert :sys.get_state(pid).storage_opts.repo == DMEx.Test.IntegrationRepo
    end
  end

  describe "resolving the producer map" do
    test "a bare subscribe/2 with no map and no consumer names both fixes" do
      assert_raise ArgumentError, ~r/no producer map.*producer: %\{/s, fn ->
        Consumer.subscribe(Handlers.Echo, repo: DMEx.Test.Repo, consumer: NotRunningConsumer)
      end
    end

    # The escape hatch, exactly as `repo:` is one: the call site may bring its
    # own binding rather than taking the consumer's.
    test "a call-site map overrides the consumer's" do
      start_consumer([])

      {:ok, pid} = subscribe(Handlers.Echo, producer: %{test: "https://override.test/stream"})

      assert :sys.get_state(pid).url == "https://override.test/stream"
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

  # A raise in init/1 does not reach the caller as an exception — the supervisor
  # dies and the link delivers an exit — so this traps it and returns the
  # message. What matters is that the consumer refuses to start at all.
  defp start_error(opts) do
    Process.flag(:trap_exit, true)

    {:error, {%ArgumentError{message: message}, _stacktrace}} =
      Consumer.start_link(
        Keyword.merge([name: TestConsumer, repo: DMEx.Test.Repo, producer: @producers], opts)
      )

    message
  after
    Process.flag(:trap_exit, false)
  end

  defp start_consumer(opts) do
    start_supervised!(
      {Consumer,
       Keyword.merge(
         [name: TestConsumer, repo: DMEx.Test.Repo, producer: @producers],
         opts
       )}
    )
  end

  defp subscribe(handler, opts \\ []) do
    Consumer.subscribe(handler, Keyword.merge([consumer: TestConsumer], opts))
  end

  defp subscribed?(id) do
    Registry.lookup(Consumer.registry(TestConsumer), {:subscription, id}) != []
  end
end
