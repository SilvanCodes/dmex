defmodule DMEx.SupervisorTest do
  # Not async: subscriptions run in their own processes and need the shared
  # sandbox to read their cursors.
  use DMEx.Case, async: false, database: true

  alias DMEx.Handler
  alias DMEx.Test.Handlers
  alias DMEx.Test.Messages.OrderPlaced

  @moduletag :capture_log

  # Nothing listens here. A subscription that cannot connect backs off and
  # retries, which is exactly the behaviour under test for everything except
  # actual delivery — that lives in the integration suite.
  @url "http://127.0.0.1:1/stream"
  @logs [test: @url]

  # Modules defined in a `.exs` are not in the application's module list, so
  # discovery never sees these three — which is what lets them declare things a
  # discovered handler is not allowed to.
  defmodule OtherHandler do
    @moduledoc false
    use DMEx.Handler, log: :test, id: :other

    @impl true
    def handle_message(%OrderPlaced{}, _metadata), do: :ok
  end

  defmodule EchoTwin do
    @moduledoc "Declares Handlers.Echo's identity, for the duplicate check."
    use DMEx.Handler, log: :test, id: :echo

    @impl true
    def handle_message(%OrderPlaced{}, _metadata), do: :ok
  end

  defmodule ElsewhereHandler do
    @moduledoc "Names a log nothing binds."
    use DMEx.Handler, log: :elsewhere, id: :elsewhere

    @impl true
    def handle_message(%OrderPlaced{}, _metadata), do: :ok
  end

  describe "the child spec" do
    test "starts a registry and a subscription supervisor" do
      start_supervisor([])

      assert Process.whereis(DMEx.Supervisor.registry(TestSupervisor))
      assert Process.whereis(DMEx.Supervisor.subscriptions(TestSupervisor))
    end

    # `:dmex` does not depend on `:dmex`, so nothing in this suite is discovered
    # unless a test asks for it by name. A real application is found because it
    # does depend on DMEx.
    test "discovers nothing when no scanned application has handlers" do
      start_supervisor([])

      refute subscribed?("test/echo")
    end

    test "starts every discovered handler" do
      start_supervisor(otp_app: :dmex)

      for handler <- Handler.discover(:dmex) do
        eventually(fn -> subscribed?(handler.__dmex__().id) end)
      end
    end

    test "discovers the handlers in test/support and nothing else" do
      discovered = Handler.discover(:dmex)

      assert Handlers.Echo in discovered
      assert Handlers.FailTimes in discovered

      # Not a handler at all, and a log module answers `__dmex__/0` too.
      refute Handlers.NotAHandler in discovered
      refute DMEx.Test.Log in discovered

      # Compiled from a `.exs`, so not in the application's module list.
      refute EchoTwin in discovered
    end

    test "refuses to start when a discovered handler's log is not bound" do
      assert start_error(otp_app: :dmex, logs: [other: @url]) =~
               ~r/declares `log: "test"`, which is not in `logs:`/

      assert start_error(otp_app: :dmex, logs: [other: @url]) =~ "Known logs: other"
    end

    test "starts the log module given as `log:`" do
      start_supervisor(log: DMEx.Test.Log)

      assert Process.whereis(DMEx.Test.Log.Supervisor)
    end

    test "rejects a `log:` that is not a module" do
      assert start_error(log: "MyApp.Log") =~
               "`log:` must be a log module built with `use DMEx.Log`"
    end
  end

  # Both are pure functions of the declarations, which is why `init/1` can ask
  # them before it starts anything.
  describe "__startable__!/2" do
    test "refuses two handlers declaring one subscription id, naming both" do
      assert_raise ArgumentError, ~r/both declare the subscription "test\/echo"/s, fn ->
        DMEx.Supervisor.__startable__!([Handlers.Echo, EchoTwin], %{"test" => @url})
      end
    end

    test "refuses a handler naming a log nothing binds, listing the known ones" do
      assert_raise ArgumentError, ~r/`log: "elsewhere"`.*Known logs: test/s, fn ->
        DMEx.Supervisor.__startable__!([ElsewhereHandler], %{"test" => @url})
      end
    end

    test "hands back what it was given when everything is startable" do
      handlers = [Handlers.Echo, OtherHandler]

      assert DMEx.Supervisor.__startable__!(handlers, %{"test" => @url}) == handlers
    end
  end

  # Validated where the names are declared rather than at first connect, because
  # a typo in a key and a typo in a handler's `log:` are the same bug and only
  # one of them shows up in the "unknown log" message.
  describe "logs:" do
    test "rejects a name that is not a legal identifier" do
      assert start_error(logs: [{"Orders", @url}]) =~
               ~s(log name "Orders" is not a legal identifier)
    end

    test "rejects a URL with no scheme, naming the entry" do
      assert start_error(logs: [test: "orders.internal/stream"]) =~
               ~r/URL for "test".*no scheme/s
    end

    test "rejects a non-http scheme" do
      assert start_error(logs: [test: "ftp://orders.internal/stream"]) =~
               ~r/URL for "test".*not http or https/s
    end

    test "rejects something that is not a URL at all" do
      assert start_error(logs: [test: nil]) =~ ~r/URL for "test".*not a non-empty string/s
    end

    test "rejects an entry that is not a pair" do
      assert start_error(logs: ["nope"]) =~ "must be `name: url`"
    end

    test "rejects a logs option that is not a list" do
      assert start_error(logs: "nope") =~ "`logs:` must be a keyword list"
    end
  end

  describe "subscribe/2" do
    setup do
      start_supervisor([])

      :ok
    end

    test "starts a subscription and registers it under its declared id" do
      assert {:ok, pid} = subscribe(Handlers.Echo)

      assert Process.alive?(pid)
      assert subscribed?("test/echo")
    end

    test "resolves the log name against the supervisor's logs" do
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

    test "different ids may read the same log" do
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

    test "raises on an unknown log name, listing the known ones" do
      assert_raise ArgumentError, ~r/`log: "elsewhere"`.*Known logs: test/s, fn ->
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
    test "rejects an attempt to name the log or the message types here" do
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

    test "takes the handler's declared config, and lets the call site override it" do
      {:ok, pid} = subscribe(Handlers.Careful)

      assert :sys.get_state(pid).config.on_failure == :crash
      assert :sys.get_state(pid).config.max_attempts == 5

      assert :ok = DMEx.Supervisor.unsubscribe(Handlers.Careful, supervisor: TestSupervisor)
      eventually(fn -> not subscribed?("test/careful") end)

      {:ok, pid} = subscribe(Handlers.Careful, on_failure: :skip)

      assert :sys.get_state(pid).config.on_failure == :skip
      assert :sys.get_state(pid).config.max_attempts == 5
    end
  end

  describe "unsubscribe/2" do
    setup do
      start_supervisor([])

      :ok
    end

    test "stops the subscription and frees the id" do
      {:ok, pid} = subscribe(Handlers.Echo)
      ref = Process.monitor(pid)

      assert :ok = DMEx.Supervisor.unsubscribe(Handlers.Echo, supervisor: TestSupervisor)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}

      eventually(fn -> not subscribed?("test/echo") end)

      assert {:ok, _} = subscribe(Handlers.Echo)
    end

    test "reports a subscription that is not running" do
      assert {:error, :not_found} =
               DMEx.Supervisor.unsubscribe(OtherHandler, supervisor: TestSupervisor)
    end
  end

  describe "resolving where the cursor is written" do
    test "a bare subscribe/2 with no repo and nothing running names both fixes" do
      assert_raise ArgumentError, ~r/nowhere to write.*repo: MyApp.Repo.*DMEx.Supervisor,/s, fn ->
        DMEx.Supervisor.subscribe(Handlers.Echo, logs: @logs, supervisor: NotRunningSupervisor)
      end
    end

    test "the child spec's repo becomes the default for every subscription" do
      start_supervisor([])

      {:ok, pid} = subscribe(Handlers.Echo)

      assert :sys.get_state(pid).storage_opts.repo == DMEx.Test.Repo
    end

    test "a subscription may override the supervisor's repo" do
      # For an application whose bookkeeping lives in a second repo.
      start_supervisor([])

      {:ok, pid} = subscribe(Handlers.Echo, repo: DMEx.Test.IntegrationRepo)

      assert :sys.get_state(pid).storage_opts.repo == DMEx.Test.IntegrationRepo
    end
  end

  describe "resolving logs" do
    test "a bare subscribe/2 with no logs and nothing running names both fixes" do
      assert_raise ArgumentError, ~r/no `logs:`.*logs: \[orders:/s, fn ->
        DMEx.Supervisor.subscribe(Handlers.Echo,
          repo: DMEx.Test.Repo,
          supervisor: NotRunningSupervisor
        )
      end
    end

    # The escape hatch, exactly as `repo:` is one: the call site may bring its
    # own binding rather than taking the supervisor's.
    test "call-site logs override the supervisor's" do
      start_supervisor([])

      {:ok, pid} = subscribe(Handlers.Echo, logs: [test: "https://override.test/stream"])

      assert :sys.get_state(pid).url == "https://override.test/stream"
    end
  end

  describe "naming" do
    test "derives the registry and subscription supervisor from the name" do
      assert DMEx.Supervisor.registry(MyApp.Supervisor) == MyApp.Supervisor.Registry
      assert DMEx.Supervisor.subscriptions(MyApp.Supervisor) == MyApp.Supervisor.Subscriptions
    end

    test "defaults to DMEx.Supervisor" do
      assert DMEx.Supervisor.registry() == DMEx.Supervisor.Registry
      assert DMEx.Supervisor.subscriptions() == DMEx.Supervisor.Subscriptions
    end
  end

  ## Helpers

  # A raise in init/1 does not reach the caller as an exception — the supervisor
  # dies and the link delivers an exit — so this traps it and returns the
  # message. What matters is that it refuses to start at all.
  defp start_error(opts) do
    Process.flag(:trap_exit, true)

    {:error, {%ArgumentError{message: message}, _stacktrace}} =
      DMEx.Supervisor.start_link(
        Keyword.merge([name: TestSupervisor, repo: DMEx.Test.Repo, logs: @logs], opts)
      )

    message
  after
    Process.flag(:trap_exit, false)
  end

  defp start_supervisor(opts) do
    start_supervised!(
      {DMEx.Supervisor,
       Keyword.merge([name: TestSupervisor, repo: DMEx.Test.Repo, logs: @logs], opts)}
    )
  end

  defp subscribe(handler, opts \\ []) do
    DMEx.Supervisor.subscribe(handler, Keyword.merge([supervisor: TestSupervisor], opts))
  end

  defp subscribed?(id) do
    Registry.lookup(DMEx.Supervisor.registry(TestSupervisor), {:subscription, id}) != []
  end
end
