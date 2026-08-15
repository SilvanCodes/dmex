defmodule DMEx.SupervisorTest do
  # Not async: subscriptions run in their own processes and need the shared
  # sandbox to read their cursors.
  use DMEx.Case, async: false, database: true

  alias DMEx.Handler
  alias DMEx.Test.Handlers
  alias DMEx.Test.IntegrationRepo
  alias DMEx.Test.Log
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Repo

  @moduletag :capture_log

  # Nothing listens here. A subscription that cannot connect backs off and
  # retries, which is exactly the behaviour under test for everything except
  # actual delivery — that lives in the integration suite.
  @url "http://127.0.0.1:1/stream"
  @other "http://127.0.0.1:2/stream"
  @logs [test: @url, other: @other]

  # Modules defined in a `.exs` are not in the application's module list, so
  # discovery never sees these three — which is what lets them declare things a
  # discovered handler is not allowed to.
  defmodule OtherHandler do
    @moduledoc false
    use Handler, logs: [:test], name: :other

    @impl true
    def handle_message(%OrderPlaced{}, _metadata), do: :ok
  end

  defmodule EchoTwin do
    @moduledoc "Declares Handlers.Echo's identity, for the duplicate check."
    use Handler, logs: [:test], name: :echo

    @impl true
    def handle_message(%OrderPlaced{}, _metadata), do: :ok
  end

  defmodule ElsewhereHandler do
    @moduledoc "Names a log nothing binds."
    use Handler, logs: [:elsewhere], name: :elsewhere

    @impl true
    def handle_message(%OrderPlaced{}, _metadata), do: :ok
  end

  defmodule FannedTwin do
    @moduledoc "Collides with Handlers.Fanned on one of its two logs."
    use Handler, logs: [:other], name: :fanned

    @impl true
    def handle_message(%OrderPlaced{}, _metadata), do: :ok
  end

  describe "the child spec" do
    test "starts a registry and a subscription supervisor" do
      start_supervisor([])

      assert Process.whereis(DMEx.Supervisor.registry(TestSupervisor))
      assert Process.whereis(DMEx.Supervisor.subscriptions(TestSupervisor))
    end

    test "refuses an option it does not know, naming it" do
      assert start_error(options(rep: Repo)) =~ "unknown option [:rep]"
    end

    # Every test but the discovery ones passes `otp_app: nil`, since this suite
    # *is* the `:dmex` application and would otherwise start its own fixtures.
    test "discovers nothing when no scanned application has handlers" do
      start_supervisor([])

      refute subscribed?("test/echo")
    end

    test "starts every subscription every discovered handler declares" do
      start_supervisor(otp_app: :dmex)

      for handler <- Handler.discover(:dmex),
          id <- Handler.subscription_ids(handler) do
        eventually(fn -> subscribed?(id) end)
      end

      # One handler, one subscription per log — two cursors, two connections.
      assert subscribed?("test/fanned")
      assert subscribed?("other/fanned")
    end

    # Which is what an ordinary application gets without saying anything: its own
    # app is the Mix project, and in a release it declares `:dmex` and is scanned
    # for that reason instead.
    test "defaults the scanned application to the Mix project" do
      start_supervised!({DMEx.Supervisor, name: TestSupervisor, repo: Repo, bindings: @logs})

      eventually(fn -> subscribed?("test/echo") end)
    end

    test "discovers the handlers in test/support and nothing else" do
      discovered = Handler.discover(:dmex)

      assert Handlers.Echo in discovered
      assert Handlers.FailTimes in discovered

      # Not a handler at all, and a log module answers `__dmex__/0` too.
      refute Handlers.NotAHandler in discovered
      refute Log in discovered

      # Compiled from a `.exs`, so not in the application's module list.
      refute EchoTwin in discovered
    end

    test "refuses to start when a discovered handler's log is not bound" do
      assert start_error(options(otp_app: :dmex, bindings: [other: @other])) =~
               ~r/reads "test", which is not in `bindings:`/

      assert start_error(options(otp_app: :dmex, bindings: [other: @other])) =~ "Known logs: other"
    end

    test "starts a log module bound in bindings:" do
      start_supervisor(bindings: [owned: Log] ++ @logs)

      assert Process.whereis(DMEx.Test.Log.Supervisor)
    end

    test "starts an owned log once even when two names point at it" do
      start_supervisor(bindings: [owned: Log, mirror: Log] ++ @logs)

      assert Process.whereis(DMEx.Test.Log.Supervisor)
    end

    # The binding is the whole of the choice: a module is read in this VM, a URL
    # over HTTP. Nothing names a transport anywhere.
    test "reads a log this application owns in this VM" do
      start_supervisor(bindings: [test: Log, other: @other])

      assert {:ok, [pid]} = subscribe(Handlers.Echo)

      # The transport is named once, in the options the reader is started from.
      assert origin(pid) == Log
      assert transport(pid) == DMEx.Transport.Local
    end

    test "reads a log bound to a URL over HTTP" do
      start_supervisor([])

      assert {:ok, [pid]} = subscribe(Handlers.Echo)

      assert origin(pid) == @url
      assert transport(pid) == DMEx.Transport.SSE
    end

    test "rejects a logs entry that is neither a module nor a URL" do
      assert start_error(options(bindings: [owned: Enum])) =~
               "which is not a module built with `use DMEx.Log`"
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
      assert_raise ArgumentError, ~r/reads "elsewhere".*Known logs: test/s, fn ->
        DMEx.Supervisor.__startable__!([ElsewhereHandler], %{"test" => @url})
      end
    end

    # The pair is the identity, so a clash on one log is a clash even when the
    # handlers read different sets.
    test "sees a duplicate on any one of a multi-log handler's subscriptions" do
      assert_raise ArgumentError, ~r/both declare the subscription "other\/fanned"/s, fn ->
        DMEx.Supervisor.__startable__!(
          [Handlers.Fanned, FannedTwin],
          %{"test" => @url, "other" => @other}
        )
      end
    end

    test "hands back what it was given when everything is startable" do
      handlers = [Handlers.Echo, OtherHandler]

      assert DMEx.Supervisor.__startable__!(handlers, %{"test" => @url}) == handlers
    end
  end

  # Validated where the names are declared rather than at first connect, because
  # a typo in a key and a typo in a handler's `logs:` are the same bug and only
  # one of them shows up in the "unknown log" message.
  describe "bindings:" do
    test "rejects a name that is not a legal identifier" do
      assert start_error(options(bindings: [{"Orders", @url}])) =~
               ~s(a log name must be an atom or a string matching)
    end

    test "rejects a URL with no scheme, naming the entry" do
      assert start_error(options(bindings: [test: "orders.internal/stream"])) =~
               ~r/URL for "test".*no scheme/s
    end

    test "rejects a non-http scheme" do
      assert start_error(options(bindings: [test: "ftp://orders.internal/stream"])) =~
               ~r/URL for "test".*not http or https/s
    end

    # `URI.parse/1` answers `%URI{scheme: "http", host: nil}` for this, so a
    # scheme check alone reports the one part of the URL that was fine.
    test "rejects an http URL with no host, saying so" do
      assert start_error(options(bindings: [test: "http:stream"])) =~
               ~r/URL for "test".*no host/s
    end

    test "rejects something that is neither a URL nor a module" do
      assert start_error(options(bindings: [test: nil])) =~
               ~r/URL for "test".*neither a log module nor a non-empty string/s
    end

    test "rejects an entry that is not a pair" do
      assert start_error(options(bindings: ["nope"])) =~ "must be `name: url_or_module`"
    end

    # A repeat would silently discard a log's target — and with it the transport
    # every subscription on that name uses.
    test "rejects a repeated name" do
      assert start_error(options(bindings: [test: @url, test: Log])) =~
               ~s(`bindings:` names "test" twice)
    end

    test "rejects a logs option that is not a list" do
      assert start_error(options(bindings: "nope")) =~ "`bindings:` must be a keyword list"
    end
  end

  describe "subscribe/2" do
    setup do
      start_supervisor([])

      :ok
    end

    test "starts a subscription and registers it under its declared id" do
      assert {:ok, [pid]} = subscribe(Handlers.Echo)

      assert Process.alive?(pid)
      assert subscribed?("test/echo")
    end

    test "resolves the log name against the supervisor's logs" do
      {:ok, [pid]} = subscribe(Handlers.Echo)

      assert origin(pid) == @url
    end

    # Two streams on one identity would share and clobber a single cursor.
    test "refuses a second subscription for the same id" do
      {:ok, [first]} = subscribe(Handlers.Echo)

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
      assert_raise ArgumentError, ~r/reads "elsewhere".*Known logs: other, test/s, fn ->
        subscribe(ElsewhereHandler)
      end
    end

    test "rejects an unknown option, and says there is no filter" do
      assert_raise ArgumentError,
                   ~r/unknown option.*subscribe\/2.*no filter or selection option here/s,
                   fn -> subscribe(Handlers.Echo, filter: ["Some.Type"]) end
    end

    # Whether the log narrows the stream is the handler's clauses' to say, so
    # there is no option for it anywhere — here least of all, where a call site
    # able to vary it could disagree with the module.
    test "rejects `selective:`, which is not an option anywhere" do
      assert_raise ArgumentError, ~r/\[:selective\].*belong to the handler module/s, fn ->
        subscribe(Handlers.Echo, selective: false)
      end
    end

    # What is consumed and from whom belongs to the handler module, so naming
    # either here is a mistake rather than an override.
    test "rejects an attempt to name the log or the message types here" do
      assert_raise ArgumentError, ~r/unknown option \[:url\]/, fn ->
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
      {:ok, [pid]} = subscribe(Handlers.Careful)

      assert config(pid).on_failure == :crash
      assert config(pid).max_attempts == 5

      assert :ok = DMEx.Supervisor.unsubscribe(Handlers.Careful, supervisor: TestSupervisor)
      eventually(fn -> not subscribed?("test/careful") end)

      {:ok, [pid]} = subscribe(Handlers.Careful, on_failure: :skip)

      assert config(pid).on_failure == :skip
      assert config(pid).max_attempts == 5
    end
  end

  describe "unsubscribe/2" do
    setup do
      start_supervisor([])

      :ok
    end

    test "stops the subscription and frees the id" do
      {:ok, [pid]} = subscribe(Handlers.Echo)
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
      assert_raise ArgumentError, ~r/nowhere to write.*`repo:` on the call, or start/s, fn ->
        DMEx.Supervisor.subscribe(Handlers.Echo, bindings: @logs, supervisor: NotRunningSupervisor)
      end
    end

    test "the child spec's repo becomes the default for every subscription" do
      start_supervisor([])

      {:ok, [pid]} = subscribe(Handlers.Echo)

      assert storage(pid).repo == Repo
    end

    test "a subscription may override the supervisor's repo" do
      # For an application whose bookkeeping lives in a second repo.
      start_supervisor([])

      {:ok, [pid]} = subscribe(Handlers.Echo, repo: IntegrationRepo)

      assert storage(pid).repo == IntegrationRepo
    end
  end

  describe "resolving logs" do
    test "a bare subscribe/2 with no logs and nothing running names both fixes" do
      assert_raise ArgumentError, ~r/no `bindings:`.*Pass them on the call, or start/s, fn ->
        DMEx.Supervisor.subscribe(Handlers.Echo,
          repo: Repo,
          supervisor: NotRunningSupervisor
        )
      end
    end

    # The escape hatch, exactly as `repo:` is one: the call site may bring its
    # own binding rather than taking the supervisor's.
    test "call-site logs override the supervisor's" do
      start_supervisor([])

      {:ok, [pid]} = subscribe(Handlers.Echo, bindings: [test: "https://override.test/stream"])

      assert origin(pid) == "https://override.test/stream"
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

  defp start_supervisor(opts), do: start_supervised!({DMEx.Supervisor, options(opts)})

  defp options(opts) do
    Keyword.merge([name: TestSupervisor, otp_app: nil, repo: Repo, bindings: @logs], opts)
  end

  defp subscribe(handler, opts \\ []) do
    DMEx.Supervisor.subscribe(handler, Keyword.merge([supervisor: TestSupervisor], opts))
  end

  defp subscribed?(id) do
    Registry.lookup(DMEx.Supervisor.registry(TestSupervisor), {:subscription, id}) != []
  end

  # Every resolution `subscribe/2` performs lands in the reader's options, and
  # the started subscription is the only place to read one back: the transport is
  # inferred, not passed, and the config and storage are merged from three
  # sources. These name the four questions the tests above ask, so a change to the
  # subscription's state shape is four lines here rather than ten assertions.
  defp origin(pid), do: :sys.get_state(pid).pipeline.origin
  defp transport(pid), do: :sys.get_state(pid).opts[:transport]
  defp config(pid), do: :sys.get_state(pid).pipeline.config
  defp storage(pid), do: :sys.get_state(pid).pipeline.storage
end
