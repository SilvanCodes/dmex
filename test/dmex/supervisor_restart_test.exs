defmodule DMEx.SupervisorRestartTest do
  @moduledoc """
  What survives a restart of the supervision tree, and what a failing subscribe
  is allowed to take with it.
  """

  # Not async: subscriptions run in their own processes and need the shared
  # sandbox to read their cursors.
  use DMEx.Case, async: false, database: true

  alias DMEx.Handler
  alias DMEx.Test.Handlers
  alias DMEx.Test.Log
  alias DMEx.Test.Server

  @moduletag :capture_log

  @url "http://127.0.0.1:1/stream"
  @other "http://127.0.0.1:2/stream"

  # Answers every request `400` and tells the test it was asked, so a restart
  # loop is observable as a second request rather than as elapsed time.
  defmodule Refuse do
    @moduledoc false

    def init(owner), do: owner

    def call(conn, owner) do
      send(owner, :requested)

      Plug.Conn.send_resp(conn, 400, "this client got the request wrong")
    end
  end

  describe "rest_for_one restarts" do
    test "brings every discovered subscription back after an owned log crashes" do
      start_supervisor(otp_app: :dmex, logs: [test: Log, other: @other])

      ids = discovered_ids()
      assert "test/echo" in ids

      for id <- ids, do: eventually(fn -> subscribed?(id) end)

      sup = Process.whereis(TestSupervisor)
      registry = registry_pid()

      # `Supervisor.stop/2` rather than `Process.exit(pid, :kill)`: a brutal kill
      # races the log's own restart against its not-yet-dead named ETS tables,
      # which fails the restart and takes the whole tree down — a different
      # failure, and one that hides this one behind a parent's restart.
      Supervisor.stop(Process.whereis(DMEx.Test.Log.Supervisor), :crashed)

      # `:rest_for_one` took the registry — and therefore every subscription —
      # with it. Waiting for the replacement is what makes the assertion below
      # about recovery rather than about the teardown not having happened yet.
      eventually(fn -> registry_pid() not in [nil, registry] end)

      # And the tree reports itself perfectly healthy while it is missing them,
      # which is what makes the loss silent.
      assert Process.whereis(TestSupervisor) == sup
      counts = Supervisor.count_children(TestSupervisor)
      assert counts.active == counts.specs

      for id <- ids, do: eventually(fn -> subscribed?(id) end)
    end
  end

  describe "a fatal transport failure" do
    # `DMEx.Transport.fatal/1` exists to stop rather than retry. A `:permanent`
    # child is restarted whatever it exits with, and a restarted subscription
    # connects immediately at `attempt: 0` — so the failure the backoff exists
    # to prevent was exactly what a 400 produced.
    test "stops the subscription instead of restarting it into a request loop" do
      url = Server.start!(plug: {Refuse, self()}) <> "/stream"
      start_supervisor(otp_app: nil, logs: [test: url])

      {:ok, [pid]} = DMEx.Supervisor.subscribe(Handlers.Echo, supervisor: TestSupervisor)
      ref = Process.monitor(pid)

      assert_receive :requested, 2_000
      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, {:bad_request, 400, _}}}, 2_000

      refute_receive :requested, 300
      refute subscribed?("test/echo")
    end
  end

  describe "the discovered subscriptions" do
    # The whole point of their being a child list: it is the only shape a
    # supervisor can put back. Started by a process that then exited, they were
    # gone from the child list altogether and an owned log's death took them with
    # it for the lifetime of the node.
    test "are children of the subscriptions supervisor" do
      start_supervisor(otp_app: :dmex, logs: [test: Log, other: @other])

      ids = discovered_ids()
      children = Supervisor.which_children(DMEx.Supervisor.subscriptions(TestSupervisor))

      assert length(children) == length(ids)

      for {id, pid, type, _modules} <- children do
        assert {DMEx.Handler.Subscription, subscription} = id
        assert subscription in ids
        assert is_pid(pid)
        assert type == :worker
      end
    end
  end

  defp registry_pid, do: Process.whereis(DMEx.Supervisor.registry(TestSupervisor))

  ## Helpers

  defp discovered_ids do
    for handler <- Handler.discover(:dmex), id <- Handler.subscription_ids(handler), do: id
  end

  defp start_supervisor(opts) do
    # `restart: :temporary` so ExUnit's own supervisor cannot restart the whole
    # tree behind the test's back — a fresh `init/1` would re-run the discovery
    # starter and hide exactly what these tests are about.
    start_supervised!(
      {DMEx.Supervisor,
       Keyword.merge(
         [name: TestSupervisor, otp_app: nil, repo: DMEx.Test.Repo, logs: [test: @url]],
         opts
       )},
      restart: :temporary
    )
  end

  defp subscribed?(id) do
    Registry.lookup(DMEx.Supervisor.registry(TestSupervisor), {:subscription, id}) != []
  end
end
