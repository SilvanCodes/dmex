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

  @moduletag :capture_log

  @url "http://127.0.0.1:1/stream"
  @other "http://127.0.0.1:2/stream"

  describe "rest_for_one restarts" do
    test "brings every discovered subscription back after an owned log crashes" do
      start_supervisor(otp_app: :dmex, logs: [test: DMEx.Test.Log, other: @other])

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

  describe "starting the discovered handlers" do
    test "a failing subscribe does not stop the handlers after it" do
      start_supervisor(otp_app: nil, logs: [test: @url, other: @other])

      # The second attempt at the same identity is `{:error,
      # {:already_subscribed, _}}` — one of `subscribe/2`'s documented returns.
      {:ok, _} = DMEx.Supervisor.subscribe(Handlers.Echo, supervisor: TestSupervisor)

      DMEx.Supervisor.__start_discovered__(TestSupervisor, [Handlers.Echo, Handlers.Careful])

      assert subscribed?("test/careful")
    end
  end

  ## Helpers

  defp registry_pid, do: Process.whereis(DMEx.Supervisor.registry(TestSupervisor))

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
