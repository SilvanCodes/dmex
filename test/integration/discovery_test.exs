defmodule DMEx.Integration.DiscoveryTest do
  @moduledoc """
  Handler discovery, over a real socket.

  Every other integration test passes `otp_app: nil` and calls
  `DMEx.Supervisor.subscribe/2` by hand, so none of them exercises the
  mechanism README describes as the whole of adding a subscription: declare a
  handler module, and its subscriptions start themselves. This one turns
  discovery on and asserts a handler nobody subscribed delivers anyway.
  """

  use DMEx.IntegrationCase

  alias DMEx.Storage.Postgres
  alias DMEx.Test.Handlers
  alias DMEx.Test.IntegrationLog, as: Log
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Server

  setup do
    Handlers.report_to(self())

    start_supervised!(Log)

    url = Server.start!() <> "/stream"

    # `otp_app: :dmex` is the difference from every other integration setup: it
    # scans this suite's own modules, so `test/support/handlers.ex` is
    # discovered and every handler in it subscribes without being named here.
    start_supervised!(
      {DMEx.Supervisor,
       name: TestSupervisor,
       otp_app: :dmex,
       repo: DMEx.Test.IntegrationRepo,
       logs: Server.logs(url)}
    )

    %{url: url}
  end

  test "a discovered handler delivers without anyone subscribing it", _ctx do
    {:ok, id} = Log.append(%OrderPlaced{order_id: 7, total: 1999})

    assert_receive {:handled, %OrderPlaced{order_id: 7}, _metadata}, 5_000

    eventually(fn -> Postgres.last_seen(consumer_opts(), "test/echo") == {:ok, id} end)
  end

  test "a handler on two logs commits a cursor per subscription", _ctx do
    {:ok, id} = Log.append(%OrderPlaced{order_id: 8, total: 500})

    # Both names point at the same log, so both subscriptions see the same row
    # and advance their own cursor to it.
    eventually(fn -> Postgres.last_seen(consumer_opts(), "test/fanned") == {:ok, id} end)
    eventually(fn -> Postgres.last_seen(consumer_opts(), "other/fanned") == {:ok, id} end)
  end

  defp consumer_opts, do: %{repo: DMEx.Test.IntegrationRepo, prefix: nil}
end
