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
  alias DMEx.Test.IntegrationLog, as: Log
  alias DMEx.Test.Messages.OrderPlaced

  setup do
    # `otp_app: :dmex` is the difference from every other integration setup: it
    # scans this suite's own modules, so `test/support/handlers.ex` is
    # discovered and every handler in it subscribes without being named here.
    start_stack!(otp_app: :dmex)
  end

  test "a discovered handler delivers without anyone subscribing it", _ctx do
    {:ok, id} = Log.append(%OrderPlaced{order_id: 7, total: 1999})

    assert_receive {:handled, %OrderPlaced{order_id: 7}, _metadata}, 5_000

    eventually(fn -> Postgres.last_seen("test/echo", consumer_opts()) == {:ok, id} end)
  end

  test "a handler on two logs commits a cursor per subscription", _ctx do
    {:ok, id} = Log.append(%OrderPlaced{order_id: 8, total: 500})

    # Both names point at the same log, so both subscriptions see the same row
    # and advance their own cursor to it.
    eventually(fn -> Postgres.last_seen("test/fanned", consumer_opts()) == {:ok, id} end)
    eventually(fn -> Postgres.last_seen("other/fanned", consumer_opts()) == {:ok, id} end)
  end

  defp consumer_opts, do: %{repo: DMEx.Test.IntegrationRepo, prefix: nil}
end
