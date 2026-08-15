defmodule DMEx.Integration.LocalSliceTest do
  @moduledoc """
  A local reader woken by a real COMMIT — **no server is started**.

  Append writes a row and commits; `NOTIFY` wakes the local reader; the send
  loop reads it and hands it straight to the pipeline; the handler runs; the
  cursor commits.

  That wakeup is the one thing `DMEx.Transport.LocalTest` structurally cannot
  reach: `LISTEN`/`NOTIFY` only fires at COMMIT, and the sandbox never commits.
  Everything else about a local subscription is pinned there, and the rest of
  this suite pins the same properties over a socket.
  """

  use DMEx.IntegrationCase

  alias DMEx.Test.Handlers
  alias DMEx.Test.IntegrationLog, as: Log
  alias DMEx.Test.Messages.OrderPlaced

  setup do
    # `:test` bound to the module rather than to a URL. That is the entire
    # difference from the SSE slice — no server, no port, no URL to build, and
    # the log itself started as a child of this one entry, hence `log: nil`.
    #
    # `otp_app: nil` costs nothing here: the message scan it feeds is what
    # expands a *remote* subscription's filter, and a local read never builds
    # one.
    start_stack!(log: nil, bindings: [test: Log])

    %{storage: Log.__dmex__().storage}
  end

  test "delivers a message appended after connect, live" do
    subscribe!(Handlers.Echo)

    # Nothing was waiting when the reader connected, so this is the wakeup path:
    # the append commits, `NOTIFY` fires, the parked loop reads.
    {:ok, id} = Log.append(%OrderPlaced{order_id: 7, total: 1999}, metadata: %{"who" => "me"})

    assert_receive {:handled, message, metadata}, 5_000

    assert message == %OrderPlaced{order_id: 7, customer_id: nil, total: 1999}
    assert metadata == %{"who" => "me"}

    eventually(fn -> cursor("test/echo") == {:ok, id} end)
  end
end
