defmodule DMEx.Integration.LocalSliceTest do
  @moduledoc """
  The whole of DMEx end to end with no wire at all — **no server is started**.

  Append writes a row and commits; `NOTIFY` wakes the local reader; the send
  loop reads it and hands it straight to the pipeline; the handler runs; the
  cursor commits. Every property the SSE slice has, minus the socket.

  It runs in the integration suite rather than the sandboxed one for the same
  reason that one does: `LISTEN`/`NOTIFY` only fires at COMMIT, so a live wakeup
  cannot be exercised inside a transaction that never commits.
  """

  use DMEx.IntegrationCase

  alias DMEx.Storage.Postgres
  alias DMEx.Test.Handlers
  alias DMEx.Test.IntegrationLog, as: Log
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Messages.OrderShipped

  setup do
    # `:test` bound to the module rather than to a URL. That is the entire
    # difference from the SSE slice — no server, no port, no URL to build, and
    # the log itself started as a child of this one entry, hence `log: nil`.
    #
    # `otp_app: nil` costs nothing here: the message scan it feeds is what
    # expands a *remote* subscription's filter, and a local read never builds
    # one.
    start_stack!(log: nil, logs: [test: Log])

    %{storage_opts: Log.__dmex__().storage_opts}
  end

  test "delivers a message appended after connect, live" do
    subscribe(Handlers.Echo)

    # Nothing was waiting when the reader connected, so this is the wakeup path:
    # the append commits, `NOTIFY` fires, the parked loop reads.
    {:ok, id} = Log.append(%OrderPlaced{order_id: 7, total: 1999}, metadata: %{"who" => "me"})

    assert_receive {:handled, message, metadata}, 5_000

    assert message == %OrderPlaced{order_id: 7, customer_id: nil, total: 1999}
    assert metadata == %{"who" => "me"}

    eventually(fn -> Postgres.last_seen("test/echo", consumer_opts()) == {:ok, id} end)
  end

  test "delivers a backlog appended before anything subscribed, in order" do
    for id <- 1..5, do: {:ok, _} = Log.append(%OrderPlaced{order_id: id})

    subscribe(Handlers.Echo)

    for id <- 1..5 do
      assert_receive {:handled, %OrderPlaced{order_id: ^id}, _}, 5_000
    end
  end

  # The cursor is the identity, and it does not know how the rows reached it.
  test "resumes from the durable cursor after the subscription restarts" do
    subscribe(Handlers.Echo)

    {:ok, _} = Log.append(%OrderPlaced{order_id: 1})
    assert_receive {:handled, %OrderPlaced{order_id: 1}, _}, 5_000

    :ok = DMEx.Supervisor.unsubscribe(Handlers.Echo, supervisor: TestSupervisor)

    {:ok, _} = Log.append(%OrderPlaced{order_id: 2})

    subscribe(Handlers.Echo)

    assert_receive {:handled, %OrderPlaced{order_id: 2}, _}, 5_000
    refute_received {:handled, %OrderPlaced{order_id: 1}, _}
  end

  # Selection with no fingerprint on either side: the handler's declared types
  # go into the read as a type list, and the rest of the log is never read.
  test "reads only the types the handler declares" do
    subscribe(Handlers.Echo)

    {:ok, _} = Log.append(%OrderShipped{order_id: 1})
    {:ok, _} = Log.append(%OrderPlaced{order_id: 2})

    assert_receive {:handled, %OrderPlaced{order_id: 2}, _}, 5_000
    refute_received {:handled, %OrderShipped{}, _}
  end

  # Two subscriptions on one owned log, each with its own reader and cursor.
  test "two handlers on one local log each get everything" do
    subscribe(Handlers.Echo)
    subscribe(Handlers.EchoWholeLog)

    {:ok, _} = Log.append(%OrderPlaced{order_id: 1})

    assert_receive {:handled, %OrderPlaced{order_id: 1}, _}, 5_000
    assert_receive {:handled, %OrderPlaced{order_id: 1}, _}, 5_000

    eventually(fn ->
      Postgres.last_seen("test/echo", consumer_opts()) != {:ok, nil} and
        Postgres.last_seen("test/echo_whole_log", consumer_opts()) != {:ok, nil}
    end)
  end

  ## Helpers

  # `last_seen: 0` throughout: these tests append first and subscribe after,
  # which is the backlog they are about. The default — unset — would correctly
  # skip all of it.
  defp subscribe(handler, opts \\ [last_seen: 0]) do
    {:ok, _pids} = DMEx.Supervisor.subscribe(handler, [supervisor: TestSupervisor] ++ opts)

    :ok
  end

  defp consumer_opts, do: %{repo: DMEx.Test.IntegrationRepo, prefix: nil}
end
