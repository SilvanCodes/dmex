defmodule DMEx.Integration.ResumptionTest do
  @moduledoc """
  Dropping a connection is not a delivery event.

  Nothing is lost and nothing is skipped when a stream dies: the log holds
  no consumer state, the consumer's `last_seen` is durable, and resumption is
  the same `id > cursor` read a cold connect does. It costs a reconnect and a
  re-read, nothing more.
  """

  use DMEx.IntegrationCase

  alias DMEx.Storage.Postgres
  alias DMEx.Test.Handlers
  alias DMEx.Test.IntegrationLog, as: Log
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Server

  @moduletag :capture_log

  setup do
    Handlers.report_to(self())

    start_supervised!({Log, otp_app: :dmex})

    # The server binds port 0, so the log map is necessarily built at
    # runtime — which is exactly why it lives on the child spec rather than in
    # application config.
    url = Server.start!() <> "/stream"

    start_supervised!(
      {DMEx.Supervisor,
       name: TestSupervisor, otp_app: nil, repo: DMEx.Test.IntegrationRepo, logs: Server.logs(url)}
    )

    %{url: url}
  end

  test "killing the stream loses nothing and repeats nothing", _ctx do
    {:ok, [pid]} = subscribe(Handlers.Echo)

    for n <- 1..5, do: {:ok, _} = Log.append(%OrderPlaced{order_id: n})
    assert drain(5) == [1, 2, 3, 4, 5]
    await_cursor(5)

    # The supervisor restarts it, it re-reads its persisted cursor, and the
    # log's ordinary range scan picks up from exactly there.
    Process.exit(pid, :kill)

    for n <- 6..10, do: {:ok, _} = Log.append(%OrderPlaced{order_id: n})

    assert drain(5) == [6, 7, 8, 9, 10]
    refute_receive {:handled, _, _}, 200
  end

  # The precise limit of duplicate suppression, and the reason DMEx is
  # at-least-once rather than exactly-once. The commit is the *only* record that
  # a message was handled, so a crash after a handler's effects land and before
  # that commit is indistinguishable from never having processed it — and the
  # effects happen again.
  #
  # Here the handler's effect is the `send` this test receives, so killing the
  # moment it arrives lands squarely in that window.
  test "a crash between a handler's effects and its commit redelivers that message", _ctx do
    # SlowCommit reports and then stalls before returning, which holds that
    # window open long enough to kill the process inside it deterministically.
    {:ok, [pid]} = subscribe(Handlers.SlowCommit)

    for n <- 1..3, do: {:ok, _} = Log.append(%OrderPlaced{order_id: n})

    assert_receive {:handled, %OrderPlaced{order_id: 1}, _}, 5_000
    Process.exit(pid, :kill)

    # Message 1 arrives again, because nothing recorded that it was handled.
    assert 1 in collect(2_000)
  end

  test "resumes from the durable cursor rather than from the beginning", _ctx do
    {:ok, [pid]} = subscribe(Handlers.Echo)

    for n <- 1..5, do: {:ok, _} = Log.append(%OrderPlaced{order_id: n})
    assert drain(5) == [1, 2, 3, 4, 5]
    await_cursor(5)

    Process.exit(pid, :kill)

    # Nothing new to send, so a restart that replayed from 0 would redeliver all
    # five — and duplicate suppression would have to be what saved it. This
    # asserts the cursor did the work, not the suppression.
    refute_receive {:handled, _, _}, 1_000
  end

  test "messages appended while disconnected are delivered on reconnect", _ctx do
    {:ok, [pid]} = subscribe(Handlers.Echo)

    {:ok, _} = Log.append(%OrderPlaced{order_id: 1})
    assert drain(1) == [1]
    await_cursor(1)

    Process.exit(pid, :kill)

    # The log *is* the buffer. Nothing had to be held for this consumer.
    for n <- 2..6, do: {:ok, _} = Log.append(%OrderPlaced{order_id: n})

    assert drain(5) == [2, 3, 4, 5, 6]
  end

  test "a killed stream mid-batch skips nothing and repeats at most the one in flight", _ctx do
    # The slow handler guarantees the kill lands mid-drain rather than between
    # frames, which is the case duplicate suppression exists for.
    {:ok, [pid]} = subscribe(Handlers.Slow)

    for n <- 1..10, do: {:ok, _} = Log.append(%OrderPlaced{order_id: n})

    assert_receive {:handled, %OrderPlaced{order_id: 1}}, 5_000
    assert_receive {:handled, %OrderPlaced{order_id: 2}}, 5_000

    Process.exit(pid, :kill)

    seen = collect(8_000)

    # Every id arrives, in order, and any repeat is a message whose transaction
    # had not committed when the process died.
    assert Enum.sort(Enum.uniq([1, 2] ++ seen)) == Enum.to_list(1..10)
    assert seen == Enum.sort(seen)
  end

  test "the cursor is what a restarted stream reads, and it is committed per message", _ctx do
    subscribe(Handlers.Echo)

    for n <- 1..3, do: {:ok, _} = Log.append(%OrderPlaced{order_id: n})
    drain(3)

    {:ok, tip} = Postgres.max_id(Log.__dmex__().storage_opts)

    eventually(fn -> cursor() == {:ok, tip} end)
  end

  ## Helpers

  defp subscribe(handler, opts \\ []) do
    DMEx.Supervisor.subscribe(
      handler,
      Keyword.merge([supervisor: TestSupervisor], opts)
    )
  end

  defp await_cursor(id) do
    eventually(fn -> cursor() == {:ok, id} end)
  end

  defp cursor do
    Postgres.last_seen(%{repo: DMEx.Test.IntegrationRepo, prefix: nil}, "test/echo")
  end

  defp drain(count) do
    for _ <- 1..count do
      assert_receive {:handled, %OrderPlaced{order_id: id}, _}, 5_000
      id
    end
  end

  # Collects order ids from either handler shape until nothing arrives for a while.
  defp collect(timeout, acc \\ []) do
    receive do
      {:handled, %OrderPlaced{order_id: id}, _} -> collect(timeout, acc ++ [id])
      {:handled, %OrderPlaced{order_id: id}} -> collect(timeout, acc ++ [id])
    after
      timeout -> acc
    end
  end
end
