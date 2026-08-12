defmodule DMEx.Transport.LocalTest do
  # Not async: the reader runs in its own process and needs the shared sandbox,
  # and the log's own supervisor is a singleton.
  use DMEx.Case, async: false, database: true

  alias DMEx.Log.Streams
  alias DMEx.Storage.Postgres
  alias DMEx.Test.Handlers
  alias DMEx.Test.Log
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Messages.OrderShipped

  @moduletag :capture_log

  # `:test` bound to the module rather than to a URL is the whole of the
  # configuration: that is what makes these subscriptions local.
  @logs [test: Log]

  setup do
    Handlers.report_to(self())

    start_supervisor()

    :ok
  end

  describe "delivery" do
    # Appended first, then subscribed: the loop reads before it waits, so the
    # backlog is drained on connect. `LISTEN`/`NOTIFY` cannot fire under the
    # sandbox — nothing commits — which is exactly why reading before waiting is
    # the shape the loop has.
    test "runs an appended message through the handler, with no wire in between" do
      {:ok, id} = Log.append(%OrderPlaced{order_id: 7, total: 1999}, metadata: %{"who" => "me"})

      subscribe(Handlers.Echo)

      assert_receive {:handled, message, metadata}, 2_000

      assert message == %OrderPlaced{order_id: 7, customer_id: nil, total: 1999}
      assert metadata == %{"who" => "me"}

      # The cursor is durable and keyed exactly as a remote subscription's is.
      eventually(fn -> last_seen("test/echo") == id end)
    end

    test "delivers a backlog in log order" do
      for id <- 1..5, do: {:ok, _} = Log.append(%OrderPlaced{order_id: id})

      subscribe(Handlers.Echo)

      for id <- 1..5 do
        assert_receive {:handled, %OrderPlaced{order_id: ^id}, _}, 2_000
      end
    end

    # The handler's declared types go straight into the read as a type list —
    # no fingerprint is built, encoded or expanded, because nothing crosses a
    # wire to need one.
    test "reads only the message types the handler declares" do
      {:ok, _} = Log.append(%OrderShipped{order_id: 1})
      {:ok, _} = Log.append(%OrderPlaced{order_id: 2})

      subscribe(Handlers.Echo)

      assert_receive {:handled, %OrderPlaced{order_id: 2}, _}, 2_000
      refute_received {:handled, %OrderShipped{}, _}
    end

    test "a handler with a clause naming no type reads the whole log" do
      {:ok, _} = Log.append(%OrderPlaced{order_id: 1})

      subscribe(Handlers.EchoWholeLog)

      assert_receive {:handled, %OrderPlaced{order_id: 1}, _}, 2_000
      eventually(fn -> selections() == [:all] end)
    end
  end

  describe "last_seen" do
    # No socket, no header: the same two answers the wire carries, resolved
    # directly against the log's storage through the same `Request.position/2`.
    test "a cursor delivers everything above it" do
      {:ok, first} = Log.append(%OrderPlaced{order_id: 1})
      {:ok, _} = Log.append(%OrderPlaced{order_id: 2})

      subscribe(Handlers.Echo, last_seen: first)

      assert_receive {:handled, %OrderPlaced{order_id: 2}, _}, 2_000
      refute_received {:handled, %OrderPlaced{order_id: 1}, _}
    end

    # The default, and the reason it needs no tip token: unset resolves to the
    # log's own `max_id`, which is the tip as an exclusive cursor.
    test "unset skips the backlog" do
      {:ok, _} = Log.append(%OrderPlaced{order_id: 1})

      [pid] = subscribe(Handlers.Echo, last_seen: nil)

      # "Now" is resolved at connect, so the second append has to land after
      # that read — otherwise it is part of the backlog this is about skipping.
      eventually(fn -> :sys.get_state(pid).source != nil end)

      {:ok, _} = Log.append(%OrderPlaced{order_id: 2})

      assert_receive {:handled, %OrderPlaced{order_id: 2}, _}, 2_000
      refute_received {:handled, %OrderPlaced{order_id: 1}, _}
    end

    # A subscription that has never committed reads its durable cursor as
    # `:none`, which is what makes `last_seen:` apply exactly once. The
    # pipeline compares `id <= cursor` and must never see that atom: every
    # number sorts below every atom in Erlang term order, so `id <= :none` is
    # always true and *every row* would be dropped as a duplicate — a
    # subscription that connects, looks healthy and delivers nothing. The
    # transport still gets the raw `:none`; only the pipeline is normalised.
    test "a never-committed cursor reaches the pipeline as 0, not :none" do
      [pid] = subscribe(Handlers.Echo, last_seen: 0)

      assert {:ok, nil} = Postgres.last_seen("test/echo", Log.__dmex__().storage)

      eventually(fn -> :sys.get_state(pid).source != nil end)

      assert :sys.get_state(pid).pipeline.cursor == 0
    end

    test "a subscription with no durable cursor still delivers from the start" do
      {:ok, _} = Log.append(%OrderPlaced{order_id: 1})

      subscribe(Handlers.Echo, last_seen: 0)

      assert_receive {:handled, %OrderPlaced{order_id: 1}, _}, 2_000
    end
  end

  describe "the reader" do
    # The same entry an HTTP request process makes, carrying the same selection —
    # which is what gets a local reader wakeup routing and watcher reclaim
    # without reimplementing either.
    test "registers with the log, carrying its selection" do
      subscribe(Handlers.Echo)

      eventually(fn ->
        selections() == [MapSet.new(["order_placed"])]
      end)
    end

    # Its death is an ordinary disconnect: the subscription reconnects on its
    # backoff schedule and resumes from the durable cursor.
    test "dying reconnects the subscription rather than killing it" do
      [pid] = subscribe(Handlers.Echo)

      # `subscribe/2` returns once `init/1` has, and the reader is started in the
      # `handle_continue` after it — so there is a moment with no reader yet.
      first = eventually(fn -> reader(pid) end)
      Process.exit(first, :kill)

      # The backoff is jittered and this runs alongside the rest of the suite, so
      # give the reconnect room rather than racing it.
      eventually(fn -> Process.alive?(pid) and reader(pid) not in [nil, first] end, 10_000)

      {:ok, _} = Log.append(%OrderPlaced{order_id: 1})

      assert_receive {:handled, %OrderPlaced{order_id: 1}, _}, 5_000
    end

    test "goes away with the subscription" do
      [pid] = subscribe(Handlers.Echo)
      reader = eventually(fn -> reader(pid) end)

      :ok = DMEx.Supervisor.unsubscribe(Handlers.Echo, supervisor: TestSupervisor)

      eventually(fn -> not Process.alive?(reader) end)
    end
  end

  ## Helpers

  defp start_supervisor do
    start_supervised!({DMEx.Supervisor, name: TestSupervisor, otp_app: nil, repo: Repo, logs: @logs})
  end

  # `last_seen: 0` throughout: every test here appends first and subscribes
  # after, which is the backlog these assertions are about. The default —
  # unset — would correctly skip all of it.
  defp subscribe(handler, opts \\ [last_seen: 0]) do
    {:ok, pids} = DMEx.Supervisor.subscribe(handler, [supervisor: TestSupervisor] ++ opts)

    pids
  end

  defp reader(pid), do: :sys.get_state(pid).source[:reader]

  # `each/2` runs in the calling process, so this collects without a holder.
  defp selections do
    parent = self()

    :ok =
      Streams.each(Log, fn _pid, _status, selection ->
        send(parent, {:selection, selection})
      end)

    drain_selections([])
  end

  defp drain_selections(acc) do
    receive do
      {:selection, selection} -> drain_selections([selection | acc])
    after
      0 -> acc
    end
  end

  defp last_seen(subscription) do
    {:ok, cursor} = Postgres.last_seen(subscription, %{repo: Repo, prefix: nil})

    cursor
  end
end
