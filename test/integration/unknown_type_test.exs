defmodule DMEx.Integration.UnknownTypeTest do
  @moduledoc """
  What reaches a consumer, and what happens to what it cannot decode.

  A subscription's message set does two jobs: it is the selection the log
  resolves its filter against, so undecodable rows mostly never cross the wire
  at all — and it is what resolves a `type` back to a module for the
  ones that do.

  Both halves are tested here, because **the skip path is still load-bearing**.
  Filter inclusion is heuristic: there are no false negatives, but a false
  positive admits one extra type for the life of a connection, and a handler
  with a clause naming no type asks for the whole log deliberately. That handler
  decodes every type its own application knows, named or not — so what it still
  skips is a type *no* module here declares, and such a row must advance the
  cursor and invoke nothing or a consumer would wedge on the first one.
  """

  use DMEx.IntegrationCase

  alias DMEx.Storage.Postgres
  alias DMEx.Test.Handlers
  alias DMEx.Test.IntegrationLog, as: Log
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Messages.OrderShipped

  @undecodable "nothing.declares.this"

  setup do
    start_stack!()
  end

  test "a type the consumer did not select never arrives, and the rest do", _ctx do
    subscribe(Handlers.Echo)

    # Interleaved deliberately: an unselected type first, so a consumer that
    # wedged on it would never deliver anything at all.
    {:ok, _} = Log.append(%OrderShipped{order_id: 1, carrier: "dhl"})
    {:ok, _} = Log.append(%OrderPlaced{order_id: 2})
    {:ok, _} = Log.append(%OrderShipped{order_id: 3, carrier: "ups"})
    {:ok, id} = Log.append(%OrderPlaced{order_id: 4})

    assert_receive {:handled, %OrderPlaced{order_id: 2}, _}, 5_000
    assert_receive {:handled, %OrderPlaced{order_id: 4}, _}, 5_000
    refute_receive {:handled, %OrderShipped{}, _}, 200

    # The cursor is at the last *selected* row, which here is also the log's
    # tip. It moved straight over ids 1 and 3 without those rows ever being
    # sent — a selective stream sees a sparse subset of one id space, not a
    # space of its own.
    eventually(fn -> cursor() == {:ok, id} end)
  end

  test "selecting nothing that was appended receives nothing, and that is not a stall", _ctx do
    subscribe(Handlers.Echo)

    for n <- 1..5, do: {:ok, _} = Log.append(%OrderShipped{order_id: n, carrier: "dhl"})

    # Nothing matched, so nothing was sent and there is nothing for the cursor
    # to advance over: it stays unset rather than tracking the log's tip. That
    # is the visible consequence of selection — `last_seen` is the last id
    # *delivered*, and on a selective subscription it lags the log by however
    # much of it went to somebody else.
    #
    # It costs nothing to catch up from. `(type, id)` makes the next
    # read an index seek, which is the same price whether the next matching row
    # is one id away or ten million.
    refute_receive {:handled, _, _}, 500
    assert cursor() == {:ok, nil}

    # Anchored on a positive: without this the two assertions above also pass
    # for a subscription that never connected at all. One selected row proves
    # the stream is live and filtering, rather than absent.
    {:ok, placed} = Log.append(%OrderPlaced{order_id: 6})

    assert_receive {:handled, %OrderPlaced{order_id: 6}, _}, 5_000
    eventually(fn -> cursor() == {:ok, placed} end)
  end

  test "the whole log still arrives, and still skips, for a whole-log handler", _ctx do
    # The escape hatch, and the reason the skip path cannot be deleted: this is
    # v1's delivery model, on demand, for telling "pointed at the wrong
    # log" apart from "log has appended nothing".
    ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :handler, :skip]])

    subscribe(Handlers.EchoWholeLog)

    {:ok, _} = append_undecodable!()
    {:ok, id} = Log.append(%OrderPlaced{order_id: 2})

    # The undecodable row crossed the wire, invoked nothing, and did not wedge
    # the stream behind it.
    assert_receive {[:dmex, :handler, :skip], ^ref, _measurements, metadata}, 5_000
    assert metadata.type == @undecodable
    assert metadata.handler == Handlers.EchoWholeLog

    assert_receive {:handled, %OrderPlaced{order_id: 2}, _}, 5_000
    eventually(fn -> cursor("test/echo_whole_log") == {:ok, id} end)
  end

  # The other half of asking for the whole log: a type this handler never named
  # still resolves, through the modules its own application knows, and reaches
  # the clause that named no type.
  test "a whole-log handler decodes a type it never named", _ctx do
    subscribe(Handlers.EchoWholeLog)

    {:ok, _} = Log.append(%OrderShipped{order_id: 1, carrier: "dhl"})

    assert_receive {:caught, OrderShipped, %OrderShipped{order_id: 1, carrier: "dhl"}, _}, 5_000
  end

  # A clean stop must settle the run of skips it is holding, or the next start
  # re-reads and re-skips them for nothing. That flush lives in `terminate/2`,
  # which only runs because the stream traps exits — a non-trapping child of a
  # `DynamicSupervisor` is killed outright by `terminate_child/2` and the
  # callback never fires.
  test "unsubscribing settles a pending run of skips", _ctx do
    # Far longer than this test runs, so the only thing that can write the
    # cursor is the flush on the way down.
    {:ok, _} =
      DMEx.Supervisor.subscribe(Handlers.EchoWholeLog,
        supervisor: TestSupervisor,
        last_seen: 0,
        checkpoint_interval: 600_000
      )

    ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :handler, :skip]])

    {:ok, id} = append_undecodable!()

    assert_receive {[:dmex, :handler, :skip], ^ref, _measurements, _metadata}, 5_000
    assert cursor("test/echo_whole_log") == {:ok, nil}

    :ok = DMEx.Supervisor.unsubscribe(Handlers.EchoWholeLog, supervisor: TestSupervisor)

    eventually(fn -> cursor("test/echo_whole_log") == {:ok, id} end)
  end

  test "declaring both modules delivers both", _ctx do
    # The same list does double duty: it is what makes decoding possible and
    # what narrows the stream. Widening it is one edit — of the handler module,
    # which is where the list lives.
    subscribe(Handlers.EchoAll)

    {:ok, _} = Log.append(%OrderPlaced{order_id: 1})
    {:ok, _} = Log.append(%OrderShipped{order_id: 2, carrier: "dhl"})

    assert_receive {:handled, %OrderPlaced{order_id: 1}, _}, 5_000
    assert_receive {:handled, %OrderShipped{order_id: 2, carrier: "dhl"}, _}, 5_000
  end

  # The message set is part of the module now, so "the same handler with a
  # different list" is a different module.
  # `last_seen: 0`: these tests append after subscribing, and the default
  # resolves to the tip at connect, which would race them.
  defp subscribe(handler) do
    {:ok, _} = DMEx.Supervisor.subscribe(handler, supervisor: TestSupervisor, last_seen: 0)
  end

  # A row whose `type` no compiled module declares — the only thing a
  # whole-log handler skips now that it resolves everything its application
  # knows. Appended through the storage adapter, because `Log.append/2` takes the
  # type off a message module and a module is exactly what this row must not
  # have. No `NOTIFY` either, so it arrives on the dispatcher's next poll.
  defp append_undecodable! do
    DMEx.Storage.append(Log.__dmex__().storage, @undecodable, JSON.encode!(%{"order_id" => 1}), "{}")
  end

  defp cursor(subscription \\ "test/echo") do
    Postgres.last_seen(subscription, %{repo: DMEx.Test.IntegrationRepo, prefix: nil})
  end
end
