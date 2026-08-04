defmodule DMEx.Handler.PipelineTest do
  use DMEx.Case, async: true, database: true

  alias DMEx.Handler.Pipeline
  alias DMEx.Storage.Postgres
  alias DMEx.Test.Handlers
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Messages.OrderShipped
  alias Ecto.Adapters.SQL

  @url "https://orders.internal/stream"

  setup do
    Handlers.report_to(self())

    SQL.query!(Repo, "CREATE TABLE dmex_test_scratch (id bigserial PRIMARY KEY, note text)", [])

    %{storage_opts: %{repo: Repo, prefix: nil}}
  end

  describe "duplicate suppression" do
    # Delivery within a subscription is strictly ordered and last_seen is
    # durable, so "already processed" is exactly "id not greater than the
    # cursor" — no dedupe table and no new state.
    test "drops rows at or below the cursor and runs only what is new", ctx do
      state = state(ctx, handler: Handlers.Echo, cursor: 5)

      assert {:ok, %{cursor: 7}} = run_all(rows(3..7), state)

      assert_received {:handled, %OrderPlaced{order_id: 6}, _}
      assert_received {:handled, %OrderPlaced{order_id: 7}, _}
      refute_received {:handled, %OrderPlaced{order_id: 5}, _}
      refute_received {:handled, %OrderPlaced{order_id: 4}, _}
    end

    test "a duplicate opens no transaction at all", ctx do
      state = state(ctx, handler: Handlers.Echo, cursor: 10)

      assert {:ok, %{cursor: 10, pending: false}} = Pipeline.run(row(3), state)

      # The cursor was never written, so nothing was committed for it either.
      assert {:ok, nil} = Postgres.last_seen(ctx.storage_opts, "test/echo")
    end

    test "makes a replayed frame harmless", ctx do
      state = state(ctx, handler: Handlers.Echo, cursor: 0)

      {:ok, %{cursor: cursor} = state} = run_all(rows(1..3), state)
      {:ok, %{cursor: ^cursor}} = run_all(rows(1..3), state)

      for id <- 1..3, do: assert_received({:handled, %OrderPlaced{order_id: ^id}, _})
      refute_received {:handled, _, _}
    end
  end

  describe "an unknown message type" do
    # The direct consequence of v1 having no server-side selection: a
    # subscription receives the log's entire log, so rows the consumer has
    # no module for are the normal case, not a fault.
    test "invokes nothing but still advances the cursor", ctx do
      state = state(ctx, handler: Handlers.Echo, cursor: 0)

      assert {:ok, %{cursor: 1, pending: true} = state} = Pipeline.run(unknown_row(1), state)

      refute_received {:handled, _, _}

      # Deferred, not written: a skip runs no handler, so its advance is atomic
      # with nothing and there is nothing to be in a transaction with.
      assert {:ok, nil} = Postgres.last_seen(ctx.storage_opts, "test/echo")

      assert {:ok, %{pending: false}} = Pipeline.flush(state)
      assert {:ok, 1} = Postgres.last_seen(ctx.storage_opts, "test/echo")
    end

    # Without the advance, a consumer would wedge forever on the first message
    # type it does not know.
    test "does not wedge the subscription behind it", ctx do
      state = state(ctx, handler: Handlers.Echo, cursor: 0)

      assert {:ok, %{cursor: 3}} =
               run_all([unknown_row(1), row(2), unknown_row(3)], state)

      assert_received {:handled, %OrderPlaced{order_id: 2}, _}

      # Only row 2 committed, and it committed an advance to 2 — which subsumed
      # the skip at 1. Row 3 is still pending.
      assert {:ok, 2} = Postgres.last_seen(ctx.storage_opts, "test/echo")
    end

    # A consumer silently discarding most of a stream and one silently
    # discarding the single type it wanted look identical otherwise.
    test "emits a skip event naming the type", ctx do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :handler, :skip]])
      state = state(ctx, handler: Handlers.Echo, cursor: 0)

      Pipeline.run(unknown_row(1), state)

      assert_receive {[:dmex, :handler, :skip], ^ref, %{id: 1},
                      %{handler: Handlers.Echo} = metadata}

      assert metadata.message_type == "SomeOtherApp.Messages.Unknown"
      assert metadata.origin == @url
    end
  end

  describe "a clause narrower than its message type" do
    # Selection is by type, so a clause matching a field's value gets sent
    # everything of that type. The ones it turns down are the same non-fault an
    # undeclared type is: the handler was never entered.
    test "invokes nothing but still advances the cursor", ctx do
      state = state(ctx, handler: Handlers.Narrowed, cursor: 0)
      row = narrowed_row(1, 9)

      assert {:ok, %{cursor: 1, pending: true} = state} = Pipeline.run(row, state)

      refute_received {:handled, _, _}
      assert {:ok, nil} = Postgres.last_seen(ctx.storage_opts, "test/narrowed")

      assert {:ok, %{pending: false}} = Pipeline.flush(state)
      assert {:ok, 1} = Postgres.last_seen(ctx.storage_opts, "test/narrowed")
    end

    test "still runs the messages it does match", ctx do
      state = state(ctx, handler: Handlers.Narrowed, cursor: 0)

      rows = [narrowed_row(1, 9), narrowed_row(2, 7), narrowed_row(3, 9)]

      assert {:ok, %{cursor: 3}} = run_all(rows, state)

      assert_received {:handled, %OrderPlaced{order_id: 2, customer_id: 7}, _}
      refute_received {:handled, _, _}

      # Row 2 committed an advance to 2, subsuming the skip at 1; row 3 is still
      # pending — exactly the unknown-type shape.
      assert {:ok, 2} = Postgres.last_seen(ctx.storage_opts, "test/narrowed")
    end

    test "emits a skip event naming the type", ctx do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :handler, :skip]])
      state = state(ctx, handler: Handlers.Narrowed, cursor: 0)

      Pipeline.run(narrowed_row(1, 9), state)

      assert_receive {[:dmex, :handler, :skip], ^ref, %{id: 1}, metadata}

      assert metadata.handler == Handlers.Narrowed
      assert metadata.message_type == "order_placed"
    end

    test "respects a guard as well as a pattern", ctx do
      state = state(ctx, handler: Handlers.Guarded, cursor: 0)

      assert {:ok, %{cursor: 2}} = run_all([total_row(1, 50), total_row(2, 500)], state)

      assert_received {:handled, %OrderPlaced{order_id: 2, total: 500}, _}
      refute_received {:handled, _, _}
    end

    # A skip decided before the transaction opens is a skip in every sense: no
    # handler ran, so there is no attempt to span.
    test "runs no message span at all", ctx do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:dmex, :handler, :message, :start],
          [:dmex, :handler, :message, :stop],
          [:dmex, :handler, :message, :exception]
        ])

      state = state(ctx, handler: Handlers.Narrowed, cursor: 0)

      Pipeline.run(narrowed_row(1, 9), state)

      refute_receive {[:dmex, :handler, :message, _], ^ref, _, _}
    end

    # The one thing this must not do: a clause head that did not match means
    # nothing ran, but a FunctionClauseError from *inside* a body is a handler
    # that failed halfway and has to be treated as one.
    test "does not swallow a FunctionClauseError raised by the handler's body", ctx do
      state = state(ctx, handler: Handlers.RaisesFunctionClause, cursor: 0)

      assert_raise FunctionClauseError, fn -> Pipeline.run(row(1), state) end

      assert {:ok, nil} = Postgres.last_seen(ctx.storage_opts, "test/raises_function_clause")
    end
  end

  describe "coalescing skipped rows" do
    # The measured motivation: a mostly-skipped stream was spending everything
    # it had on commits for rows nothing was done with.
    test "a run of skips costs one write, not one per row", ctx do
      state = state(ctx, handler: Handlers.Echo, cursor: 0)

      {:ok, state} = run_all(Enum.map(1..50, &unknown_row/1), state)

      assert state.pending
      assert state.cursor == 50
      assert {:ok, nil} = Postgres.last_seen(ctx.storage_opts, "test/echo")

      assert {:ok, %{pending: false}} = Pipeline.flush(state)
      assert {:ok, 50} = Postgres.last_seen(ctx.storage_opts, "test/echo")
    end

    # `advance/3` sets last_seen absolutely, so a handled row's own transaction
    # already carries every skip below it. That is what keeps "flush handled
    # messages immediately" from costing a second write.
    test "a handled message subsumes the pending run rather than adding a write", ctx do
      state = state(ctx, handler: Handlers.Echo, cursor: 0)

      rows = [unknown_row(1), unknown_row(2), unknown_row(3), row(4)]
      {:ok, state} = run_all(rows, state)

      refute state.pending
      assert {:ok, 4} = Postgres.last_seen(ctx.storage_opts, "test/echo")

      # And the flush that follows is a no-op, not a redundant second write.
      assert {:ok, ^state} = Pipeline.flush(state)
    end

    test "flushing with nothing pending writes nothing at all", ctx do
      state = state(ctx, handler: Handlers.Echo, cursor: 0)

      assert {:ok, ^state} = Pipeline.flush(state)
      assert {:ok, nil} = Postgres.last_seen(ctx.storage_opts, "test/echo")
    end

    # A pending run is rows no code ran for, so losing it costs re-skipping and
    # never a lost or twice-handled message.
    test "a crash mid-run loses only the skipping, and it is redone", ctx do
      state = state(ctx, handler: Handlers.Echo, cursor: 0)

      {:ok, crashed} = run_all(Enum.map(1..5, &unknown_row/1), state)
      assert crashed.pending
      assert crashed.cursor == 5

      # Restart: the durable cursor is the authority, and it never moved.
      assert {:ok, nil} = Postgres.last_seen(ctx.storage_opts, "test/echo")

      {:ok, resumed} = run_all(Enum.map(1..5, &unknown_row/1), state)
      {:ok, resumed} = Pipeline.flush(resumed)

      assert resumed.cursor == 5
      assert {:ok, 5} = Postgres.last_seen(ctx.storage_opts, "test/echo")
      refute_received {:handled, _, _}
    end

    test "emits a checkpoint event carrying the position persisted", ctx do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :handler, :checkpoint]])
      state = state(ctx, handler: Handlers.Echo, cursor: 0)

      {:ok, state} = run_all([unknown_row(1), unknown_row(2)], state)
      Pipeline.flush(state)

      assert_receive {[:dmex, :handler, :checkpoint], ^ref, %{id: 2},
                      %{handler: Handlers.Echo, subscription: "test/echo"}}
    end
  end

  describe "a successful handler" do
    test "advances the cursor exactly once", ctx do
      state = state(ctx, handler: Handlers.Echo, cursor: 0)

      assert {:ok, %{cursor: 1, pending: false}} = Pipeline.run(row(1), state)

      assert {:ok, 1} = Postgres.last_seen(ctx.storage_opts, "test/echo")
    end

    # Everything else in the row stays with DMEx: the id, the message_type, the
    # frame's max_id. None of it is a property of the message.
    test "receives the decoded struct and the publisher's metadata, and nothing else", ctx do
      state = state(ctx, handler: Handlers.Echo, cursor: 0)

      row = %{
        "id" => 1,
        "message_type" => "order_placed",
        "data" => %{"order_id" => 7, "total" => 1999},
        "metadata" => %{"trace_id" => "abc"}
      }

      Pipeline.run(row, state)

      assert_received {:handled, message, metadata}
      assert message == %OrderPlaced{order_id: 7, customer_id: nil, total: 1999}
      assert metadata == %{"trace_id" => "abc"}
    end

    test "decodes into whichever module the type names", ctx do
      state = state(ctx, handler: Handlers.EchoAll, cursor: 0)

      run_all([row(1), shipped_row(2)], state)

      assert_received {:handled, %OrderPlaced{}, _}
      assert_received {:handled, %OrderShipped{order_id: 2, carrier: "dhl"}, _}
    end

    test "tolerates a row with no metadata", ctx do
      state = state(ctx, handler: Handlers.Echo, cursor: 0)

      Pipeline.run(Map.delete(row(1), "metadata"), state)

      assert_received {:handled, _, %{}}
    end

    # Casting is CPU work, not IO; holding a database connection across it would
    # inflate transaction — and therefore lock — time for nothing.
    test "casts outside the transaction", ctx do
      state = state(ctx, handler: Handlers.ChecksTransaction, cursor: 0)

      Pipeline.run(row(1), state)

      # The handler itself *is* inside one — that is the whole point.
      assert_received {:in_transaction?, true}
    end
  end

  describe "a raising handler" do
    test "propagates the exception", ctx do
      state = state(ctx, handler: Handlers.Raising, cursor: 0)

      assert_raise RuntimeError, "handler exploded", fn -> Pipeline.run(row(1), state) end
    end

    test "leaves the cursor where it was", ctx do
      state = state(ctx, handler: Handlers.Raising, cursor: 0)

      catch_error(Pipeline.run(row(1), state))

      assert {:ok, nil} = Postgres.last_seen(ctx.storage_opts, "test/raising")
    end

    # The rollback probe. If the scratch row survived, the callback and the
    # cursor advance were not in one transaction, and a handler's side effects
    # could outlive a cursor that never moved.
    test "rolls back the handler's own writes with the cursor", ctx do
      state = state(ctx, handler: Handlers.WritesThenRaises, cursor: 0)

      catch_error(Pipeline.run(row(1), state))

      assert [[0]] = SQL.query!(Repo, "SELECT count(*) FROM dmex_test_scratch", []).rows
      assert {:ok, nil} = Postgres.last_seen(ctx.storage_opts, "test/writes_then_raises")
    end

    test "stops the batch at the failing row rather than skipping past it", ctx do
      state = state(ctx, handler: Handlers.Raising, cursor: 0)

      catch_error(run_all(rows(1..3), state))

      assert_received {:attempted, %OrderPlaced{order_id: 1}}
      refute_received {:attempted, %OrderPlaced{order_id: 2}}
    end
  end

  describe "build/1" do
    test "rejects a module that is not a handler", ctx do
      assert_raise ArgumentError, ~r/is not a DMEx handler.*use DMEx.Handler/s, fn ->
        state(ctx, handler: Handlers.NotAHandler)
      end
    end

    # Nothing is carried for resolving a type: the handler's own
    # `__dmex_cast__/3` is compiled from the set its clauses declare.
    test "carries no message registry", ctx do
      refute Map.has_key?(state(ctx, handler: Handlers.EchoAll), :modules)

      assert %OrderShipped{order_id: 1} =
               Handlers.EchoAll.__dmex_cast__("order_shipped", %{"order_id" => 1}, %{})

      assert Handlers.EchoAll.__dmex_cast__("nothing_it_declares", %{}, %{}) == nil
    end

    test "carries the handler's subscription id", ctx do
      assert state(ctx, handler: Handlers.Echo).subscription == "test/echo"
    end

    # No runtime check that the modules are messages, and nothing for one to
    # catch: the list is derived from the clause patterns and each is verified at
    # compile time. See DMEx.HandlerTest.
  end

  describe "telemetry" do
    test "spans each handled message" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:dmex, :handler, :message, :start],
          [:dmex, :handler, :message, :stop]
        ])

      state =
        state(%{storage_opts: %{repo: Repo, prefix: nil}}, handler: Handlers.Echo, cursor: 0)

      Pipeline.run(row(1), state)

      assert_receive {[:dmex, :handler, :message, :start], ^ref, _,
                      %{handler: Handlers.Echo, id: 1} = metadata}

      assert metadata.message_type == "order_placed"

      assert_receive {[:dmex, :handler, :message, :stop], ^ref, %{duration: _},
                      %{handler: Handlers.Echo}}
    end

    test "reports an exception rather than swallowing it" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:dmex, :handler, :message, :exception]
        ])

      state =
        state(%{storage_opts: %{repo: Repo, prefix: nil}}, handler: Handlers.Raising, cursor: 0)

      catch_error(Pipeline.run(row(1), state))

      assert_receive {[:dmex, :handler, :message, :exception], ^ref, _,
                      %{handler: Handlers.Raising}}
    end
  end

  ## Helpers

  # `on_failure: :crash` explicitly, because these tests are about what the
  # pipeline does with an exception rather than about what the default does with
  # a poison message — the terminal action is `DMEx.Handler.PipelineFailureTest`'s
  # subject, and the default is `:dead_message`.
  defp state(ctx, opts) do
    Pipeline.build(
      storage: DMEx.Storage.Postgres,
      storage_opts: ctx.storage_opts,
      handler: Keyword.fetch!(opts, :handler),
      origin: @url,
      cursor: Keyword.get(opts, :cursor, 0),
      config: DMEx.Config.handler!(on_failure: :crash)
    )
  end

  # `DMEx.Handler.Stream` runs rows one at a time; this is the local
  # convenience for a test holding a whole batch already.
  defp run_all(rows, state) do
    Enum.reduce(rows, {:ok, state}, fn row, {:ok, state} -> Pipeline.run(row, state) end)
  end

  defp rows(range), do: Enum.map(range, &row/1)

  defp row(id) do
    %{
      "id" => id,
      "message_type" => "order_placed",
      "data" => %{"order_id" => id},
      "metadata" => %{}
    }
  end

  defp shipped_row(id) do
    %{
      "id" => id,
      "message_type" => "order_shipped",
      "data" => %{"order_id" => id, "carrier" => "dhl"},
      "metadata" => %{}
    }
  end

  defp narrowed_row(id, customer_id) do
    %{row(id) | "data" => %{"order_id" => id, "customer_id" => customer_id}}
  end

  defp total_row(id, total) do
    %{row(id) | "data" => %{"order_id" => id, "total" => total}}
  end

  defp unknown_row(id) do
    %{row(id) | "message_type" => "SomeOtherApp.Messages.Unknown"}
  end
end
