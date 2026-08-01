defmodule DMEx.Handler.DeadMessagesTest do
  use DMEx.Case, async: true, database: true

  alias DMEx.Config
  alias DMEx.Handler.DeadMessages
  alias DMEx.Handler.Pipeline
  alias DMEx.Storage.Postgres
  alias DMEx.Test.Handlers
  alias DMEx.Test.Messages.OrderPlaced

  @moduletag :capture_log

  @url "https://orders.internal/stream"
  @handler Handlers.FailTimes
  @subscription "test/fail_times"

  defmodule Mismatched do
    @moduledoc """
    `Handlers.FailTimes`' identity with the wrong message set.

    Deliberately declares the same `{log, id}` — so it addresses the very same
    cursor and dead messages — while matching on a type those rows are not. It is
    how the replay tests reach "a dead row this handler cannot decode", given
    that the message set is whatever the clauses match on.

    Defined here rather than in `test/support` on purpose: modules compiled from
    a `.exs` are not in the application's module list, so discovery never sees
    this one — and a discovered pair sharing an id is exactly what
    `DMEx.Supervisor.__startable__!/2` refuses.
    """
    use DMEx.Handler, logs: [:test], id: :fail_times

    alias DMEx.Test.Handlers
    alias DMEx.Test.Messages.OrderShipped

    @impl true
    def handle_message(%OrderShipped{} = message, _metadata) do
      send(Handlers.owner(), {:attempted, message})

      :ok
    end
  end

  setup do
    Handlers.report_to(self())

    # `replay/2` takes no message set — it reads the handler's own.
    %{
      opts: [repo: Repo],
      replay: [repo: Repo],
      storage_opts: %{repo: Repo, prefix: nil}
    }
  end

  describe "list/2" do
    test "is empty when nothing has failed", ctx do
      assert {:ok, []} = DeadMessages.list(@handler, ctx.opts)
    end

    test "returns what a handler gave up on, oldest first", ctx do
      kill(ctx, 3)
      kill(ctx, 1)
      kill(ctx, 2)

      assert {:ok, rows} = DeadMessages.list(@handler, ctx.opts)
      assert Enum.map(rows, & &1.id) == [1, 2, 3]
    end

    test "is scoped to one handler", ctx do
      kill(ctx, 1)

      assert {:ok, []} = DeadMessages.list(Handlers.Echo, ctx.opts)
    end

    test "returns log rows, not a shape of their own", ctx do
      kill(ctx, 1)

      assert {:ok, [row]} = DeadMessages.list(@handler, ctx.opts)

      # Exactly what read/3 returns and what the pipeline consumes.
      assert Map.keys(row) |> Enum.sort() ==
               [:data, :id, :inserted_at, :message_type, :metadata, :previous_id]
    end
  end

  describe "replay/2" do
    # The whole reason the table has the log's shape: a dead row *is* a log row,
    # so replaying one is handing it back rather than rebuilding a message from
    # parts.
    test "feeds messages back to the handler once it accepts them", ctx do
      kill(ctx, 1)
      kill(ctx, 2)

      # "Fix the handler, deploy, replay" — here the fix is that it stops
      # raising.
      Handlers.FailTimes.set(0)

      assert %{replayed: 2, failed: 0} = DeadMessages.replay(@handler, ctx.replay)
    end

    test "delivers the same struct and metadata the handler would have seen", ctx do
      kill(ctx, 1, metadata: %{"trace_id" => "abc"})
      Handlers.FailTimes.set(0)

      DeadMessages.replay(@handler, ctx.replay)

      assert_received {:attempted, %OrderPlaced{order_id: 1}}
    end

    test "removes the rows it replayed", ctx do
      kill(ctx, 1)
      kill(ctx, 2)
      Handlers.FailTimes.set(0)

      DeadMessages.replay(@handler, ctx.replay)

      assert {:ok, []} = DeadMessages.list(@handler, ctx.opts)
    end

    test "leaves behind anything that fails again, without stopping the rest", ctx do
      kill(ctx, 1)
      kill(ctx, 2)

      # Still broken.
      Handlers.FailTimes.set(10)

      assert %{replayed: 0, failed: 2} = DeadMessages.replay(@handler, ctx.replay)

      assert {:ok, rows} = DeadMessages.list(@handler, ctx.opts)
      assert length(rows) == 2
    end

    # The handler call and the delete share one transaction, so a message leaves
    # the table exactly when its handler accepts it — never before.
    test "a failing replay rolls back its own delete", ctx do
      kill(ctx, 1)
      Handlers.FailTimes.set(10)

      DeadMessages.replay(@handler, ctx.replay)

      assert {:ok, [_row]} = DeadMessages.list(@handler, ctx.opts)
    end

    # Mismatched declares FailTimes' identity — so it addresses the very
    # same dead rows — while listing a type those rows are not. That is what
    # "cannot decode" looks like once the list lives on the module.
    test "replays only what it can decode, rather than losing the rest", ctx do
      kill(ctx, 1)
      Handlers.FailTimes.set(0)

      assert %{replayed: 0, failed: 1} =
               DeadMessages.replay(Mismatched, ctx.replay)

      assert {:ok, [_row]} = DeadMessages.list(@handler, ctx.opts)
    end

    test "rejects a module that is not a handler", ctx do
      assert_raise ArgumentError, ~r/is not a DMEx handler/, fn ->
        DeadMessages.replay(Handlers.NotAHandler, ctx.replay)
      end
    end

    test "does not touch the subscription's cursor", ctx do
      kill(ctx, 1)
      :ok = Postgres.advance(ctx.storage_opts, @subscription, 500)
      Handlers.FailTimes.set(0)

      DeadMessages.replay(@handler, ctx.replay)

      # A replayed message is delivered out of band; rewinding the cursor to
      # reach it would redeliver everything in between.
      assert {:ok, 500} = Postgres.last_seen(ctx.storage_opts, @subscription)
    end

    test "is a no-op when there is nothing dead", ctx do
      assert %{replayed: 0, failed: 0} = DeadMessages.replay(@handler, ctx.replay)
    end
  end

  describe "discard/3" do
    test "drops a message without running it", ctx do
      kill(ctx, 1)
      kill(ctx, 2)
      Handlers.FailTimes.set(0)

      assert :ok = DeadMessages.discard(@handler, 1, ctx.opts)

      assert {:ok, [row]} = DeadMessages.list(@handler, ctx.opts)
      assert row.id == 2
      refute_received {:attempted, _}
    end

    test "is fine on something already gone", ctx do
      assert :ok = DeadMessages.discard(@handler, 999, ctx.opts)
    end
  end

  ## Helpers

  # Drives a message through the real failure path, so these tests exercise rows
  # written exactly the way production writes them.
  defp kill(ctx, id, opts \\ []) do
    Handlers.FailTimes.set(1)

    state =
      Pipeline.build(
        storage: DMEx.Storage.Postgres,
        storage_opts: ctx.storage_opts,
        handler: @handler,
        url: @url,
        cursor: id - 1,
        config: Config.handler!(on_failure: :dead_message, max_attempts: 1)
      )

    {:ok, %{cursor: ^id}} =
      Pipeline.run(
        %{
          "id" => id,
          "message_type" => "DMEx.Test.Messages.OrderPlaced",
          "data" => %{"order_id" => id},
          "metadata" => Keyword.get(opts, :metadata, %{}),
          "inserted_at" => "2026-07-27T10:31:04.221Z"
        },
        state
      )

    drain()
  end

  defp drain do
    receive do
      {:attempted, _} -> drain()
    after
      0 -> :ok
    end
  end
end
