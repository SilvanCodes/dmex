defmodule DMEx.Handler.PipelineFailureTest do
  @moduledoc """
  `on_failure` — section 6 of `DMEx.Handler.Pipeline`.

  Split from `DMEx.Handler.PipelineTest` by subject rather than by module:
  that one is about what the pipeline does with a row, this one is about what it
  does once a handler has run out of attempts.
  """

  use DMEx.Case, async: true, database: true

  alias DMEx.Config
  alias DMEx.Handler.Pipeline
  alias DMEx.Storage.Postgres
  alias DMEx.Test.Handlers
  alias DMEx.Test.Messages.OrderPlaced
  alias Ecto.Adapters.SQL

  @url "https://orders.internal/stream"

  setup do
    Handlers.report_to(self())

    SQL.query!(Repo, "CREATE TABLE dmex_test_scratch (id bigserial PRIMARY KEY, note text)", [])

    %{storage_opts: %{repo: Repo, prefix: nil}}
  end

  describe "local retries" do
    # Deliberately at the default `on_failure`, because the retry step is
    # identical in all three modes — only the terminal action after them differs.
    test "a failing handler is retried up to max_attempts", ctx do
      state = state(ctx, handler: Handlers.Raising, max_attempts: 3)

      {:ok, %{cursor: 1}} = Pipeline.run(row(1), state)

      # The message is still in hand after a rollback, so a retry is just
      # calling the handler again — no refetch, no reconnect.
      assert attempts() == 3
    end

    test "max_attempts of 1 means no retry at all", ctx do
      state = state(ctx, handler: Handlers.Raising, max_attempts: 1)

      {:ok, %{cursor: 1}} = Pipeline.run(row(1), state)

      assert attempts() == 1
    end

    test "a handler that recovers commits without reaching on_failure", ctx do
      # Fails once, then succeeds — the transient case retries exist for.
      Handlers.FailTimes.set(1)
      state = state(ctx, handler: Handlers.FailTimes, max_attempts: 3)

      assert {:ok, %{cursor: 1}} = Pipeline.run(row(1), state)
      assert {:ok, 1} = Postgres.last_seen(ctx.storage_opts, "test/fail_times")
    end

    test "each attempt gets its own telemetry span", ctx do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:dmex, :handler, :message, :stop],
          [:dmex, :handler, :message, :exception]
        ])

      Handlers.FailTimes.set(1)
      state = state(ctx, handler: Handlers.FailTimes, max_attempts: 3)

      Pipeline.run(row(1), state)

      # No span ever covers the backoff sleep, so a span's duration is what
      # the handler cost rather than how long the schedule waited.
      assert_receive {[:dmex, :handler, :message, :exception], ^ref, _,
                      %{attempt: 1, handler: Handlers.FailTimes}}

      assert_receive {[:dmex, :handler, :message, :stop], ^ref, _,
                      %{attempt: 2, handler: Handlers.FailTimes}}
    end
  end

  describe ":crash" do
    test "re-raises the original exception once retries are exhausted", ctx do
      state = state(ctx, handler: Handlers.Raising, on_failure: :crash)

      assert_raise RuntimeError, "handler exploded", fn -> Pipeline.run(row(1), state) end
    end

    test "leaves the cursor untouched, so the message is re-streamed on restart", ctx do
      state = state(ctx, handler: Handlers.Raising, on_failure: :crash)

      catch_error(Pipeline.run(row(1), state))

      assert {:ok, nil} = Postgres.last_seen(ctx.storage_opts, "test/raising")
    end

    test "writes no dead message", ctx do
      state = state(ctx, handler: Handlers.Raising, on_failure: :crash)

      catch_error(Pipeline.run(row(1), state))

      assert dead_count() == 0
    end
  end

  describe ":skip" do
    test "advances past the failing message and carries on", ctx do
      state = state(ctx, handler: Handlers.Raising, on_failure: :skip)

      assert {:ok, %{cursor: 1}} = Pipeline.run(row(1), state)
      assert {:ok, 1} = Postgres.last_seen(ctx.storage_opts, "test/raising")
    end

    test "does not stop the rest of the batch", ctx do
      state = state(ctx, handler: Handlers.Raising, on_failure: :skip)

      assert {:ok, %{cursor: 3}} = run_all([row(1), row(2), row(3)], state)
    end

    # The message is simply gone from this consumer's perspective, and this
    # event is the only trace DMEx leaves — worth knowing before choosing it.
    test "records nothing beyond a telemetry event", ctx do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :handler, :failed]])
      state = state(ctx, handler: Handlers.Raising, on_failure: :skip)

      Pipeline.run(row(1), state)

      # The handler is matched in the pattern, not asserted afterwards:
      # telemetry handlers are global, so an async sibling's event arrives with
      # this test's own ref and assert_receive scans for a match.
      assert_receive {[:dmex, :handler, :failed], ^ref, measurements,
                      %{on_failure: :skip, handler: Handlers.Raising} = metadata}

      assert measurements.id == 1
      assert measurements.attempts == 3
      assert metadata.reason == "handler exploded"

      assert dead_count() == 0
    end
  end

  describe ":dead_message" do
    test "records the message and advances past it", ctx do
      state = state(ctx, handler: Handlers.Raising, on_failure: :dead_message)

      assert {:ok, %{cursor: 1}} = Pipeline.run(row(1), state)
      assert {:ok, 1} = Postgres.last_seen(ctx.storage_opts, "test/raising")
      assert dead_count() == 1
    end

    # The stored row is the log row, verbatim — same columns, same meanings —
    # which is what makes replay a matter of handing it back rather than
    # rebuilding a message out of it.
    test "stores the message exactly as the log held it", ctx do
      state = state(ctx, handler: Handlers.Raising, on_failure: :dead_message)

      Pipeline.run(row(1, metadata: %{"trace_id" => "abc"}), state)

      assert [[subscription, id, type, data, metadata, inserted_at]] =
               query("""
               SELECT subscription, id, message_type,
                      data::text, metadata::text, inserted_at
                 FROM dmex_dead_messages
               """)

      # The declared identity, not the module name — which is what makes
      # renaming the handler free.
      assert subscription == "test/raising"
      # The log's own id, not a surrogate — so duplicate suppression
      # still means what it always meant.
      assert id == 1
      assert type == "DMEx.Test.Messages.OrderPlaced"
      assert JSON.decode!(data) == %{"order_id" => 1}
      assert JSON.decode!(metadata) == %{"trace_id" => "abc"}

      # The one column that does not mean what it means in the log: when this
      # row landed here, not when the message was appended — which is the axis
      # anything pruning this table has to work along.
      assert DateTime.diff(DateTime.utc_now(), inserted_at, :second) < 60
    end

    # A failure is an event, not a property of the message. Persisting it would
    # make each row a mixture of a message and an incident, and stop it being
    # replayable as the former.
    test "stores nothing about the failure itself", ctx do
      state = state(ctx, handler: Handlers.Raising, on_failure: :dead_message)

      Pipeline.run(row(1), state)

      assert [columns] =
               query("""
               SELECT array_agg(column_name::text ORDER BY column_name)
                 FROM information_schema.columns
                WHERE table_name = 'dmex_dead_messages'
               """)

      assert columns == [~w(data id inserted_at message_type metadata subscription)]
    end

    test "reports the failure through logs and telemetry instead", ctx do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :handler, :failed]])
      state = state(ctx, handler: Handlers.Raising, on_failure: :dead_message)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          Pipeline.run(row(1), state)
        end)

      assert log =~ "gave up on message 1"
      assert log =~ "handler exploded"

      assert_receive {[:dmex, :handler, :failed], ^ref, measurements,
                      %{on_failure: :dead_message, handler: Handlers.Raising} = metadata}

      assert measurements.attempts == 3
      assert metadata.reason == "handler exploded"
    end

    # The atomicity is the point: there is no state where a message was skipped
    # without being recorded, or recorded without being skipped.
    test "the dead row and the cursor advance commit together", ctx do
      state = state(ctx, handler: Handlers.Raising, on_failure: :dead_message)

      # First pass: message 2 dies, is recorded, and the cursor moves to 2.
      assert {:ok, %{cursor: 2}} = Pipeline.run(row(2), state)
      assert dead_count() == 1

      # Now rewind the cursor to 1 and feed message 2 again. Its dead-message
      # insert will conflict on (subscription, id), which aborts the transaction
      # the cursor advance is sharing.
      :ok = Postgres.advance(ctx.storage_opts, "test/raising", 1)

      assert catch_error(Pipeline.run(row(2), %{state | cursor: 1}))

      # The advance went down with the failed insert rather than committing on
      # its own — which is the whole reason they share a transaction. Without
      # that, the cursor would now read 2 and the message would be both skipped
      # and unrecorded.
      assert {:ok, 1} = Postgres.last_seen(ctx.storage_opts, "test/raising")
      assert dead_count() == 1
    end

    test "does not stop the rest of the batch", ctx do
      state = state(ctx, handler: Handlers.Raising, on_failure: :dead_message)

      assert {:ok, %{cursor: 3}} = run_all([row(1), row(2), row(3)], state)
      assert dead_count() == 3
    end
  end

  describe "successful messages are unaffected" do
    test "a handler that works never reaches any of this", ctx do
      state = state(ctx, handler: Handlers.Echo, on_failure: :dead_message)

      assert {:ok, %{cursor: 1}} = Pipeline.run(row(1), state)

      assert_received {:handled, %OrderPlaced{order_id: 1}, _}
      assert dead_count() == 0
    end
  end

  ## Helpers

  defp state(ctx, opts) do
    {config_opts, opts} = Keyword.split(opts, [:on_failure, :max_attempts])

    Pipeline.build(
      storage: DMEx.Storage.Postgres,
      storage_opts: ctx.storage_opts,
      handler: Keyword.fetch!(opts, :handler),
      origin: @url,
      cursor: 0,
      # A short backoff: these tests are about which terminal action runs, not
      # about how long the schedule waits.
      config: Config.handler!([retry_backoff: 1, retry_backoff_max: 2] ++ config_opts)
    )
  end

  # `DMEx.Handler.Stream` runs rows one at a time; this is the local
  # convenience for a test holding a whole batch already.
  defp run_all(rows, state) do
    Enum.reduce(rows, {:ok, state}, fn row, {:ok, state} -> Pipeline.run(row, state) end)
  end

  defp row(id, opts \\ []) do
    %{
      "id" => id,
      "message_type" => "DMEx.Test.Messages.OrderPlaced",
      "data" => %{"order_id" => id},
      "metadata" => Keyword.get(opts, :metadata, %{}),
      "inserted_at" => "2026-07-27T10:31:04.221Z"
    }
  end

  defp attempts(count \\ 0) do
    receive do
      {:attempted, _} -> attempts(count + 1)
    after
      0 -> count
    end
  end

  defp query(sql), do: SQL.query!(Repo, sql, []).rows

  defp dead_count do
    [[count]] = query("SELECT count(*) FROM dmex_dead_messages")
    count
  end
end
