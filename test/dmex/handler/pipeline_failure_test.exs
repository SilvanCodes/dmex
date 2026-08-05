defmodule DMEx.Handler.PipelineFailureTest.FlakyStorage do
  @moduledoc """
  The in-memory stub with a scripted `transaction/2`.

  Local to this test rather than in `test/support`, because it models the one
  thing no other test cares about: storage failing *underneath* a handler that
  did nothing wrong. The script is a list of faults, one consumed per
  `transaction/2` call, `:ok` meaning "behave normally".
  """

  @behaviour DMEx.Storage

  alias DMEx.Test.Storage

  @doc "Storage opts whose `transaction/2` follows `faults`, then behaves."
  @spec new([:ok | :timeout | :commit]) :: DMEx.Storage.opts()
  def new(faults) do
    {:ok, pid} = Agent.start_link(fn -> faults end)

    Map.put(Storage.new(), :faults, pid)
  end

  @impl DMEx.Storage
  def transaction(opts, fun) do
    case next_fault(opts) do
      # A checkout that timed out: the callback never ran.
      :timeout ->
        {:error, :pool_timeout}

      # A commit that failed after the callback returned — a serialization
      # failure, say. The callback ran and its writes rolled back.
      :commit ->
        state = Agent.get(store(opts), & &1)
        _ = fun.()
        Agent.update(store(opts), fn _ -> state end)

        {:error, :serialization_failure}

      :ok ->
        Storage.transaction(opts, fun)
    end
  end

  defp next_fault(opts) do
    Agent.get_and_update(Map.fetch!(opts, :faults), fn
      [] -> {:ok, []}
      [fault | rest] -> {fault, rest}
    end)
  end

  defp store(opts), do: Map.fetch!(opts, :store)

  @impl DMEx.Storage
  defdelegate append(opts, type, data, metadata), to: Storage
  @impl DMEx.Storage
  defdelegate read(opts, cursor, limit, max_bytes, types), to: Storage
  @impl DMEx.Storage
  defdelegate max_id(opts), to: Storage
  @impl DMEx.Storage
  defdelegate appended_since(opts, cursor), to: Storage
  @impl DMEx.Storage
  defdelegate last_seen(opts, subscription), to: Storage
  @impl DMEx.Storage
  defdelegate cursor(opts, subscription), to: Storage
  @impl DMEx.Storage
  defdelegate delete_cursor(opts, subscription), to: Storage
  @impl DMEx.Storage
  defdelegate advance(opts, subscription, id), to: Storage
  @impl DMEx.Storage
  defdelegate dead_message(opts, attrs), to: Storage
  @impl DMEx.Storage
  defdelegate dead_messages(opts, subscription), to: Storage
  @impl DMEx.Storage
  defdelegate delete_dead_message(opts, subscription, id), to: Storage
end

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
  alias DMEx.Handler.PipelineFailureTest.FlakyStorage
  alias DMEx.Storage.Error
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
      assert_receive {[:dmex, :handler, :message, :exception], ^ref, _, %{attempt: 1, handler: Handlers.FailTimes}}

      assert_receive {[:dmex, :handler, :message, :stop], ^ref, _, %{attempt: 2, handler: Handlers.FailTimes}}
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

    # The pause has to happen in the process that is about to die: a restart
    # builds fresh state and connects immediately, so nothing after the crash is
    # in a position to pace the loop. Without it a poison message reconnects to
    # another application's log as fast as the supervisor can restart this
    # process — which is what `DMEx.Handler.Supervisor`'s absent restart budget
    # assumes is not happening.
    test "pauses for reconnect_backoff before re-raising", ctx do
      state =
        state(ctx,
          handler: Handlers.Raising,
          on_failure: :crash,
          reconnect_backoff: 200,
          reconnect_backoff_max: 200
        )

      {elapsed, _} = :timer.tc(fn -> catch_error(Pipeline.run(row(1), state)) end)

      # A jittered draw over [1, 200], so only the ceiling is assertable — and it
      # is generous, because it also covers scheduling on a loaded CI runner. A
      # loop with no pause at all lands two orders of magnitude below it.
      assert elapsed <= 500_000
      assert Enum.any?(1..12, fn _ -> pause_of(ctx) > 20 end)
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
      assert type == "order_placed"
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

  # Section 6 is about a message *a handler* could not process. Storage failing
  # underneath it is a different event with a different answer: nothing ran, or
  # nothing committed, so the message is still owed and the durable cursor still
  # points below it.
  describe "an infrastructure failure" do
    test "does not dead-letter a message the handler never rejected" do
      # Fails for every attempt and heals in time for the terminal action —
      # a transient blip, which is exactly what retries exist for.
      opts = FlakyStorage.new([:commit, :commit, :commit])
      state = stub_state(opts, handler: Handlers.Echo, max_attempts: 3)

      assert_raise Error, fn -> Pipeline.run(row(1), state) end

      assert stub_dead(opts, "test/echo") == []
      assert {:ok, nil} = FlakyStorage.last_seen(opts, "test/echo")
    end

    test "does not consume the handler's retry attempts" do
      opts = FlakyStorage.new([:commit, :commit, :commit])
      state = stub_state(opts, handler: Handlers.Echo, max_attempts: 3)

      catch_error(Pipeline.run(row(1), state))

      # The handler succeeded; retrying it three times would be answering a
      # database problem by re-running side effects that never failed.
      assert_received {:handled, %OrderPlaced{order_id: 1}, _}
      refute_received {:handled, _, _}
    end

    test "propagates when the callback never even ran" do
      opts = FlakyStorage.new([:timeout])
      state = stub_state(opts, handler: Handlers.Echo, max_attempts: 3)

      assert_raise Error, ~r/pool_timeout/, fn -> Pipeline.run(row(1), state) end

      refute_received {:handled, _, _}
      assert stub_dead(opts, "test/echo") == []
      assert {:ok, nil} = FlakyStorage.last_seen(opts, "test/echo")
    end

    # The two commits answer the same event the same way. Here the handler
    # genuinely failed, retries ran out, and it is the terminal action's own
    # transaction that cannot commit.
    test "on the terminal action is the same answer, not a different one" do
      opts = FlakyStorage.new([:ok, :ok, :ok, :commit])
      state = stub_state(opts, handler: Handlers.Raising, max_attempts: 3)

      assert_raise Error, fn -> Pipeline.run(row(1), state) end

      assert stub_dead(opts, "test/raising") == []
      assert {:ok, nil} = FlakyStorage.last_seen(opts, "test/raising")
    end

    test "in flush/1 is not silently swallowed either" do
      opts = FlakyStorage.new([:timeout])
      state = stub_state(opts, handler: Handlers.Echo, max_attempts: 3)

      assert_raise Error, fn ->
        Pipeline.flush(%{state | cursor: 7, pending: true})
      end
    end
  end

  ## Helpers

  # The pause is a jittered draw, so one sample proves nothing. Callers take
  # several and assert that at least one is clearly non-zero.
  defp pause_of(ctx) do
    state =
      state(ctx,
        handler: Handlers.Raising,
        on_failure: :crash,
        reconnect_backoff: 200,
        reconnect_backoff_max: 200
      )

    {elapsed, _} = :timer.tc(fn -> catch_error(Pipeline.run(row(1), state)) end)

    div(elapsed, 1000)
  end

  defp stub_state(storage_opts, opts) do
    Pipeline.build(
      storage: FlakyStorage,
      storage_opts: storage_opts,
      handler: Keyword.fetch!(opts, :handler),
      origin: @url,
      cursor: 0,
      config:
        Config.handler!(
          retry_backoff: 1,
          retry_backoff_max: 2,
          reconnect_backoff: 1,
          reconnect_backoff_max: 2,
          max_attempts: Keyword.fetch!(opts, :max_attempts)
        )
    )
  end

  defp stub_dead(storage_opts, subscription) do
    {:ok, rows} = FlakyStorage.dead_messages(storage_opts, subscription)

    rows
  end

  defp state(ctx, opts) do
    {config_opts, opts} =
      Keyword.split(opts, [:on_failure, :max_attempts, :reconnect_backoff, :reconnect_backoff_max])

    Pipeline.build(
      storage: Postgres,
      storage_opts: ctx.storage_opts,
      handler: Keyword.fetch!(opts, :handler),
      origin: @url,
      cursor: 0,
      # A short backoff: these tests are about which terminal action runs, not
      # about how long the schedule waits. `reconnect_backoff` matters here too —
      # `:crash` pauses on it before re-raising, and at the 500 ms default three
      # tests would sit in `Process.sleep/1` for no reason.
      config:
        Config.handler!(
          [
            retry_backoff: 1,
            retry_backoff_max: 2,
            reconnect_backoff: 1,
            reconnect_backoff_max: 2
          ] ++ config_opts
        )
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
      "message_type" => "order_placed",
      "data" => %{"order_id" => id},
      "metadata" => Keyword.get(opts, :metadata, %{})
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
