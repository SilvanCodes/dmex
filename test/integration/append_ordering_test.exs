# The most dangerous failure mode in the design, and the only test that can see
# it: a reader that steps over a row that had not committed yet, and never comes
# back for it.
#
# Postgres allocates a sequence value at INSERT but makes the row visible at
# COMMIT. If T1 takes `id=100` and T2 takes `id=101` but T2 commits first, a
# reader scanning `id > 99 ORDER BY id` sees 101, advances its cursor past it,
# and **permanently skips message 100** once T1 finally commits. Nothing
# downstream detects that: `last_seen` advanced right over the gap, and every
# later read is correct. It is silent data loss, which is why `DMEx.Storage.Postgres`
# serializes appends with a transaction-scoped advisory lock.
#
# SQLite prevents it structurally instead: there is one write transaction
# database-wide, so a second writer cannot draw a rowid until the first has
# committed, and assignment order *is* commit order. `DMEx.Storage.SQLite` takes
# no lock at all.
#
# So the contract is one — under real concurrency a cursor-following reader sees
# every row, in order, exactly once — and both engines are held to it here. The
# *mechanism* is not shared, and the two engine-guarded blocks below are the
# whole of the difference: there is no lock to hold or release on SQLite, and no
# way to stage the inversion the advisory lock exists to prevent.
#
# A `for` comprehension over the engines, as in
# `test/dmex/storage/conformance_test.exs`: every test stays an ordinary
# top-level `test`, with line numbers that point at the thing you are reading.
#
# Not sandboxed, and deliberately: a sandbox would put every append inside one
# transaction, which is precisely the property under test.
for {storage, repo, log, tag} <- [
      {DMEx.Storage.Postgres, DMEx.Test.IntegrationRepo, DMEx.Test.IntegrationLog, :database},
      {DMEx.Storage.SQLite, DMEx.Test.SQLiteIntegrationRepo, DMEx.Test.SQLiteLog, :sqlite}
    ] do
  postgres? = storage == DMEx.Storage.Postgres

  defmodule Module.concat(storage, AppendOrderingTest) do
    use ExUnit.Case, async: false

    alias DMEx.Test.Messages.OrderPlaced
    alias Ecto.Adapters.SQL

    @storage storage
    @repo repo
    @log log

    @moduletag :integration
    @moduletag tag
    @moduletag :capture_log

    @writers 8
    @per_writer 25
    @total @writers * @per_writer
    @megabyte 1_048_576

    setup do
      truncate_all!()
      on_exit(&truncate_all!/0)
      :ok
    end

    if postgres? do
      describe "the lock itself" do
        test "append takes a transaction-scoped advisory lock on the log's key" do
          {classid, objid} = storage().lock

          @repo.transaction(fn ->
            {:ok, _id} = @log.append(%OrderPlaced{order_id: 1})

            # Still held *inside* the transaction. A lock released at the end of
            # the INSERT would let a later-allocated id commit first, which is
            # the exact failure it exists to prevent.
            assert [[1]] =
                     SQL.query!(
                       @repo,
                       """
                       SELECT count(*) FROM pg_locks
                        WHERE locktype = 'advisory'
                          AND classid = $1 AND objid = $2
                          AND pid = pg_backend_pid()
                       """,
                       [classid, objid]
                     ).rows
          end)
        end

        test "the lock is released by COMMIT, not left behind" do
          {classid, objid} = storage().lock

          {:ok, _} = @log.append(%OrderPlaced{order_id: 1})

          assert [[0]] =
                   SQL.query!(
                     @repo,
                     "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND classid = $1 AND objid = $2",
                     [classid, objid]
                   ).rows
        end

        test "an append through another log on the same table waits for it" do
          # Sharing the lock is the point of keying it on the table: two log
          # modules writing one `dmex_messages` that took *different* locks would
          # not exclude each other, and their ids would interleave with commit
          # order. Co-tenants that must not serialize against each other get
          # their own schema with `:prefix`, which is what changes the key.
          parent = self()

          other =
            Task.async(fn ->
              assert_receive :go, 5_000
              {:ok, id} = DMEx.Test.CoTenantLog.append(%OrderPlaced{order_id: 2})
              send(parent, :appended)
              id
            end)

          @repo.transaction(fn ->
            {:ok, _id} = @log.append(%OrderPlaced{order_id: 1})
            send(other.pid, :go)

            # Blocked on this transaction's lock, and nothing releases it before
            # COMMIT — so this is the design and not a timing accident.
            refute_receive :appended, 500
          end)

          assert is_integer(Task.await(other, 10_000))
        end
      end
    else
      # The whole tree, as a host application starts it: a SQLite-backed log with
      # the notifier its adapter names, which is the pairing nothing else
      # exercises together.
      describe "the log's supervision tree" do
        test "wakes a live stream on append, without waiting for the poll" do
          start_supervised!({@log, name: :sqlite_slice_log})

          {:ok, _status} = DMEx.Log.Streams.register(@log, 5_000, :all)

          {:ok, _} = @log.append(%OrderPlaced{order_id: 1})

          # The poll interval is the 1s default, so anything faster than that
          # came from `DMEx.Notifier.Beam`.
          assert_receive :wakeup, 500
        end
      end
    end

    # Nothing in DMEx reads `inserted_at` — it is observability an operator
    # queries — but a column that does not ascend with `id` makes that signal a
    # lie, and the fix is a column default rather than a query. Both engines
    # stamp it from the INSERT that draws the id, inside the write it belongs to.
    describe "inserted_at ascends with id" do
      test "across appends that really commit" do
        for n <- 1..20, do: {:ok, _} = @log.append(%OrderPlaced{order_id: n})

        {:ok, rows} = @storage.read(0, 100, @megabyte, :all, storage())

        stamps = Enum.map(rows, & &1.inserted_at)

        assert Enum.map(rows, & &1.id) == Enum.sort(Enum.map(rows, & &1.id))
        assert stamps == Enum.sort(stamps, &(DateTime.compare(&1, &2) != :gt))
      end

      if postgres? do
        # Only stageable where two writers can overlap: on SQLite the second
        # transaction cannot begin until the first commits, so there is no
        # inversion to build.
        test "even when the transactions begin in the opposite order" do
          parent = self()

          # Begins first, appends last: its transaction timestamp predates
          # everything while its id comes after. `now()` records the BEGIN and
          # would invert the pair; `clock_timestamp()` is stamped by the INSERT,
          # inside the advisory lock's region, by the statement that draws the id.
          late =
            Task.async(fn ->
              @repo.transaction(fn ->
                # Forces the transaction to actually begin, fixing its `now()`.
                SQL.query!(@repo, "SELECT 1", [])
                send(parent, :begun)

                assert_receive :go, 5_000

                {:ok, id} = @log.append(%OrderPlaced{order_id: 2})
                id
              end)
            end)

          assert_receive :begun, 5_000

          {:ok, first} = @log.append(%OrderPlaced{order_id: 1})
          send(late.pid, :go)
          {:ok, second} = Task.await(late, 10_000)

          assert second > first
          assert DateTime.after?(inserted_at(second), inserted_at(first))
        end
      end
    end

    describe "concurrent appends" do
      @tag timeout: 120_000
      test "a cursor-following reader never skips a row" do
        parent = self()

        reader = Task.async(fn -> follow(parent, 0, []) end)

        writers =
          for w <- 1..@writers do
            Task.async(fn ->
              for n <- 1..@per_writer do
                append_and_pause(%OrderPlaced{order_id: w * 1000 + n})
              end
            end)
          end

        Task.await_many(writers, 90_000)
        send(reader.pid, :stop)
        observed = Task.await(reader, 30_000)

        assert length(observed) == @total, "reader saw #{length(observed)} of #{@total} rows"

        # The three properties that together mean "no row was ever skipped":
        assert observed == Enum.sort(observed), "reader saw ids out of order"
        assert observed == Enum.uniq(observed), "reader saw an id twice"
        assert observed == Enum.to_list(1..@total), "reader's ids are not dense"
      end

      if not postgres? do
        # An append inside a caller's transaction is documented as supported, and
        # on SQLite it is the case `default_transaction_mode: :immediate` exists
        # for: a deferred transaction would start as a reader and fail to promote
        # under this much contention. On Postgres the test above already appends
        # inside one.
        test "hold inside an enclosing transaction" do
          writers =
            for w <- 1..4 do
              Task.async(fn ->
                for n <- 1..10 do
                  {:ok, _} =
                    @repo.transaction(fn ->
                      {:ok, id} = @log.append(%OrderPlaced{order_id: w * 1000 + n})
                      id
                    end)
                end
              end)
            end

          Task.await_many(writers, 60_000)

          {:ok, rows} = @storage.read(0, 1_000, @megabyte, :all, storage())

          assert Enum.map(rows, & &1.id) == Enum.to_list(1..40)
        end
      end
    end

    if postgres? do
      # The pause belongs *inside* the transaction: that gap between drawing the
      # id and committing is the window the reader can fall into, and a
      # randomised one is what makes out-of-order commits likely rather than
      # theoretical.
      defp append_and_pause(message) do
        @repo.transaction(fn ->
          {:ok, _} = @log.append(message)
          Process.sleep(:rand.uniform(3))
        end)
      end

      defp inserted_at(id) do
        {:ok, [row]} = @storage.read(id - 1, 1, @megabyte, :all, storage())

        row.inserted_at
      end

      defp truncate_all!, do: DMEx.IntegrationCase.truncate_all!()
    else
      # No window to widen — the write lock is released by the commit that draws
      # the next rowid — so the pause goes between appends, where it keeps the
      # writers overlapping without serializing them behind a sleeping one.
      defp append_and_pause(message) do
        {:ok, _} = @log.append(message)
        Process.sleep(:rand.uniform(3))
      end

      # `sqlite_sequence` is where AUTOINCREMENT keeps its high-water mark, so
      # clearing it is what makes the next test's ids start at 1 — the equivalent
      # of Postgres's RESTART IDENTITY.
      defp truncate_all! do
        for table <- ~w(dmex_message_entities dmex_messages dmex_cursors dmex_dead_messages) do
          SQL.query!(@repo, "DELETE FROM #{table}", [])
        end

        SQL.query!(@repo, "DELETE FROM sqlite_sequence WHERE name = 'dmex_messages'", [])

        :ok
      end
    end

    # Reads exactly as the log's send loop does — `id > cursor ORDER BY id
    # LIMIT n`, advancing the cursor to the last id seen and never looking back.
    # That is what makes a skipped row unrecoverable, and what this observes.
    defp follow(parent, cursor, acc) do
      {:ok, rows} = @storage.read(cursor, 100, @megabyte, :all, storage())
      ids = Enum.map(rows, & &1.id)
      acc = acc ++ ids
      cursor = List.last(ids) || cursor

      receive do
        :stop ->
          # Drain whatever committed after the writers finished.
          drain(cursor, acc)
      after
        1 -> follow(parent, cursor, acc)
      end
    end

    defp drain(cursor, acc) do
      {:ok, rows} = @storage.read(cursor, 1_000, @megabyte, :all, storage())

      case Enum.map(rows, & &1.id) do
        [] -> acc
        ids -> drain(List.last(ids), acc ++ ids)
      end
    end

    defp storage, do: @log.__dmex_log__().storage
  end
end
