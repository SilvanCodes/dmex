defmodule DMEx.Integration.SQLiteAppendOrderingTest do
  @moduledoc """
  The same failure `DMEx.Integration.AppendOrderingTest` guards against, asserted
  against the engine that prevents it structurally.

  Postgres needs an advisory lock because it allocates a sequence value at INSERT
  and makes the row visible at COMMIT, so a reader scanning `id > cursor` can
  advance past a lower id that has not committed yet and skip it permanently.
  SQLite has one write transaction database-wide: a second writer cannot draw a
  rowid until the first has committed, so assignment order *is* commit order.

  That makes the *mechanism* tests meaningless here — there is no lock to hold or
  release, and the inversion cannot be staged — so what is asserted is the
  outcome: under real concurrency, a cursor-following reader sees every row, in
  order, exactly once.

  Not sandboxed, and deliberately: a sandbox would put every append inside one
  transaction, which is precisely the property under test.
  """

  use ExUnit.Case, async: false

  alias DMEx.Log.Streams
  alias DMEx.Storage.SQLite
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.SQLiteIntegrationRepo, as: Repo
  alias DMEx.Test.SQLiteLog, as: Log
  alias Ecto.Adapters.SQL

  @moduletag :integration
  @moduletag :sqlite

  @writers 8
  @per_writer 25
  @total @writers * @per_writer

  setup do
    truncate_all!()
    on_exit(&truncate_all!/0)
    :ok
  end

  # The whole tree, as a host application starts it: a SQLite-backed log with
  # the notifier its adapter names, which is the pairing nothing else exercises
  # together.
  describe "the log's supervision tree" do
    test "wakes a live stream on append, without waiting for the poll" do
      start_supervised!({Log, name: :sqlite_slice_log})

      {:ok, _status} = Streams.register(Log, 5_000, :all)

      {:ok, _} = Log.append(%OrderPlaced{order_id: 1})

      # The poll interval is the 1s default, so anything faster than that came
      # from `DMEx.Notifier.Beam`.
      assert_receive :wakeup, 500
    end
  end

  describe "inserted_at ascends with id" do
    # `inserted_at` is observability, but a stamp that fell behind a later id
    # would make it a lie. The column default is evaluated by the INSERT that
    # draws the rowid, inside the write transaction — SQLite has no
    # transaction-start clock to reach for by mistake.
    test "across appends that really commit" do
      for n <- 1..20, do: {:ok, _} = Log.append(%OrderPlaced{order_id: n})

      {:ok, rows} = SQLite.read(0, 100, 1_048_576, :all, storage_opts())

      stamps = Enum.map(rows, & &1.inserted_at)

      assert Enum.map(rows, & &1.id) == Enum.sort(Enum.map(rows, & &1.id))
      assert stamps == Enum.sort(stamps, &(DateTime.compare(&1, &2) != :gt))
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
              {:ok, _} = Log.append(%OrderPlaced{order_id: w * 1000 + n})

              Process.sleep(:rand.uniform(3))
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

    # An append inside a caller's transaction is documented as supported, and on
    # SQLite it is the case `default_transaction_mode: :immediate` exists for: a
    # deferred transaction would start as a reader and fail to promote under this
    # much contention.
    test "hold inside an enclosing transaction" do
      writers =
        for w <- 1..4 do
          Task.async(fn ->
            for n <- 1..10 do
              {:ok, _} =
                Repo.transaction(fn ->
                  {:ok, id} = Log.append(%OrderPlaced{order_id: w * 1000 + n})
                  id
                end)
            end
          end)
        end

      Task.await_many(writers, 60_000)

      {:ok, rows} = SQLite.read(0, 1_000, 1_048_576, :all, storage_opts())

      assert Enum.map(rows, & &1.id) == Enum.to_list(1..40)
    end
  end

  # Reads exactly as the log's send loop does — `id > cursor ORDER BY id LIMIT n`,
  # advancing to the last id seen and never looking back. That is what makes a
  # skipped row unrecoverable, and what this observes.
  defp follow(parent, cursor, acc) do
    {:ok, rows} = SQLite.read(cursor, 100, 1_048_576, :all, storage_opts())
    ids = Enum.map(rows, & &1.id)
    acc = acc ++ ids
    cursor = List.last(ids) || cursor

    receive do
      :stop -> drain(cursor, acc)
    after
      1 -> follow(parent, cursor, acc)
    end
  end

  defp drain(cursor, acc) do
    {:ok, rows} = SQLite.read(cursor, 1_000, 1_048_576, :all, storage_opts())

    case Enum.map(rows, & &1.id) do
      [] -> acc
      ids -> drain(List.last(ids), acc ++ ids)
    end
  end

  defp storage_opts, do: Log.__dmex__().storage_opts

  # `sqlite_sequence` is where AUTOINCREMENT keeps its high-water mark, so
  # clearing it is what makes the next test's ids start at 1 — the equivalent of
  # Postgres's RESTART IDENTITY.
  defp truncate_all! do
    for table <- ~w(dmex_messages dmex_cursors dmex_dead_messages) do
      SQL.query!(Repo, "DELETE FROM #{table}", [])
    end

    SQL.query!(Repo, "DELETE FROM sqlite_sequence WHERE name = 'dmex_messages'", [])

    :ok
  end
end
