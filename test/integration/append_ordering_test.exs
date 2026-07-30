defmodule DMEx.Integration.AppendOrderingTest do
  @moduledoc """
  The most dangerous failure mode in the design, and the only test that can see it.

  Postgres allocates sequence values at INSERT time but makes rows visible at
  COMMIT time. If T1 takes `id=100` and T2 takes `id=101` but T2 commits first,
  a reader scanning `id > 99 ORDER BY id` sees 101, advances its cursor past it,
  and **permanently skips message 100** once T1 finally commits.

  Nothing downstream detects that: `last_seen` advanced right over the gap, and
  every later read is correct. It is silent data loss, which is why appends are
  serialized with a transaction-scoped advisory lock and why this test exists.
  """

  use DMEx.IntegrationCase

  alias DMEx.Storage.Postgres
  alias DMEx.Test.IntegrationProducer, as: Producer
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Producer, as: UnitProducer
  alias Ecto.Adapters.SQL

  @writers 8
  @per_writer 25
  @total @writers * @per_writer

  describe "the lock itself" do
    test "publish takes a transaction-scoped advisory lock on the producer's key" do
      {classid, objid} = Producer.__dmex__().lock

      Repo.transaction(fn ->
        {:ok, _id} = Producer.publish(%OrderPlaced{order_id: 1})

        # Still held *inside* the transaction. A lock released at the end of the
        # INSERT would let a later-allocated id commit first, which is the exact
        # failure it exists to prevent.
        assert [[1]] =
                 SQL.query!(
                   Repo,
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
      {classid, objid} = Producer.__dmex__().lock

      {:ok, _} = Producer.publish(%OrderPlaced{order_id: 1})

      assert [[0]] =
               SQL.query!(
                 Repo,
                 "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND classid = $1 AND objid = $2",
                 [classid, objid]
               ).rows
    end

    test "two producers with different app keys take different locks" do
      # Co-tenants in one database must not serialize against each other.
      {_classid, objid} = Producer.__dmex__().lock
      {_classid, other_objid} = UnitProducer.__dmex__().lock

      refute objid == other_objid
    end
  end

  describe "concurrent publishes" do
    @tag timeout: 120_000
    test "a cursor-following reader never skips a row" do
      parent = self()

      reader =
        Task.async(fn ->
          follow(parent, 0, [])
        end)

      writers =
        for w <- 1..@writers do
          Task.async(fn ->
            for n <- 1..@per_writer do
              # A randomised gap between allocating the id and committing is
              # what makes out-of-order commits likely rather than theoretical.
              Repo.transaction(fn ->
                {:ok, _} = Producer.publish(%OrderPlaced{order_id: w * 1000 + n})
                Process.sleep(:rand.uniform(3))
              end)
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
  end

  # Reads exactly as the producer's send loop does — `id > cursor ORDER BY id
  # LIMIT n`, advancing the cursor to the last id seen and never looking back.
  # That is what makes a skipped row unrecoverable, and what this observes.
  defp follow(parent, cursor, acc) do
    {:ok, rows} = Postgres.read(storage_opts(), cursor, 100, :all)
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
    {:ok, rows} = Postgres.read(storage_opts(), cursor, 1_000, :all)

    case Enum.map(rows, & &1.id) do
      [] -> acc
      ids -> drain(List.last(ids), acc ++ ids)
    end
  end

  defp storage_opts, do: Producer.__dmex__().storage_opts
end
