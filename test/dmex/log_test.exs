defmodule DMEx.LogTest do
  use DMEx.Case, async: true

  alias DMEx.Notifier.Beam
  alias DMEx.Storage.Postgres
  alias DMEx.Test.Log
  alias DMEx.Test.Repo

  defmodule TdsRepo do
    @moduledoc false
    # An engine DMEx ships no adapter for. `__adapter__/0` is all inference reads.
    def __adapter__, do: Ecto.Adapters.Tds
  end

  describe "__dmex_log__/0" do
    test "resolves everything the append and stream paths need" do
      assert %{
               log: Log,
               config: %{max_batch_frequency: 100},
               storage: storage,
               notifier: notifier
             } = Log.__dmex_log__()

      # Each handle carries its own adapter; storage carries nothing about
      # notification and no second copy of the log module.
      assert %{
               adapter: Postgres,
               repo: Repo,
               lock: {classid, objid},
               prefix: nil
             } = storage

      refute Map.has_key?(storage, :log)

      assert is_integer(classid)
      assert is_integer(objid)

      # The notifier's process name is derived here and nowhere else, which is
      # why neither adapter rebuilds it from `:log` — and why `:log` is not in
      # this map at all.
      assert notifier == %{
               adapter: DMEx.Notifier.Postgres,
               repo: Repo,
               name: DMEx.Test.Log.Notifications,
               channel: "dmex_public"
             }
    end

    test "the storage adapter chooses the notifier, and a log may override it" do
      assert define!("NotifierDefault", "repo: DMEx.Test.Repo").notifier.adapter ==
               DMEx.Notifier.Postgres

      assert define!(
               "NotifierExplicit",
               "repo: DMEx.Test.Repo, notifier: DMEx.Notifier.Beam"
             ).notifier.adapter == Beam
    end

    test "the repo's engine chooses the storage adapter, and a log may override it" do
      inferred = define!("StorageInferred", "repo: DMEx.Test.SQLiteRepo")

      assert inferred.storage.adapter == DMEx.Storage.SQLite
      # And the notifier follows the adapter, so a SQLite log states neither.
      assert inferred.notifier.adapter == Beam

      assert define!(
               "StorageExplicit",
               "repo: DMEx.Test.SQLiteRepo, storage: DMEx.Storage.Postgres"
             ).storage.adapter == Postgres
    end

    test "refuses a repo whose engine DMEx ships no adapter for" do
      assert_raise ArgumentError, ~r/ships no storage adapter for Ecto.Adapters.Tds/, fn ->
        define!("StorageUnshipped", "repo: DMEx.LogTest.TdsRepo")
      end
    end

    test "is a compile-time constant rather than rebuilt per call" do
      assert :erts_debug.same(Log.__dmex_log__(), Log.__dmex_log__())
    end
  end

  describe "the schema scopes the lock and the channel" do
    test "an unset prefix is the default schema" do
      config = define!("ScopeDefault", "repo: DMEx.Test.Repo")

      assert config.storage.prefix == nil
      assert config.notifier.channel == "dmex_public"
    end

    test "an explicit prefix names the channel" do
      assert define!("ScopePrefix", ~s(repo: DMEx.Test.Repo, prefix: "orders")).notifier.channel ==
               "dmex_orders"

      # Not a DMEx identifier: the charset is the storage adapter's to police,
      # and Postgrex quotes the channel name, so anything legal as a schema is
      # legal here.
      assert define!("ScopeCased", ~s(repo: DMEx.Test.Repo, prefix: "Orders$1")).notifier.channel ==
               "dmex_Orders$1"
    end

    test "an unset prefix and an explicit \"public\" are one table, so one key" do
      implicit = define!("ScopeImplicit", "repo: DMEx.Test.Repo")
      explicit = define!("ScopeExplicit", ~s(repo: DMEx.Test.Repo, prefix: "public"))

      assert implicit.notifier.channel == explicit.notifier.channel
      assert implicit.storage.lock == explicit.storage.lock
    end

    test "rejects a schema too long for a Postgres channel name" do
      # 63-byte identifier limit less the "dmex_" prefix leaves 58.
      long = String.duplicate("a", 59)

      assert_raise ArgumentError, ~r/59 bytes; the limit is 58/, fn ->
        define!("ScopeLong", ~s(repo: DMEx.Test.Repo, prefix: "#{long}"))
      end

      at_limit = String.duplicate("a", 58)

      assert define!("ScopeAtLimit", ~s(repo: DMEx.Test.Repo, prefix: "#{at_limit}")).notifier.channel ==
               "dmex_" <> at_limit
    end

    test "rejects a prefix that is not a schema name" do
      assert_raise ArgumentError, ~r/must be a schema name as a string or nil/, fn ->
        define!("ScopeAtom", "repo: DMEx.Test.Repo, prefix: :orders")
      end
    end
  end

  describe "the advisory lock key" do
    test "is shared by two logs on one table, which is what makes it sound" do
      first = define!("LockSameA", ~s(repo: DMEx.Test.Repo, prefix: "shared")).storage.lock
      second = define!("LockSameB", ~s(repo: DMEx.Test.Repo, prefix: "shared")).storage.lock

      assert first == second
    end

    test "differs between schemas, but shares one DMEx namespace" do
      {classid_a, objid_a} = define!("LockDiffA", ~s(repo: DMEx.Test.Repo, prefix: "one")).storage.lock
      {classid_b, objid_b} = define!("LockDiffB", ~s(repo: DMEx.Test.Repo, prefix: "two")).storage.lock

      # The classid is DMEx's own namespace, keeping its lock space disjoint
      # from any other library sharing the database; the objid is the schema.
      assert classid_a == classid_b
      refute objid_a == objid_b
    end

    test "fits in the signed 32-bit ints pg_advisory_xact_lock takes" do
      {classid, objid} = Log.__dmex_log__().storage.lock

      assert classid in 0..2_147_483_647
      assert objid in 0..2_147_483_647
    end
  end

  describe "options" do
    test "requires a repo" do
      assert_raise ArgumentError, ~r/requires :repo/, fn ->
        define!("NoRepo", ~s(prefix: "orders"))
      end
    end

    test "defaults the repo to the application's `:ecto_repos` when it names one" do
      with_ecto_repos([Repo], fn ->
        assert define!("DefaultRepo", ~s(prefix: "orders")).storage.repo == Repo
      end)
    end

    test "requires a repo when `:ecto_repos` names several" do
      with_ecto_repos([Repo, DMEx.Test.IntegrationRepo], fn ->
        assert_raise ArgumentError, ~r/requires :repo/, fn ->
          define!("AmbiguousRepo", ~s(prefix: "orders"))
        end
      end)
    end

    test "carries DMEx.Config overrides through" do
      config = define!("WithConfig", "repo: DMEx.Test.Repo, max_batch_frequency: 50").config

      assert config.max_batch_frequency == 50
    end

    test "validates config overrides at compile time" do
      assert_raise ArgumentError, ~r/:max_batch_frequency must be a positive number/, fn ->
        define!("BadConfig", "repo: DMEx.Test.Repo, max_batch_frequency: 0")
      end
    end

    test "rejects an unknown option rather than ignoring it" do
      # A silently-dropped `batch_windwo:` is a setting that never takes effect
      # and never says so.
      assert_raise ArgumentError, ~r/unknown option \[:batch_windwo\]/, fn ->
        define!("Typo", "repo: DMEx.Test.Repo, batch_windwo: 500")
      end
    end

    # A duplicate key is legal in a keyword list and loses silently: the reader
    # takes the first, so the second is a setting that never takes effect.
    test "rejects a repeated option" do
      assert_raise ArgumentError, ~r/passes :max_batch_frequency more than once/, fn ->
        define!("Repeat", "repo: DMEx.Test.Repo, max_batch_frequency: 50, max_batch_frequency: 60")
      end
    end

    # The repeat is of a *known* key, so it survives `keys -- known` only if
    # that subtraction is fed the raw list. Naming it "unknown" would point at
    # the right line with the wrong complaint.
    test "names a repeated option as repeated rather than unknown" do
      error =
        assert_raise ArgumentError, fn ->
          define!("RepeatNotUnknown", "repo: DMEx.Test.Repo, repo: DMEx.Test.Repo")
        end

      assert Exception.message(error) =~ "passes :repo more than once"
      refute Exception.message(error) =~ "unknown option"
    end
  end

  ## Helpers

  # Defines a log module at runtime so compile-time raises are assertable.
  # Only this file compiles a log module without a `repo:`, so the global env is
  # safe to move for the length of one test.
  defp with_ecto_repos(repos, fun) do
    previous = Application.fetch_env(:dmex, :ecto_repos)
    Application.put_env(:dmex, :ecto_repos, repos)

    try do
      fun.()
    after
      case previous do
        {:ok, value} -> Application.put_env(:dmex, :ecto_repos, value)
        :error -> Application.delete_env(:dmex, :ecto_repos)
      end
    end
  end

  defp define!(name, opts) do
    module = Module.concat(DMEx.LogTest.Generated, name)

    {{:module, ^module, _, _}, _binding} =
      Code.eval_string("""
      defmodule #{inspect(module)} do
        use DMEx.Log, #{opts}
      end
      """)

    module.__dmex_log__()
  end
end
