defmodule DMEx.LogTest do
  use DMEx.Case, async: true

  alias DMEx.Test.Log

  describe "__dmex__/0" do
    test "resolves everything the append and stream paths need" do
      assert %{
               log: Log,
               repo: DMEx.Test.Repo,
               storage: DMEx.Storage.Postgres,
               app_key: "dm_ex_test_log",
               channel: "dmex_dm_ex_test_log",
               lock: {classid, objid},
               prefix: nil,
               config: %{max_batch_frequency: 100},
               storage_opts: storage_opts
             } = Log.__dmex__()

      assert is_integer(classid)
      assert is_integer(objid)

      # The adapter is handed exactly this map and nothing else, so the two
      # cannot drift.
      assert storage_opts == %{
               repo: DMEx.Test.Repo,
               log: Log,
               lock: {classid, objid},
               channel: "dmex_dm_ex_test_log",
               prefix: nil
             }
    end

    test "is a compile-time constant rather than rebuilt per call" do
      assert :erts_debug.same(Log.__dmex__(), Log.__dmex__())
    end
  end

  describe "app key derivation" do
    test "defaults to the module name, underscored and flattened" do
      # Every segment is underscored and the dots become underscores too, so a
      # deeply-nested log still yields one legal identifier.
      assert define!("AppKeyDefault", "repo: DMEx.Test.Repo").app_key ==
               "dm_ex_log_test_generated_app_key_default"
    end

    test "an explicit atom or string wins" do
      assert define!("AppKeyAtom", "repo: DMEx.Test.Repo, app: :orders").app_key == "orders"
      assert define!("AppKeyString", ~s(repo: DMEx.Test.Repo, app: "orders")).app_key == "orders"
    end

    test "names the notification channel" do
      assert define!("AppKeyChannel", "repo: DMEx.Test.Repo, app: :orders").channel ==
               "dmex_orders"
    end

    test "rejects a key that is not a legal identifier" do
      for app <- [~s(:"Orders"), ~s(:"orders-app"), ~s(:"orders app"), ":_orders", ~s(:"9orders")] do
        assert_raise ArgumentError, ~r/is not a legal identifier/, fn ->
          define!(
            "AppKeyBad#{:erlang.unique_integer([:positive])}",
            "repo: DMEx.Test.Repo, app: #{app}"
          )
        end
      end
    end

    test "rejects a key too long for a Postgres channel name" do
      # 63-byte identifier limit less the "dmex_" prefix leaves 58.
      long = String.duplicate("a", 59)

      assert_raise ArgumentError, ~r/59 bytes; the limit is 58/, fn ->
        define!("AppKeyLong", ~s(repo: DMEx.Test.Repo, app: "#{long}"))
      end

      assert define!(
               "AppKeyAtLimit",
               ~s(repo: DMEx.Test.Repo, app: "#{String.duplicate("a", 58)}")
             ).app_key
             |> byte_size() == 58
    end
  end

  describe "the advisory lock key" do
    test "is stable for a given app key" do
      first = define!("LockStableA", "repo: DMEx.Test.Repo, app: :same_key").lock
      second = define!("LockStableB", "repo: DMEx.Test.Repo, app: :same_key").lock

      assert first == second
    end

    test "differs between app keys, but shares one DMEx namespace" do
      {classid_a, objid_a} = define!("LockDiffA", "repo: DMEx.Test.Repo, app: :one").lock
      {classid_b, objid_b} = define!("LockDiffB", "repo: DMEx.Test.Repo, app: :two").lock

      # The classid is DMEx's own namespace, keeping its lock space disjoint
      # from any other library sharing the database; the objid is the app.
      assert classid_a == classid_b
      refute objid_a == objid_b
    end

    test "fits in the signed 32-bit ints pg_advisory_xact_lock takes" do
      {classid, objid} = Log.__dmex__().lock

      assert classid in 0..2_147_483_647
      assert objid in 0..2_147_483_647
    end
  end

  describe "options" do
    test "requires a repo" do
      assert_raise ArgumentError, ~r/requires a `:repo`/, fn ->
        define!("NoRepo", "app: :thing")
      end
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

    test "carries the columns to lift into metadata through" do
      config = define!("WithLift", "repo: DMEx.Test.Repo, metadata: [:id, :inserted_at]").config

      assert config.metadata == [:id, :inserted_at]
    end

    test "validates the columns to lift at compile time" do
      assert_raise ArgumentError, ~r/:metadata may only lift/, fn ->
        define!("BadLift", "repo: DMEx.Test.Repo, metadata: [:data]")
      end
    end

    test "rejects an unknown option rather than ignoring it" do
      # A silently-dropped `batch_windwo:` is a setting that never takes effect
      # and never says so.
      assert_raise ArgumentError, ~r/unknown option \[:batch_windwo\]/, fn ->
        define!("Typo", "repo: DMEx.Test.Repo, batch_windwo: 500")
      end
    end
  end

  ## Helpers

  # Defines a log module at runtime so compile-time raises are assertable.
  defp define!(name, opts) do
    module = Module.concat(DMEx.LogTest.Generated, name)

    {{:module, ^module, _, _}, _binding} =
      Code.eval_string("""
      defmodule #{inspect(module)} do
        use DMEx.Log, #{opts}
      end
      """)

    module.__dmex__()
  end
end
