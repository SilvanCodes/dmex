defmodule DMEx.DefaultRepoTest do
  # Deliberately not `async: true`: every test here moves `config :dmex, :repo`,
  # which is global and which `DMEx.Storage.opts/1` now reads. ExUnit runs sync
  # modules only once every async one has finished, which is what keeps the move
  # invisible to the tests that assert a missing repo raises.
  use DMEx.Case, database: true

  alias DMEx.Query
  alias DMEx.Storage
  alias DMEx.Storage.Postgres
  alias DMEx.Test.Log
  alias DMEx.Test.Messages.OrderPlaced

  # Not an Ecto repo, so a door that reads it when an explicit `repo:` was given
  # fails loudly in `DMEx.Storage.default_adapter/1` instead of passing by luck.
  defmodule NotARepo do
    @moduledoc false
  end

  describe "default_repo/0" do
    test "is nil when the application names none" do
      refute Storage.default_repo()
    end

    test "is what `config :dmex, repo:` names" do
      with_default_repo(Repo, fn -> assert Storage.default_repo() == Repo end)
    end
  end

  describe "the operator-facing doors" do
    test "resolve storage from the configured repo, adapter and all" do
      with_default_repo(Repo, fn ->
        assert %{repo: Repo, adapter: Postgres, prefix: nil} = DMEx.Supervisor.storage!([])
      end)
    end

    test "read a log with no `repo:` on the call" do
      {:ok, _} = Log.append(%OrderPlaced{order_id: 7})

      with_default_repo(Repo, fn ->
        assert {:ok, [%OrderPlaced{order_id: 7}]} = Query.all(OrderPlaced)
      end)
    end

    # The specific answer beats the application-wide one, and the configured
    # value is not even looked at — `NotARepo` has no `__adapter__/0`.
    test "prefer an explicit `repo:` over the configured one" do
      with_default_repo(NotARepo, fn ->
        assert %{repo: Repo, adapter: Postgres} = DMEx.Supervisor.storage!(repo: Repo)
      end)
    end

    test "name both fixes when neither the call nor the configuration has a repo" do
      assert_raise ArgumentError, ~r/`repo:` is required.*config :dmex, repo:/s, fn ->
        Query.all(OrderPlaced)
      end
    end
  end

  describe "use DMEx.Log" do
    test "defaults its repo to the configured one" do
      with_default_repo(Repo, fn ->
        assert define!("ConfiguredRepo", ~s(prefix: "orders")).storage.repo == Repo
      end)
    end

    test "names both fixes when the configuration has no repo either" do
      assert_raise ArgumentError, ~r/requires :repo.*config :dmex, repo:/s, fn ->
        define!("NoConfiguredRepo", ~s(prefix: "orders"))
      end
    end
  end

  ## Helpers

  defp with_default_repo(repo, fun) do
    previous = Application.fetch_env(:dmex, :repo)
    Application.put_env(:dmex, :repo, repo)

    try do
      fun.()
    after
      case previous do
        {:ok, value} -> Application.put_env(:dmex, :repo, value)
        :error -> Application.delete_env(:dmex, :repo)
      end
    end
  end

  # Defines a log module at runtime so a compile-time resolution is assertable.
  defp define!(name, opts) do
    module = Module.concat(DMEx.DefaultRepoTest.Generated, name)

    {{:module, ^module, _, _}, _binding} =
      Code.eval_string("""
      defmodule #{inspect(module)} do
        use DMEx.Log, #{opts}
      end
      """)

    module.__dmex_log__()
  end
end
