defmodule DMEx.Case do
  @moduledoc """
  Case template for the unit suite.

  Pass `async: true` for pure tests. Pass `database: true` to check out a
  sandboxed connection; such tests are tagged `:database` and are skipped
  automatically when no Postgres is reachable.

  Pass `repo: DMEx.Test.SQLiteRepo` alongside it to run against SQLite instead.
  Those tests are tagged `:sqlite` rather than `:database` — SQLite needs no
  server, so they run wherever the driver compiled — and they cannot be `async:
  true`, which ecto_sqlite3's sandbox does not support.
  """

  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox

  # The branch is taken at macro-expansion time rather than inside the quoted
  # body: `alias` is lexically scoped to its enclosing block, so an `alias`
  # written inside a quoted `if` applies only within that `if` and never reaches
  # the test module.
  using opts do
    if opts[:database] do
      repo = opts[:repo] || DMEx.Test.Repo
      tag = if repo == DMEx.Test.SQLiteRepo, do: :sqlite, else: :database

      quote do
        import DMEx.Case

        @moduletag unquote(tag)

        # `setup/1` is handed tags and nothing else, so the repo travels as one.
        @moduletag repo: unquote(repo)

        alias unquote(repo), as: Repo
      end
    else
      quote do
        import DMEx.Case
      end
    end
  end

  setup tags do
    if repo = tags[:repo] do
      pid = Sandbox.start_owner!(repo, shared: not tags[:async])
      on_exit(fn -> Sandbox.stop_owner(pid) end)
    end

    :ok
  end

  @doc """
  Waits until `fun` returns a truthy value, or fails after `timeout` ms.

  Preferred over `Process.sleep/1` anywhere a test waits on asynchronous work —
  a fixed sleep is either slower than it needs to be or flaky, and usually both.
  """
  @spec eventually((-> any()), pos_integer(), pos_integer()) :: any()
  def eventually(fun, timeout \\ 2_000, interval \\ 20) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline, interval)
  end

  defp do_eventually(fun, deadline, interval) do
    case fun.() do
      falsy when falsy in [nil, false] ->
        if System.monotonic_time(:millisecond) >= deadline do
          raise ExUnit.AssertionError, message: "condition did not become true within the timeout"
        end

        Process.sleep(interval)
        do_eventually(fun, deadline, interval)

      truthy ->
        truthy
    end
  end
end
