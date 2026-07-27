defmodule DMEx.Case do
  @moduledoc """
  Case template for the unit suite.

  Pass `async: true` for pure tests. Pass `database: true` to check out a
  sandboxed connection; such tests are tagged `:database` and are skipped
  automatically when no Postgres is reachable.
  """

  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox

  # The branch is taken at macro-expansion time rather than inside the quoted
  # body: `alias` is lexically scoped to its enclosing block, so an `alias`
  # written inside a quoted `if` applies only within that `if` and never reaches
  # the test module.
  using opts do
    if opts[:database] do
      quote do
        import DMEx.Case

        @moduletag :database

        alias DMEx.Test.Repo
      end
    else
      quote do
        import DMEx.Case
      end
    end
  end

  setup tags do
    if tags[:database] do
      pid = Sandbox.start_owner!(DMEx.Test.Repo, shared: not tags[:async])
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
