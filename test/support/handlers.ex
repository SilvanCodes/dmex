defmodule DMEx.Test.Handlers do
  @moduledoc """
  Handlers for the consumer tests.

  Each reports what it saw to a pid taken from the process dictionary, so a test
  can assert on the exact arguments its handler received without any global
  state.
  """

  @key :dmex_test_handler_owner

  @doc """
  Routes handler callbacks back to `pid`.

  Recorded in two places on purpose. A handler invoked directly by a unit test
  runs *in* that test's process, where the process dictionary keeps parallel
  tests isolated from each other. A handler invoked through a live subscription
  runs in the stream's process, which has no such dictionary entry — so the
  application environment carries it there. Integration tests are serial, so
  that single global slot is safe.
  """
  @spec report_to(pid()) :: :ok
  def report_to(pid) do
    Process.put(@key, pid)
    Application.put_env(:dmex, @key, pid)

    :ok
  end

  @doc false
  @spec owner() :: pid() | nil
  def owner, do: Process.get(@key) || Application.get_env(:dmex, @key)

  defmodule Echo do
    @moduledoc "Reports every message it handles and does nothing else."
    use DMEx.Consumer.Handler

    alias DMEx.Test.Handlers

    @impl true
    def handle_message(message, metadata) do
      send(Handlers.owner(), {:handled, message, metadata})

      :ok
    end
  end

  defmodule Raising do
    @moduledoc "Always raises, for the failure paths."
    use DMEx.Consumer.Handler

    alias DMEx.Test.Handlers

    @impl true
    def handle_message(message, _metadata) do
      send(Handlers.owner(), {:attempted, message})

      raise "handler exploded"
    end
  end

  defmodule WritesThenRaises do
    @moduledoc """
    Writes to a scratch table and then raises.

    The rollback probe: if the row is gone afterwards, the callback and the
    cursor advance really did share one transaction.
    """
    use DMEx.Consumer.Handler

    alias DMEx.Test.Repo
    alias Ecto.Adapters.SQL

    @impl true
    def handle_message(message, _metadata) do
      SQL.query!(
        Repo,
        "INSERT INTO dmex_test_scratch (note) VALUES ($1)",
        [inspect(message)]
      )

      raise "handler exploded after writing"
    end
  end

  defmodule ChecksTransaction do
    @moduledoc "Reports whether it was invoked inside a transaction."
    use DMEx.Consumer.Handler

    alias DMEx.Test.Handlers
    alias DMEx.Test.Repo

    @impl true
    def handle_message(_message, _metadata) do
      send(Handlers.owner(), {:in_transaction?, Repo.in_transaction?()})

      :ok
    end
  end

  defmodule Slow do
    @moduledoc "Sleeps before reporting, for tests that need a drain in progress."
    use DMEx.Consumer.Handler

    alias DMEx.Test.Handlers

    @impl true
    def handle_message(message, _metadata) do
      Process.sleep(50)
      send(Handlers.owner(), {:handled, message})

      :ok
    end
  end

  defmodule FailTimes do
    @moduledoc """
    Raises for the first N calls, then succeeds.

    The transient failure retries exist for — a deadlock, a blipped downstream —
    as opposed to a handler that is simply broken.
    """
    use DMEx.Consumer.Handler

    alias DMEx.Test.Handlers

    @key :dmex_test_fail_times

    @doc """
    Makes the next `count` calls raise.

    Kept in the process dictionary rather than the application environment: this
    handler is invoked synchronously in the calling test's own process, so a
    per-process counter is what keeps parallel tests from consuming each other's
    failures.
    """
    def set(count) do
      Process.put(@key, count)

      :ok
    end

    @impl true
    def handle_message(message, _metadata) do
      remaining = Process.get(@key, 0)

      send(Handlers.owner(), {:attempted, message})

      if remaining > 0 do
        Process.put(@key, remaining - 1)

        raise "handler exploded (#{remaining} failures left)"
      end

      :ok
    end
  end

  defmodule SlowCommit do
    @moduledoc """
    Reports, then stalls before returning.

    Puts a wide, deliberate window between a handler's side effect landing and
    its transaction committing — which is precisely the gap DMEx's duplicate
    suppression cannot cover, and the reason delivery is at-least-once rather
    than exactly-once. Killing the stream on receipt lands squarely inside it.
    """
    use DMEx.Consumer.Handler

    alias DMEx.Test.Handlers

    @impl true
    def handle_message(message, metadata) do
      send(Handlers.owner(), {:handled, message, metadata})
      Process.sleep(300)

      :ok
    end
  end

  defmodule NotAHandler do
    @moduledoc "Deliberately missing `use DMEx.Consumer.Handler`."
    def handle_message(_message, _metadata), do: :ok
  end
end
