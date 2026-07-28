defmodule DMEx.Test.Handlers do
  @moduledoc """
  Handlers for the consumer tests.

  Each reports what it saw to a pid taken from the process dictionary, so a test
  can assert on the exact arguments its handler received without any global
  state.

  Every one declares `producer: :test`, so `DMEx.Test.Server.producers/1` binds
  the whole file to one runtime URL. Ids are per-module because a subscription
  id keys the cursor and these run concurrently — and the message list is now
  whatever `handle_message/2` matches on, so a handler needed for two different
  sets of types is two modules.
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
    use DMEx.Consumer.Handler, producer: :test, id: :echo

    alias DMEx.Test.Handlers
    alias DMEx.Test.Messages.OrderPlaced

    @impl true
    def handle_message(%OrderPlaced{} = message, metadata) do
      send(Handlers.owner(), {:handled, message, metadata})

      :ok
    end
  end

  defmodule EchoAll do
    @moduledoc """
    `Echo` with a wider message list.

    A second module rather than an option, because the list is part of the
    declaration now: a handler subscribed to a list that does not contain what
    it matches on is unrepresentable, and the price is a module per combination.
    """
    use DMEx.Consumer.Handler, producer: :test, id: :echo_all

    alias DMEx.Test.Handlers
    alias DMEx.Test.Messages.OrderPlaced
    alias DMEx.Test.Messages.OrderShipped

    @impl true
    def handle_message(%OrderPlaced{} = message, metadata), do: report(message, metadata)
    def handle_message(%OrderShipped{} = message, metadata), do: report(message, metadata)

    defp report(message, metadata) do
      send(Handlers.owner(), {:handled, message, metadata})

      :ok
    end
  end

  defmodule Raising do
    @moduledoc "Always raises, for the failure paths."
    use DMEx.Consumer.Handler, producer: :test, id: :raising

    alias DMEx.Test.Handlers
    alias DMEx.Test.Messages.OrderPlaced

    @impl true
    def handle_message(%OrderPlaced{} = message, _metadata) do
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
    use DMEx.Consumer.Handler, producer: :test, id: :writes_then_raises

    alias DMEx.Test.Messages.OrderPlaced
    alias DMEx.Test.Repo
    alias Ecto.Adapters.SQL

    @impl true
    def handle_message(%OrderPlaced{} = message, _metadata) do
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
    use DMEx.Consumer.Handler, producer: :test, id: :checks_transaction

    alias DMEx.Test.Handlers
    alias DMEx.Test.Messages.OrderPlaced
    alias DMEx.Test.Repo

    @impl true
    def handle_message(%OrderPlaced{}, _metadata) do
      send(Handlers.owner(), {:in_transaction?, Repo.in_transaction?()})

      :ok
    end
  end

  defmodule Slow do
    @moduledoc "Sleeps before reporting, for tests that need a drain in progress."
    use DMEx.Consumer.Handler, producer: :test, id: :slow

    alias DMEx.Test.Handlers
    alias DMEx.Test.Messages.OrderPlaced

    @impl true
    def handle_message(%OrderPlaced{} = message, _metadata) do
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
    use DMEx.Consumer.Handler, producer: :test, id: :fail_times

    alias DMEx.Test.Handlers
    alias DMEx.Test.Messages.OrderPlaced

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
    def handle_message(%OrderPlaced{} = message, _metadata) do
      remaining = Process.get(@key, 0)

      send(Handlers.owner(), {:attempted, message})

      if remaining > 0 do
        Process.put(@key, remaining - 1)

        raise "handler exploded (#{remaining} failures left)"
      end

      :ok
    end
  end

  defmodule FailTimesMismatched do
    @moduledoc """
    `FailTimes`' identity with the wrong message list.

    Deliberately declares the same `{producer, id}` — so it addresses the very
    same cursor and dead messages — while matching on a type those rows are not.
    It is how the replay tests reach "a dead row this handler cannot decode",
    given that the message list is whatever the clauses match on.

    Never put this and `FailTimes` in one `handlers:` list; sharing an id is
    exactly what `DMEx.Consumer.init/1` refuses.
    """
    use DMEx.Consumer.Handler, producer: :test, id: :fail_times

    alias DMEx.Test.Handlers
    alias DMEx.Test.Messages.OrderShipped

    @impl true
    def handle_message(%OrderShipped{} = message, _metadata) do
      send(Handlers.owner(), {:attempted, message})

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
    use DMEx.Consumer.Handler, producer: :test, id: :slow_commit

    alias DMEx.Test.Handlers
    alias DMEx.Test.Messages.OrderPlaced

    @impl true
    def handle_message(%OrderPlaced{} = message, metadata) do
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
