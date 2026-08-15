defmodule DMEx.Test.Handlers do
  @moduledoc """
  Handlers for the consumer tests.

  Each reports what it saw to a pid taken from the process dictionary, so a test
  can assert on the exact arguments its handler received without any global
  state.

  `Fanned` is the only one on two logs (`:test` and `:other`); the rest are on
  `:test` alone, which `DMEx.Test.Server.bindings/1` binds. Ids are per-module: the
  id keys the cursor and these run concurrently.
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
    @moduledoc """
    Reports every message it handles and does nothing else. `last_seen: 0`
    because tests subscribe and *then* append, which the unset default races.
    """
    use DMEx.Handler, logs: [:test], name: :echo, last_seen: 0

    alias DMEx.Test.Handlers
    alias DMEx.Test.Messages.OrderPlaced

    @impl true
    def handle_message(%OrderPlaced{} = message, metadata) do
      send(Handlers.owner(), {:handled, message, metadata})

      :ok
    end
  end

  defmodule EchoWholeLog do
    @moduledoc """
    `Echo` with server-side selection off: a catch-all clause names no type, so
    no fingerprint is built and the whole log is read. Both clauses report, so a
    test can tell which one a row reached.
    """
    use DMEx.Handler, logs: [:test], name: :echo_whole_log

    alias DMEx.Test.Handlers
    alias DMEx.Test.Messages.OrderPlaced

    @impl true
    def handle_message(%OrderPlaced{} = message, metadata) do
      send(Handlers.owner(), {:handled, message, metadata})

      :ok
    end

    def handle_message(%module{} = message, metadata) do
      send(Handlers.owner(), {:caught, module, message, metadata})

      :ok
    end
  end

  defmodule Sink do
    @moduledoc """
    Names no type at all: reads the whole log and reports whatever this
    application has a module for, skipping the rest.
    """
    use DMEx.Handler, logs: [:test], name: :sink

    alias DMEx.Test.Handlers

    @impl true
    def handle_message(%module{} = message, metadata) do
      send(Handlers.owner(), {:caught, module, message, metadata})

      :ok
    end
  end

  defmodule WholeLogNarrowed do
    @moduledoc """
    Asks for the whole log and still turns messages down: its second clause
    names no type, so there is no fingerprint, but narrows on the metadata.
    """
    use DMEx.Handler, logs: [:test], name: :whole_log_narrowed

    alias DMEx.Test.Handlers
    alias DMEx.Test.Messages.OrderPlaced

    @impl true
    def handle_message(%OrderPlaced{} = message, metadata) do
      send(Handlers.owner(), {:handled, message, metadata})

      :ok
    end

    def handle_message(%module{} = message, %{"replay" => true} = metadata) do
      send(Handlers.owner(), {:caught, module, message, metadata})

      :ok
    end
  end

  defmodule EchoAll do
    @moduledoc "`Echo` with a wider message set: two message types rather than one."
    use DMEx.Handler, logs: [:test], name: :echo_all

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

  defmodule Narrowed do
    @moduledoc """
    Matches a field's value, not just the type: the `OrderPlaced`s this clause
    turns down are skipped rather than raising.
    """
    use DMEx.Handler, logs: [:test], name: :narrowed

    alias DMEx.Test.Handlers
    alias DMEx.Test.Messages.OrderPlaced

    @impl true
    def handle_message(%OrderPlaced{customer_id: 7} = message, metadata) do
      send(Handlers.owner(), {:handled, message, metadata})

      :ok
    end
  end

  defmodule Guarded do
    @moduledoc "Narrows with a guard rather than with a pattern."
    use DMEx.Handler, logs: [:test], name: :guarded

    alias DMEx.Test.Handlers
    alias DMEx.Test.Messages.OrderPlaced

    @impl true
    def handle_message(%OrderPlaced{total: total} = message, metadata) when total > 100 do
      send(Handlers.owner(), {:handled, message, metadata})

      :ok
    end
  end

  defmodule RaisesFunctionClause do
    @moduledoc """
    Raises a `FunctionClauseError` from inside its body: the skip path has to
    tell a head that never matched from one that was entered and failed.
    """
    use DMEx.Handler, logs: [:test], name: :raises_function_clause

    alias DMEx.Test.Messages.OrderPlaced

    @impl true
    def handle_message(%OrderPlaced{} = message, _metadata), do: classify(message.total)

    defp classify(total) when is_integer(total), do: :ok
  end

  defmodule Fanned do
    @moduledoc """
    One handler, two logs (`:test` and `:other`), with its own cursor and its
    own connection per subscription and nothing shared but the module.
    """
    use DMEx.Handler, logs: [:test, :other], name: :fanned, last_seen: 0

    alias DMEx.Test.Handlers
    alias DMEx.Test.Messages.OrderPlaced

    @impl true
    def handle_message(%OrderPlaced{} = message, metadata) do
      send(Handlers.owner(), {:handled, message, metadata})

      :ok
    end
  end

  defmodule Careful do
    @moduledoc "Declares its own failure policy, for the config-on-the-module path."
    use DMEx.Handler, logs: [:test], name: :careful, on_failure: :crash, max_attempts: 5

    alias DMEx.Test.Handlers
    alias DMEx.Test.Messages.OrderPlaced

    @impl true
    def handle_message(%OrderPlaced{} = message, metadata) do
      send(Handlers.owner(), {:handled, message, metadata})

      :ok
    end
  end

  defmodule Raising do
    @moduledoc "Always raises, for the failure paths."
    use DMEx.Handler, logs: [:test], name: :raising

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
    Writes to a scratch table and then raises: if the row is gone afterwards,
    the callback and the cursor advance really did share one transaction.
    """
    use DMEx.Handler, logs: [:test], name: :writes_then_raises

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
    use DMEx.Handler, logs: [:test], name: :checks_transaction

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
    use DMEx.Handler, logs: [:test], name: :slow

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
    Raises for the first N calls, then succeeds: the transient failure retries
    exist for, as opposed to a handler that is simply broken.
    """
    use DMEx.Handler, logs: [:test], name: :fail_times

    alias DMEx.Test.Handlers
    alias DMEx.Test.Messages.OrderPlaced

    @key :dmex_test_fail_times

    @doc """
    Makes the next `count` calls raise.

    Per-process rather than in the application environment: this handler runs in
    the calling test's own process, so parallel tests cannot consume each
    other's failures.
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

  defmodule SlowCommit do
    @moduledoc """
    Reports, then stalls before returning: a wide window between a handler's
    side effect landing and its transaction committing, which is the gap
    duplicate suppression cannot cover. Killing the stream on receipt lands
    squarely inside it.
    """
    use DMEx.Handler, logs: [:test], name: :slow_commit

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
    @moduledoc "Deliberately missing `use DMEx.Handler`."
    def handle_message(_message, _metadata), do: :ok
  end
end
