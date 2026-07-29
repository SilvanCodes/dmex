defmodule DMEx.Producer.Registry do
  @moduledoc """
  Where a producer's live stream processes register themselves for wakeups.

  One Elixir `Registry` per producer, holding one duplicate-key entry per open
  SSE connection. `DMEx.Producer.Dispatcher` broadcasts a bare `:wakeup` across
  it; nothing else is ever sent. (Unqualified `Registry` in this module's body
  is the standard library's, not this one — nested modules are not
  auto-aliased.)

  That emptiness is the point. A wakeup is a hint that work may exist, never a
  carrier of work — each stream reads from its own cursor to whatever is there
  now, so anything in the payload would be information nothing consumes. It also
  means this registry holds no message data on any connection's behalf, which is
  what keeps the one shared per-app process out of the memory story entirely.

  What an entry *does* carry is the connection's `DMEx.Producer.Status`: its
  wakeup flag and its deadline, two machine words the dispatcher and
  `DMEx.Producer.Watcher` read without ever messaging the stream process — which
  matters most exactly when that process is wedged and would not answer.

  Entries are registered by the **request process itself**, because on the
  producer side the request *is* the stream. When the connection dies, the
  process dies, and the registry entry goes with it — taking the status with it,
  so there is nothing to clean up and no way for a stale entry or a stale
  deadline to survive its connection.
  """

  alias DMEx.Producer.Status

  @key :streams

  @doc "The registry name for a producer module."
  @spec name(module()) :: atom()
  def name(producer) when is_atom(producer), do: Module.concat(producer, "Registry")

  @doc "The child spec a producer's supervisor uses to start this registry."
  @spec child_spec(module()) :: Supervisor.child_spec()
  def child_spec(producer) when is_atom(producer) do
    Supervisor.child_spec(
      {Registry, keys: :duplicate, name: name(producer)},
      id: name(producer)
    )
  end

  @doc """
  Registers the calling process as one of this producer's streams.

  Called by the request process serving an SSE connection, before its first
  read — so a publish landing between registration and that read is picked up by
  the read itself rather than being missed — and before its first *write*, so
  the watcher covers the connect path too.

  Returns the connection's status (see `DMEx.Producer.Status`), armed for
  `budget` ms and stored as the entry's value. Keeping it there rather than in a
  table of its own is what makes cleanup free: the entry dies with this process,
  and the status with it.
  """
  @spec register(module(), Status.budget()) :: {:ok, Status.t()} | {:error, term()}
  def register(producer, budget) when is_atom(producer) do
    status = Status.new(budget)

    case Registry.register(name(producer), @key, status) do
      {:ok, _owner} -> {:ok, status}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Sends `message` to every registered stream process.

  Uses `Registry.dispatch/3`, which runs in the calling process, so a slow or
  dead stream cannot block the dispatcher — `send/2` never blocks.
  """
  @spec broadcast(module(), term()) :: :ok
  def broadcast(producer, message) when is_atom(producer) do
    each(producer, fn pid, _status -> send(pid, message) end)
  end

  @doc """
  Runs `fun` for every registered stream, with its pid and its status.

  The dispatcher uses this rather than `broadcast/2` so it can decide per
  connection whether a wakeup is worth sending, and `DMEx.Producer.Watcher` uses
  it to read every connection's deadline — see `DMEx.Producer.Status`. `fun` runs
  in the calling process and must not block.
  """
  @spec each(module(), (pid(), Status.t() -> any())) :: :ok
  def each(producer, fun) when is_atom(producer) and is_function(fun, 2) do
    Registry.dispatch(name(producer), @key, fn entries ->
      Enum.each(entries, fn {pid, status} -> fun.(pid, status) end)
    end)
  end

  @doc "How many streams are currently registered. For telemetry and tests."
  @spec count(module()) :: non_neg_integer()
  def count(producer) when is_atom(producer) do
    Registry.count_match(name(producer), @key, :_)
  end
end
