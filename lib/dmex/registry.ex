defmodule DMEx.Registry do
  @moduledoc """
  Where a producer's live stream processes register themselves for wakeups.

  One `Registry` per producer, holding one duplicate-key entry per open SSE
  connection. `DMEx.Dispatcher` broadcasts a bare `:wakeup` across it; nothing
  else is ever sent, and nothing is ever stored against an entry.

  That emptiness is the point. A wakeup is a hint that work may exist, never a
  carrier of work — each stream reads from its own cursor to whatever is there
  now, so anything in the payload would be information nothing consumes. It also
  means this registry holds no message data on any connection's behalf, which is
  what keeps the one shared per-app process out of the memory story entirely.

  Entries are registered by the **request process itself**, because on the
  producer side the request *is* the stream. When the connection dies, the
  process dies, and the registry entry goes with it — there is nothing to clean
  up and no way for a stale entry to survive its connection.
  """

  alias DMEx.Producer.Pending

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
  Registers the calling process to receive this producer's wakeups.

  Called by the request process serving an SSE connection, before its first
  read — so a publish landing between registration and that read is picked up by
  the read itself rather than being missed.

  Returns the connection's wakeup flag (see `DMEx.Producer.Pending`), which is
  stored as the entry's value. Keeping it there rather than in a table of its
  own is what makes cleanup free: the entry dies with this process, and the flag
  with it.
  """
  @spec register(module()) :: {:ok, Pending.t()} | {:error, term()}
  def register(producer) when is_atom(producer) do
    pending = Pending.new()

    case Registry.register(name(producer), @key, pending) do
      {:ok, _owner} -> {:ok, pending}
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
    each(producer, fn pid, _pending -> send(pid, message) end)
  end

  @doc """
  Runs `fun` for every registered stream, with its pid and its wakeup flag.

  The dispatcher uses this rather than `broadcast/2` so it can decide per
  connection whether a wakeup is worth sending — see `DMEx.Producer.Pending`.
  `fun` runs in the calling process and must not block.
  """
  @spec each(module(), (pid(), Pending.t() -> any())) :: :ok
  def each(producer, fun) when is_atom(producer) and is_function(fun, 2) do
    Registry.dispatch(name(producer), @key, fn entries ->
      Enum.each(entries, fn {pid, pending} -> fun.(pid, pending) end)
    end)
  end

  @doc "How many streams are currently registered. For telemetry and tests."
  @spec count(module()) :: non_neg_integer()
  def count(producer) when is_atom(producer) do
    Registry.count_match(name(producer), @key, :_)
  end
end
