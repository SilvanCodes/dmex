defmodule DMEx.Log.Registry do
  @moduledoc """
  Where a log's live stream processes register themselves for wakeups.

  One Elixir `Registry` per log, holding one duplicate-key entry per open
  connection. `DMEx.Log.Dispatcher` broadcasts a bare `:wakeup` across it;
  nothing else is ever sent. (Unqualified `Registry` in this module's body is
  the standard library's — nested modules are not auto-aliased.)

  That emptiness is the point: a wakeup is a hint that work may exist, never a
  carrier of work, so this registry holds no message data on any connection's
  behalf. What an entry *does* carry is the connection's `DMEx.Log.Status` — two
  machine words the dispatcher and `DMEx.Log.Watcher` read without messaging the
  stream process, which matters most when it is wedged and would not answer —
  and its **selection**, the message types it asked for.

  **The selection lives in the entry rather than in the key.** The obvious way
  to route wakeups by type is to key on `message_type` and register a connection
  once per type; this does the opposite. The set is already computed, since
  `DMEx.Log.Plug` expands the filter at connect to build the read predicate,
  while N entries per connection would multiply registration, cleanup and every
  `each/2` pass by the size of the selection. And the check it enables is one
  `MapSet.member?/2` against a `Registry.dispatch/3` that was already `O(C)`, so
  routing *removes* work rather than adding an iteration.

  Entries are registered by the **request process itself**, because on the log
  side the request *is* the stream. When the connection dies the process dies
  and the entry goes with it, so there is nothing to clean up and no way for a
  stale deadline to survive its connection.
  """

  alias DMEx.Log.Status

  @key :streams

  @typedoc """
  Which message types a connection asked for.

  `:all` is a whole-log subscription; a `MapSet` is a selective one, already
  expanded from its filter against the log's own message types.
  """
  @type selection :: :all | MapSet.t(String.t())

  @doc "The registry name for a log module."
  @spec name(module()) :: atom()
  def name(log) when is_atom(log), do: Module.concat(log, "Registry")

  @doc "The child spec a log's supervisor uses to start this registry."
  @spec child_spec(module()) :: Supervisor.child_spec()
  def child_spec(log) when is_atom(log) do
    Supervisor.child_spec(
      {Registry, keys: :duplicate, name: name(log)},
      id: name(log)
    )
  end

  @doc """
  Registers the calling process as one of this log's streams.

  Called by the request process before its first read — so an append landing in
  between is picked up by that read rather than missed — and before its first
  *write*, so the watcher covers the connect path too.

  Returns the connection's `DMEx.Log.Status`, armed for `budget` ms and stored
  in the entry alongside `selection`. Keeping both there rather than in a table
  of their own is what makes cleanup free.
  """
  @spec register(module(), Status.budget(), selection()) ::
          {:ok, Status.t()} | {:error, term()}
  def register(log, budget, selection \\ :all) when is_atom(log) do
    status = Status.new(budget)

    case Registry.register(name(log), @key, {status, selection}) do
      {:ok, _owner} -> {:ok, status}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Runs `fun` for every registered stream, with its pid, status and selection.

  There is deliberately no blanket `broadcast/2`: the dispatcher decides per
  connection whether a wakeup is worth sending, and `DMEx.Log.Watcher` uses this
  to read every connection's deadline. `fun` runs in the calling process
  (`Registry.dispatch/3` guarantees that), so a slow or dead stream cannot block
  the caller — and it must not block.
  """
  @spec each(module(), (pid(), Status.t(), selection() -> any())) :: :ok
  def each(log, fun) when is_atom(log) and is_function(fun, 3) do
    Registry.dispatch(name(log), @key, fn entries ->
      Enum.each(entries, fn {pid, {status, selection}} -> fun.(pid, status, selection) end)
    end)
  end

  @doc "How many streams are currently registered. For telemetry and tests."
  @spec count(module()) :: non_neg_integer()
  def count(log) when is_atom(log) do
    Registry.count_match(name(log), @key, :_)
  end
end
