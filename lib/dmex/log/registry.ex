defmodule DMEx.Log.Registry do
  @moduledoc """
  Where a log's live stream processes register themselves for wakeups.

  One Elixir `Registry` per log, holding one duplicate-key entry per open
  connection. `DMEx.Log.Dispatcher` broadcasts a bare `:wakeup` across it;
  nothing else is ever sent.

  That emptiness is the point: a wakeup is a hint that work may exist, never a
  carrier of work, so this registry holds no message data on any connection's
  behalf. What an entry *does* carry is the connection's `DMEx.Log.Status` — two
  machine words the dispatcher and `DMEx.Log.Watcher` read without messaging the
  stream process, which matters most when it is wedged and would not answer —
  and its **selection**, the message types it asked for.

  **The selection lives in the entry, not the key.** One entry per connection, so
  registration, cleanup and every `each/2` pass do not multiply by the size of a
  selection; the membership test is one `MapSet.member?/2` inside a walk that was
  already `O(C)`.

  Entries are registered by the stream process itself — the request process over
  SSE, the reader process for `DMEx.Transport.Local` — so an entry dies with its
  process and a live stream needs no cleanup.

  The one exception is a connect that registers and then fails before it becomes
  a stream: over HTTP/1.1 keep-alive the request process is the *connection*
  process and outlives its response, so that path calls `unregister/1`.

  It runs the other way too, which is what a registry crash means for the
  connections in it: `Registry` links to every process it holds an entry for, so
  a registry that dies takes its streams down with it rather than leaving them
  live and registered nowhere. The subtree restarts empty and the clients
  reconnect.
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
    %{id: name(log), start: {__MODULE__, :start_link, [log]}, type: :supervisor}
  end

  @doc """
  Starts this log's registry.

  Through `DMEx.Log.__start__/1` rather than `Registry.start_link/1` directly,
  because a `Registry` is itself a subtree: its partition traps exits, so it
  releases its registered name only once it has been *scheduled* to handle the
  kill. A restart racing that would fail `{:already_started, _}` on the
  partition, spend the log supervisor's restart intensity in a few hundred
  microseconds, and take the tree above down with it.
  """
  @spec start_link(module()) :: Supervisor.on_start()
  def start_link(log) when is_atom(log) do
    DMEx.Log.__start__(fn -> Registry.start_link(keys: :duplicate, name: name(log)) end)
  end

  @doc """
  Registers the calling process as one of this log's streams.

  Called by the request process before its first read — so an append landing in
  between is picked up by that read rather than missed — and before its first
  *write*, so the watcher covers the connect path too.

  Returns the connection's `DMEx.Log.Status`, armed for `budget` and stored in
  the entry alongside `selection`. Keeping both there rather than in a table
  of their own is what makes cleanup free.
  """
  @spec register(module(), Status.budget(), selection()) :: {:ok, Status.t()}
  def register(log, budget, selection \\ :all) when is_atom(log) do
    status = Status.new(budget)

    # This registry is `keys: :duplicate`, so registration cannot be refused —
    # `{:error, {:already_registered, _}}` is a `:unique` result. A missing
    # registry raises, which is a supervision fault rather than a connect error.
    {:ok, _owner} = Registry.register(name(log), @key, {status, selection})

    {:ok, status}
  end

  @doc """
  Drops the calling process's entry.

  Only for a connect that registered and then failed before becoming a stream —
  a stream's entry goes when its process does.
  """
  @spec unregister(module()) :: :ok
  def unregister(log) when is_atom(log), do: Registry.unregister(name(log), @key)

  @doc """
  How many message types a selection admits, or `nil` for the whole log.

  For the `matched:` measurement on `[:dmex, :stream, :connect]`, which both
  transports emit — shared so the two cannot report it differently.
  """
  @spec matched(selection()) :: non_neg_integer() | nil
  def matched(:all), do: nil
  def matched(selection), do: MapSet.size(selection)

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
