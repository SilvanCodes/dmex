defmodule DMEx.Log.Streams do
  @moduledoc """
  Where a log's live stream processes register themselves for wakeups.

  One Elixir `Registry` per log, holding one duplicate-key entry per open
  connection. `DMEx.Log.Dispatcher` broadcasts a bare `:wakeup` across it and
  nothing else is ever sent: a wakeup is a hint that work may exist, never a
  carrier of work, so this registry holds no message data on any connection's
  behalf. An entry carries the connection's `DMEx.Log.Status` and its
  `DMEx.Log.Selection` — in the entry rather than the key, so no walk multiplies
  by the size of a selection — and is registered by the stream process itself, so
  it dies with that process.

  `Registry` links to every process it holds an entry for, so a registry that
  dies takes its streams down with it rather than leaving them live and
  registered nowhere. The subtree restarts empty and the clients reconnect.
  """

  alias DMEx.Log.Selection
  alias DMEx.Log.Status

  @key :streams

  @doc "The registry name for a log module — `<Log>.Registry`, deliberately not `<Log>.Streams`."
  @spec name(module()) :: atom()
  def name(log) when is_atom(log), do: Module.concat(log, "Registry")

  @doc "The child spec a log's supervisor uses to start this registry."
  @spec child_spec(module()) :: Supervisor.child_spec()
  def child_spec(log) when is_atom(log) do
    %{id: name(log), start: {__MODULE__, :start_link, [log]}, type: :supervisor}
  end

  @doc """
  Starts this log's registry, through `DMEx.Log.__start__/1` rather than
  `Registry.start_link/1` directly: a `Registry` is itself a subtree whose
  partition traps exits, so it releases its registered name only once it has been
  *scheduled* to handle the kill — exactly the `{:already_started, _}` restart
  race `DMEx.Log.__start__/1` waits out.
  """
  @spec start_link(module()) :: Supervisor.on_start()
  def start_link(log) when is_atom(log) do
    DMEx.Log.__start__(fn -> Registry.start_link(keys: :duplicate, name: name(log)) end)
  end

  @doc """
  Registers the calling process as one of this log's streams.

  Before its first read — so an append landing in between is picked up by that
  read rather than missed — and before its first *write*, so the watcher covers
  the connect path too. Returns the connection's `DMEx.Log.Status`, armed for
  `budget`.
  """
  @spec register(module(), Status.budget(), Selection.t()) :: {:ok, Status.t()}
  def register(log, budget, selection) when is_atom(log) do
    status = Status.new(budget)

    # This registry is `keys: :duplicate`, so registration cannot be refused —
    # `{:error, {:already_registered, _}}` is a `:unique` result. A missing
    # registry raises, which is a supervision fault rather than a connect error.
    {:ok, _owner} = Registry.register(name(log), @key, {status, selection})

    {:ok, status}
  end

  @doc "Drops the calling process's entry, for a stream whose process outlives it."
  @spec unregister(module()) :: :ok
  def unregister(log) when is_atom(log), do: Registry.unregister(name(log), @key)

  @doc """
  Every registered stream: its pid, its `DMEx.Log.Status` and its selection.

  Deliberately no blanket `broadcast/2`: the dispatcher decides per connection
  whether a wakeup is worth sending and `DMEx.Log.Watcher` reads every deadline
  out of the same list, one ETS read with no message to any stream.
  """
  @spec all(module()) :: [{pid(), Status.t(), Selection.t()}]
  def all(log) when is_atom(log) do
    for {pid, {status, selection}} <- Registry.lookup(name(log), @key), do: {pid, status, selection}
  end

  @doc """
  How many streams are registered. `Registry.count/1`, so it agrees with `all/1`
  without building the list.
  """
  @spec count(module()) :: non_neg_integer()
  def count(log) when is_atom(log), do: Registry.count(name(log))
end
