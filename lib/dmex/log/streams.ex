defmodule DMEx.Log.Streams do
  @moduledoc """
  Where a log's live stream processes register themselves for wakeups.

  One Elixir `Registry` per log, holding one duplicate-key entry per open
  connection. `DMEx.Log.Dispatcher` broadcasts a bare `:wakeup` across it;
  nothing else is ever sent.

  That emptiness is the point: a wakeup is a hint that work may exist, never a
  carrier of work, so this registry holds no message data on any connection's
  behalf. What an entry *does* carry is the connection's `DMEx.Log.Status`, read
  by the dispatcher and `DMEx.Log.Watcher` without messaging the stream process,
  and its `DMEx.Log.Selection`, the message types it asked for.

  **The selection lives in the entry, not the key.** One entry per connection, so
  registration, cleanup and every `each/2` pass do not multiply by the size of a
  selection; the membership test is one `MapSet.member?/2` inside a walk that was
  already `O(C)`.

  Entries are registered by the stream process itself — the request process over
  SSE, the reader process for `DMEx.Transport.Local` — so an entry dies with its
  process and a live stream needs no cleanup.

  A stream that ends without its process ending calls `unregister/1` on the way
  out — every path, connect failures and read failures alike. `DMEx.Log.Plug`
  covers why an entry cannot be left to die with a keep-alive process.

  It runs the other way too, which is what a registry crash means for the
  connections in it: `Registry` links to every process it holds an entry for, so
  a registry that dies takes its streams down with it rather than leaving them
  live and registered nowhere. The subtree restarts empty and the clients
  reconnect.

  **The registered process is `<Log>.Registry`, not `<Log>.Streams`** — see
  `name/1`. This module is `Streams` because that is what the table holds; the
  process it starts is a plain Elixir `Registry` and its name says so. Renaming
  the module is a source change, renaming a registered process is not.
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
  Starts this log's registry.

  Through `DMEx.Log.__start__/1` rather than `Registry.start_link/1` directly,
  because a `Registry` is itself a subtree: its partition traps exits, so it
  releases its registered name only once it has been *scheduled* to handle the
  kill — exactly the `{:already_started, _}` restart race `DMEx.Log.__start__/1`
  waits out.

  **`:partitions` is omitted deliberately** — it defaults to 1, and `Registry`'s
  own docs recommend raising it under write concurrency, so this is a plausible
  thing to "optimise" later. At one partition `Registry.dispatch/3` invokes
  `each/2`'s callback exactly once, with every entry. Partitioned, it invokes it
  once per partition, and `:parallel` — inert today — starts running it in
  `Task` processes. Per-entry work survives both; per-*pass* work does not, and
  both walkers do per-pass work: `DMEx.Log.Watcher` takes one clock reading per
  sweep and `DMEx.Log.Dispatcher` emits one telemetry event per broadcast. Both
  keep it outside the callback, which is what makes them correct. Raising
  `:partitions` means checking that they still do.
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
  the entry alongside `selection`.
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

  @doc """
  Drops the calling process's entry.

  Only for a connect that registered and then failed before becoming a stream —
  a stream's entry goes when its process does.
  """
  @spec unregister(module()) :: :ok
  def unregister(log) when is_atom(log), do: Registry.unregister(name(log), @key)

  @doc """
  Runs `fun` for every registered stream, with its pid, status and selection.

  There is deliberately no blanket `broadcast/2`: the dispatcher decides per
  connection whether a wakeup is worth sending, and `DMEx.Log.Watcher` uses this
  to read every connection's deadline. It is one ETS read and no message to any
  stream, so a wedged connection cannot stall the walk — but `fun` runs in the
  caller's own process, so it must not block either.

  One call per entry, in one pass. See `start_link/1` for what that rests on.
  """
  @spec each(module(), (pid(), Status.t(), Selection.t() -> any())) :: :ok
  def each(log, fun) when is_atom(log) and is_function(fun, 3) do
    Registry.dispatch(name(log), @key, fn entries ->
      Enum.each(entries, fn {pid, {status, selection}} -> fun.(pid, status, selection) end)
    end)
  end

  @doc """
  How many streams are currently registered. For tests.

  `Registry.count/1` is `:ets.info(size)`; counting the entries under `@key`
  instead would be `:ets.select_count/2`, a full scan for the same answer —
  every entry is under that one key. A broadcast counts its own pass, so
  nothing on the hot path calls this.
  """
  @spec count(module()) :: non_neg_integer()
  def count(log) when is_atom(log), do: Registry.count(name(log))
end
