defmodule DMEx.Log.Messages do
  @moduledoc """
  The message types a log's code knows how to append.

  What a consumer's filter is resolved against: `DMEx.Log.Plug` tests every type
  here for membership once at connect, and the types that pass become the
  connection's `message_type = ANY($1)` predicate and its wakeup routing set.

  ## Why the *modules*, and not the log's contents

  Expanding over the types the log has actually seen —
  `SELECT DISTINCT message_type` — goes stale silently: a log's code can know a
  type it has not yet emitted, and first emission involves no restart and no
  reconnect, so an open connection would keep an expansion missing a type its
  consumer genuinely selected. That is a false negative, which is silent message
  loss.

  Compiled modules have no such gap: that set changes only on deploy, and a deploy
  takes every SSE connection with it, so every affected consumer reconnects and
  re-expands.

  The set only has to *contain* everything the log might append: an extra type
  matches no filter, a missing one is silent loss. Scanned applications are
  `DMEx.Discovery`'s set, cached in `:persistent_term` because the set is
  immutable for the life of the node, so expansion is a lookup rather than a
  scan.

  Built through `DMEx.Message.types!/1` rather than a bare `MapSet.new/2`, so two
  modules declaring one `type:` raise here instead of collapsing into a set that
  is quietly one short.
  """

  @doc """
  Returns this log's known message types, computing them if not cached yet.

  The lazy branch exists so `DMEx.Log.Plug` never depends on start order: a log
  whose supervisor has not run `refresh/2` still answers correctly, just more
  slowly the first time. It scans without an `otp_app`, which costs nothing in a
  release, where the host application declares `:dmex` and is scanned anyway.
  """
  @spec types(module()) :: MapSet.t(String.t())
  def types(log) when is_atom(log) do
    case :persistent_term.get(key(log), nil) do
      nil -> refresh(log, nil)
      types -> types
    end
  end

  @doc """
  Rescans and caches this log's message types.

  Called from a log's supervisor, with the `otp_app:` that `DMEx.Supervisor` was
  given. Writing a `:persistent_term` forces a global GC scan, which is why this
  happens once at boot rather than per connection.
  """
  @spec refresh(module(), atom() | nil) :: MapSet.t(String.t())
  def refresh(log, otp_app \\ nil) when is_atom(log) do
    types = scan(otp_app)

    :persistent_term.put(key(log), types)

    types
  end

  @doc false
  @spec forget(module()) :: :ok
  def forget(log) when is_atom(log) do
    :persistent_term.erase(key(log))

    :ok
  end

  ## Internals

  defp key(log), do: {__MODULE__, log}

  defp scan(otp_app) do
    otp_app
    |> DMEx.Message.discover()
    |> DMEx.Message.types!()
  end
end
