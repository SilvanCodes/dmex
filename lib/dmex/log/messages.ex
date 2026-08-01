defmodule DMEx.Log.Messages do
  @moduledoc """
  The message types a log's code knows how to append.

  This is what a consumer's filter is resolved against: `DMEx.Log.Plug`
  tests every type here for membership once at connect, and the types that pass
  become the connection's `message_type = ANY($1)` predicate and its wakeup
  routing set.

  ## Why the *modules*, and not the log's contents

  The two look interchangeable and are not. Expanding over the types the log has
  actually seen — `SELECT DISTINCT message_type ...` — goes stale silently: a
  log's code can know a type it has not yet emitted, and the first emission
  of it involves no restart and no reconnect, so an open connection would keep
  an expansion missing a type its consumer genuinely selected. That is a false
  negative, which is silent message loss.

  Expanding over the compiled modules has no such gap. That set changes only
  when the log deploys, and a deploy takes down the web server and with it
  every SSE connection, since the request process *is* the stream. Every
  affected consumer reconnects and re-expands against the new set, so the repair
  is forced by the same property that makes log restarts uneventful.

  The trap worth recording, since it is the intuitive reading: that a type the
  log has only just begun appending must have been unknown to the consumer
  too, and so could only ever be a false positive. It does not hold. The two
  vocabularies are independent — a consumer deployed ahead of its log
  legitimately names types the log has not yet emitted — so such a type can
  be a true positive, and dropping it would be a real false negative. What saves
  it is the deploy boundary, not the definition.

  ## A superset is free; a subset is silent loss

  This set only has to *contain* everything the log might append. An extra
  type in it simply never matches any consumer's filter and costs nothing, while
  a missing one is a false negative. That asymmetry is the whole reason the scan
  is what it is: deriving the list precisely — from `append/2` call sites, say —
  would be exact about a question where exactness has no value, and blind to
  every dynamic call site, `append(build_message(order))` included. A scan of
  loadable modules cannot omit a type the log is capable of appending,
  because appending one requires its module to be loadable.

  ## Which applications are scanned

  `DMEx.Discovery`'s set — every application depending on DMEx, plus this log's
  own — which is the same set handler discovery reads.

  ## Cached in `:persistent_term`

  Computed once, in a log's supervisor, and read on every connect. The
  set is immutable for the life of the node — the modules compiled into a
  release do not change while it runs — so a term with no copy-on-read is
  exactly the right shape, and connect-time expansion is a lookup rather than a
  scan.
  """

  @doc """
  This log's known message types, computing them if they are not cached yet.

  The lazy branch exists so `DMEx.Log.Plug` never depends on start order: a
  log whose supervisor has not run `refresh/1` — in a test that mounts the
  plug against a bare log module, say — still answers correctly, just more
  slowly the first time.
  """
  @spec types(module()) :: MapSet.t(String.t())
  def types(log) when is_atom(log) do
    case :persistent_term.get(key(log), nil) do
      nil -> refresh(log)
      types -> types
    end
  end

  @doc """
  Rescans and caches this log's message types.

  Called from a log's supervisor. Writing a `:persistent_term` forces
  a global GC scan, which is why this happens once at boot rather than per
  connection.
  """
  @spec refresh(module()) :: MapSet.t(String.t())
  def refresh(log) when is_atom(log) do
    types = scan(log.__dmex__().otp_app)

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
    |> DMEx.Discovery.modules()
    |> Enum.filter(&DMEx.Message.message?/1)
    |> MapSet.new(& &1.type())
  end
end
