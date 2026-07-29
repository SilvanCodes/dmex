defmodule DMEx.Producer.Messages do
  @moduledoc """
  The message types a producer's code knows how to publish.

  This is what a consumer's filter is resolved against: `DMEx.Producer.Plug`
  tests every type here for membership once at connect, and the types that pass
  become the connection's `message_type = ANY($1)` predicate and its wakeup
  routing set.

  ## Why the *modules*, and not the log's contents

  The two look interchangeable and are not. Expanding over the types the log has
  actually seen — `SELECT DISTINCT message_type ...` — goes stale silently: a
  producer's code can know a type it has not yet emitted, and the first emission
  of it involves no restart and no reconnect, so an open connection would keep
  an expansion missing a type its consumer genuinely selected. That is a false
  negative, which is silent message loss.

  Expanding over the compiled modules has no such gap. That set changes only
  when the producer deploys, and a deploy takes down the web server and with it
  every SSE connection, since the request process *is* the stream. Every
  affected consumer reconnects and re-expands against the new set, so the repair
  is forced by the same property that makes producer restarts uneventful.

  The trap worth recording, since it is the intuitive reading: that a type the
  producer has only just begun publishing must have been unknown to the consumer
  too, and so could only ever be a false positive. It does not hold. The two
  vocabularies are independent — a consumer deployed ahead of its producer
  legitimately names types the producer has not yet emitted — so such a type can
  be a true positive, and dropping it would be a real false negative. What saves
  it is the deploy boundary, not the definition.

  ## A superset is free; a subset is silent loss

  This set only has to *contain* everything the producer might publish. An extra
  type in it simply never matches any consumer's filter and costs nothing, while
  a missing one is a false negative. That asymmetry is the whole reason the scan
  is what it is: deriving the list precisely — from `publish/2` call sites, say —
  would be exact about a question where exactness has no value, and blind to
  every dynamic call site, `publish(build_message(order))` included. A scan of
  loadable modules cannot omit a type the producer is capable of publishing,
  because publishing one requires its module to be loadable.

  ## Which applications are scanned

  Only those that depend on DMEx, plus the producer's own. A module cannot
  `use DMEx.Message` unless its application depends on `:dmex`, so that
  condition is exact rather than a heuristic — and it is what keeps the scan off
  the several thousand modules in OTP and Elixir that could never qualify.

  It is also what makes a **shared message package** work: a separate library of
  message modules, depended on by producer and consumer alike, lists `:dmex`
  among its own applications and is therefore scanned like any other.

  ## Cached in `:persistent_term`

  Computed once, in a producer's supervisor, and read on every connect. The
  set is immutable for the life of the node — the modules compiled into a
  release do not change while it runs — so a term with no copy-on-read is
  exactly the right shape, and connect-time expansion is a lookup rather than a
  scan.
  """

  @doc """
  This producer's known message types, computing them if they are not cached yet.

  The lazy branch exists so `DMEx.Producer.Plug` never depends on start order: a
  producer whose supervisor has not run `refresh/1` — in a test that mounts the
  plug against a bare producer module, say — still answers correctly, just more
  slowly the first time.
  """
  @spec types(module()) :: MapSet.t(String.t())
  def types(producer) when is_atom(producer) do
    case :persistent_term.get(key(producer), nil) do
      nil -> refresh(producer)
      types -> types
    end
  end

  @doc """
  Rescans and caches this producer's message types.

  Called from a producer's supervisor. Writing a `:persistent_term` forces
  a global GC scan, which is why this happens once at boot rather than per
  connection.
  """
  @spec refresh(module()) :: MapSet.t(String.t())
  def refresh(producer) when is_atom(producer) do
    types = scan(producer.__dmex__().otp_app)

    :persistent_term.put(key(producer), types)

    types
  end

  @doc false
  @spec forget(module()) :: :ok
  def forget(producer) when is_atom(producer) do
    :persistent_term.erase(key(producer))

    :ok
  end

  ## Internals

  defp key(producer), do: {__MODULE__, producer}

  defp scan(otp_app) do
    otp_app
    |> applications()
    |> Enum.flat_map(&modules/1)
    |> Enum.filter(&DMEx.Message.message?/1)
    |> MapSet.new(& &1.type())
  end

  # The producer's own application, plus every loaded application that declares
  # a dependency on DMEx — which is exactly the set whose modules could contain
  # a `use DMEx.Message`.
  defp applications(otp_app) do
    dependents =
      for {app, _description, _vsn} <- Application.loaded_applications(),
          depends_on_dmex?(app),
          do: app

    Enum.uniq([otp_app | dependents])
  end

  defp depends_on_dmex?(app) do
    case :application.get_key(app, :applications) do
      {:ok, applications} -> :dmex in applications
      _other -> false
    end
  end

  defp modules(app) do
    case :application.get_key(app, :modules) do
      {:ok, modules} -> modules
      _other -> []
    end
  end
end
