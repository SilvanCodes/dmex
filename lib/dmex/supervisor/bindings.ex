defmodule DMEx.Supervisor.Bindings do
  @moduledoc """
  What a log name means: a log module this VM owns, or the URL of someone else's
  stream.

  The target is also the whole of the transport choice — see `DMEx.Supervisor`.

  Both functions here are pure functions of the `bindings:` option, so nothing has to
  be running to answer them. That is why `DMEx.Supervisor.init/1` resolves every
  declared log before it starts a child: a misconfiguration refuses the boot rather
  than surfacing at the first connect as a stream that silently never exists.
  """

  alias DMEx.Handler
  alias DMEx.Supervisor

  @typedoc "Log name to target: a log module in this VM, or a stream URL."
  @type t :: %{String.t() => String.t() | module()}

  @doc """
  The `bindings:` option, with every name and target checked.

  Names are validated where they are declared rather than at first connect, because
  a typo in a key and a typo in a handler's `logs:` are the same bug and only one
  of them is visible in the "unknown log" message.
  """
  @spec declared!(keyword()) :: t()
  def declared!(opts) do
    case Keyword.get(opts, :bindings, []) do
      entries when is_list(entries) or is_map(entries) ->
        Enum.reduce(entries, %{}, &bind/2)

      other ->
        raise ArgumentError,
              "`bindings:` must be a keyword list of name to URL or log module, got: #{inspect(other)}"
    end
  end

  @doc """
  The bindings a `DMEx.Supervisor.subscribe/2` call should use.

  Its own `bindings:` if it brought one, else the ones `init/1` seeded as registry
  metadata.
  """
  @spec resolve!(keyword(), atom()) :: t()
  def resolve!(opts, supervisor) do
    if Keyword.has_key?(opts, :bindings) do
      declared!(opts)
    else
      seeded!(supervisor)
    end
  end

  defp seeded!(supervisor) do
    case Supervisor.meta(supervisor, :bindings) do
      {:ok, bindings} -> bindings
      :error -> raise_no_logs(supervisor)
    end
  end

  @doc """
  The transport and origin for one of a handler's declared logs, or raises.

  Resolving is the check: `DMEx.Supervisor.init/1` calls this for every log every
  discovered handler declares, and a name nothing binds cannot become a
  subscription.
  """
  @spec transport!(Handler.declaration(), String.t(), t()) :: {module(), String.t() | module()}
  def transport!(declaration, log, logs) do
    case Map.fetch(logs, log) do
      {:ok, url} when is_binary(url) -> {DMEx.Transport.SSE, url}
      {:ok, module} when is_atom(module) -> {DMEx.Transport.Local, module}
      :error -> raise_unbound(declaration, log, logs)
    end
  end

  ## Checking one entry

  defp bind({name, target}, logs) do
    name =
      Handler.identifier!(
        name,
        "a log name",
        " It is the same charset a handler's `logs:` and `name:` use, so \"/\" can join " <>
          "them with no escaping."
      )

    # One name is one target, so a repeat is a mistake and not a merge.
    if Map.has_key?(logs, name) do
      raise ArgumentError,
            "`bindings:` names #{inspect(name)} twice. One name is one log — one target, one " <>
              "transport, one subscription per handler that reads it — so a repeat could only " <>
              "ever mean one of them."
    end

    Map.put(logs, name, target!(name, target))
  end

  defp bind(other, _logs) do
    raise ArgumentError, "a `bindings:` entry must be `name: url_or_module`, got: #{inspect(other)}"
  end

  defp target!(name, module) when is_atom(module) and not is_nil(module) do
    # `append/2` rather than `__dmex__/0`: a handler answers that one too.
    if Code.ensure_loaded?(module) and function_exported?(module, :append, 2) do
      module
    else
      raise ArgumentError,
            "the log #{inspect(name)} is bound to #{inspect(module)}, which is not a module " <>
              "built with `use DMEx.Log`. Bind a name to a log module this application owns, " <>
              "or to the URL of someone else's."
    end
  end

  defp target!(name, url) when is_binary(url) and url != "" do
    case URI.parse(url) do
      # `host` covers `nil` as well as `""`: `URI.parse/1` returns the first for
      # `"http:stream"` and the second for `"http:///stream"`, and both are the
      # missing host, not a bad scheme.
      %URI{scheme: scheme, host: host} when scheme in ["http", "https"] ->
        if host in [nil, ""], do: raise_url(name, url, "it has no host"), else: url

      %URI{scheme: nil} ->
        raise_url(name, url, "it has no scheme")

      %URI{scheme: scheme} ->
        raise_url(name, url, "#{inspect(scheme)} is not http or https")
    end
  end

  defp target!(name, other), do: raise_url(name, other, "it is neither a log module nor a non-empty string")

  ## Errors

  defp raise_url(name, url, why) do
    raise ArgumentError,
          "the URL for #{inspect(name)} is not usable — #{why}: #{inspect(url)}. It must be an " <>
            "absolute http:// or https:// URL, e.g. \"https://orders.internal/stream\"."
  end

  # Names the known logs, because the fix is either here or in the handler and only
  # the list says which.
  defp raise_unbound(declaration, log, logs) do
    raise ArgumentError,
          "#{inspect(declaration.handler)} reads #{inspect(log)}, which is not in `bindings:`. " <>
            "Known logs: #{known(logs)}. Add it where DMEx is started, e.g. " <>
            "`bindings: [#{log}: \"https://.../stream\"]`."
  end

  defp known(logs) when map_size(logs) == 0, do: "(none)"
  defp known(logs), do: logs |> Map.keys() |> Enum.sort() |> Enum.join(", ")

  defp raise_no_logs(supervisor) do
    raise ArgumentError,
          "DMEx.Supervisor.subscribe/2 has no `bindings:` to resolve this handler's `logs:` against. " <>
            "Pass them on the call, or start #{inspect(supervisor)} with them: " <>
            "`{DMEx.Supervisor, repo: MyApp.Repo, bindings: [...]}`."
  end
end
