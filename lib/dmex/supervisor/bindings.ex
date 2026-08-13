defmodule DMEx.Supervisor.Bindings do
  @moduledoc """
  What a log name means, and everything checked about it before anything starts.

  A binding maps a name a handler declared — `:orders` — to a concrete target:
  a log module this VM owns, or the URL of someone else's stream. The binding
  is also the whole of the transport choice, which is why nothing configures
  one: a module is read in this VM (`DMEx.Transport.Local`), a URL over HTTP
  (`DMEx.Transport.SSE`).

  Every check here is a pure function of the `logs:` option and the discovered
  declarations — an illegal name, a target that is neither a log module nor an
  absolute http(s) URL, two handlers sharing a subscription id, a handler
  reading a log nothing binds. Nothing has to be running to answer any of them,
  which is why `DMEx.Supervisor.init/1` asks before it starts a child: a
  misconfiguration then refuses the boot, rather than surfacing at the first
  subscribe as a stream that silently never exists.

  `DMEx.Supervisor.subscribe/2` resolves the bindings a second time, from its
  own `logs:` or from the registry metadata `init/1` seeded, since a call may
  bring its own.
  """

  alias DMEx.Handler
  alias DMEx.Supervisor

  @typedoc "Log name to target: a log module in this VM, or a stream URL."
  @type t :: %{String.t() => String.t() | module()}

  @doc false
  @spec startable!([module()], t()) :: [module()]
  def startable!(handlers, logs) do
    reject_duplicate_ids!(handlers)
    reject_unbound_logs!(handlers, logs)

    handlers
  end

  # Names both modules, because "duplicate subscription id" without them is a
  # riddle in an application with thirty handlers.
  defp reject_duplicate_ids!(handlers) do
    pairs = for handler <- handlers, id <- Handler.subscription_ids(handler), do: {id, handler}

    _ =
      Enum.reduce(pairs, %{}, fn {id, handler}, seen ->
        case seen do
          %{^id => already} ->
            raise ArgumentError,
                  "#{inspect(already)} and #{inspect(handler)} both declare the subscription " <>
                    "#{inspect(id)}. A subscription id keys the cursor, so two handlers " <>
                    "sharing one would clobber each other's position — give one of them a " <>
                    "different `name:`."

          _ ->
            Map.put(seen, id, handler)
        end
      end)

    :ok
  end

  defp reject_unbound_logs!(handlers, logs) do
    for handler <- handlers,
        declaration = Handler.declaration!(handler),
        log <- declaration.logs do
      _ = transport!(declaration, log, logs)
    end

    :ok
  end

  ## Log resolution

  # The binding is the whole of the choice, which is why there is no transport
  # option anywhere: there would be nothing left for one to say.
  @doc false
  @spec transport!(Handler.declaration(), String.t(), t()) :: {module(), String.t() | module()}
  def transport!(declaration, log, logs) do
    case Map.fetch(logs, log) do
      {:ok, url} when is_binary(url) -> {DMEx.Transport.SSE, url}
      {:ok, module} when is_atom(module) -> {DMEx.Transport.Local, module}
      :error -> raise_unbound(declaration, log, logs)
    end
  end

  defp raise_unbound(declaration, log, logs) do
    raise ArgumentError, """
    #{inspect(declaration.handler)} reads #{inspect(log)}, which is not in `logs:`.

    Known logs: #{known(logs)}

    Add it where DMEx is started:

        {DMEx.Supervisor,
         repo: MyApp.Repo,
         logs: [#{log}: "https://.../stream"]}
    """
  end

  defp known(logs) when map_size(logs) == 0, do: "(none)"
  defp known(logs), do: logs |> Map.keys() |> Enum.sort() |> Enum.join(", ")

  @doc false
  @spec resolve_logs!(keyword(), atom()) :: t()
  def resolve_logs!(opts, supervisor) do
    if Keyword.has_key?(opts, :logs) do
      logs!(opts)
    else
      # Checking the registry is alive first: Registry.meta/2 raises its own
      # "unknown registry" ArgumentError otherwise.
      with pid when is_pid(pid) <- Process.whereis(Supervisor.registry(supervisor)),
           {:ok, logs} <- Registry.meta(Supervisor.registry(supervisor), :logs) do
        logs
      else
        _ -> raise_no_logs(supervisor)
      end
    end
  end

  # Names are validated where they are declared rather than at first connect,
  # because a typo in a key and a typo in a handler's `logs:` are the same bug
  # and only one is visible in the "unknown log" message.
  @doc false
  @spec logs!(keyword()) :: t()
  def logs!(opts) do
    case Keyword.get(opts, :logs, []) do
      entries when is_list(entries) or is_map(entries) ->
        Enum.reduce(entries, %{}, &bind/2)

      other ->
        raise ArgumentError,
              "`logs:` must be a keyword list of name to URL or log module, got: #{inspect(other)}"
    end
  end

  defp bind({name, target}, logs) do
    name =
      DMEx.validate_identifier!(
        name,
        "a log name",
        " It is the same charset a handler's `logs:` and `name:` use, so \"/\" can join " <>
          "them with no escaping."
      )

    # `Map.new/2` collapsed a repeat silently, and this is the copy where the
    # loser is a whole log's target and therefore its transport. Same rule as
    # `DMEx.validate_keys!/5` and `DMEx.Handler`'s own `logs:`, for the same
    # reason.
    if Map.has_key?(logs, name) do
      raise ArgumentError,
            "`logs:` names #{inspect(name)} twice. One name is one log — one target, one " <>
              "transport, one subscription per handler that reads it — so a repeat could only " <>
              "ever mean one of them."
    end

    Map.put(logs, name, log_target!(name, target))
  end

  defp bind(other, _logs) do
    raise ArgumentError, "a `logs:` entry must be `name: url_or_module`, got: #{inspect(other)}"
  end

  defp log_target!(name, module) when is_atom(module) and not is_nil(module) do
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

  defp log_target!(name, url) when is_binary(url) and url != "" do
    case URI.parse(url) do
      # `host` covers `nil` as well as `""`: `URI.parse/1` returns the first for
      # `"http:stream"` and the second for `"http:///stream"`, and both are the
      # missing host, not a bad scheme.
      %URI{scheme: scheme, host: host} when scheme in ["http", "https"] ->
        if host in [nil, ""], do: raise_log_url(name, url, "it has no host"), else: url

      %URI{scheme: nil} ->
        raise_log_url(name, url, "it has no scheme")

      %URI{scheme: scheme} ->
        raise_log_url(name, url, "#{inspect(scheme)} is not http or https")
    end
  end

  defp log_target!(name, other), do: raise_log_url(name, other, "it is neither a log module nor a non-empty string")

  defp raise_log_url(name, url, why) do
    raise ArgumentError,
          "the URL for #{inspect(name)} is not usable — #{why}: #{inspect(url)}. " <>
            "It must be an absolute http:// or https:// URL, e.g. " <>
            "\"https://orders.internal/stream\"."
  end

  defp raise_no_logs(supervisor) do
    raise ArgumentError, """
    DMEx.Supervisor.subscribe/2 has no `logs:` to resolve this handler's `logs:` against.

    Either pass them on the call:

        DMEx.Supervisor.subscribe(MyApp.OrderProjection,
          repo: MyApp.Repo,
          logs: [orders: "https://orders.internal/stream"]
        )

    or start #{inspect(supervisor)} in your supervision tree with them:

        {DMEx.Supervisor, repo: MyApp.Repo, logs: [...]}
    """
  end
end
