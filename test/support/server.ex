defmodule DMEx.Test.Server do
  @moduledoc """
  A real Bandit server mounting `DMEx.Log.Plug`, for the integration suite.

  Binds port 0, so it never collides with anything on the machine. The router is
  the same `forward/2` line the documentation tells a host application to write,
  so these tests exercise the documented mount rather than a private one.
  """

  defmodule Router do
    @moduledoc false
    use Plug.Router

    plug(:match)
    plug(:dispatch)

    forward("/stream",
      to: DMEx.Log.Plug,
      init_opts: [log: DMEx.Test.IntegrationLog]
    )

    match _ do
      send_resp(conn, 404, "not found")
    end
  end

  @doc """
  Starts a server on an ephemeral port and returns its base URL.

  Registers cleanup with the calling test, so nothing leaks between cases.
  """
  @spec start!(keyword()) :: String.t()
  def start!(opts \\ []) do
    plug = Keyword.get(opts, :plug, Router)

    {:ok, pid} =
      Bandit.start_link(
        plug: plug,
        scheme: :http,
        ip: {127, 0, 0, 1},
        port: 0,
        startup_log: false
      )

    ExUnit.Callbacks.on_exit(fn ->
      ref = Process.monitor(pid)
      Process.exit(pid, :normal)

      receive do
        {:DOWN, ^ref, :process, ^pid, _} -> :ok
      after
        1_000 -> :ok
      end
    end)

    {:ok, {_ip, port}} = ThousandIsland.listener_info(pid)

    "http://127.0.0.1:#{port}"
  end

  @doc """
  Binds `:test` to `url` for `DMEx.Supervisor`. Not a literal anywhere, since
  the port is 0 until the server is running.
  """
  @spec logs(String.t()) :: keyword(String.t())
  def logs(url), do: [test: url]
end
