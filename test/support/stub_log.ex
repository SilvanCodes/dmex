defmodule DMEx.Test.StubLog do
  @moduledoc """
  A log whose storage is an in-memory stub, resolved per process.

  `use DMEx.Log` builds its configuration at compile time, which is exactly
  right in production and useless for a test that wants a fresh in-memory log
  per case. This module implements the same `__dmex__/0` contract but reads the
  stub's opts out of the calling process's dictionary, so every test gets its
  own log and the suite stays `async: true`.

  Call `install/2` in whichever process will run the plug.
  """

  alias DMEx.Test.Storage

  @key :dmex_test_stub

  @doc "Points this process's stub log at `opts`, with optional config overrides."
  @spec install(DMEx.Storage.opts(), keyword()) :: :ok
  def install(storage, config_opts \\ []) do
    Process.put(@key, {storage, DMEx.Config.log!(config_opts)})

    :ok
  end

  @doc "The installed state, for handing to a process that did not install it."
  @spec export() :: {DMEx.Storage.opts(), DMEx.Config.log()}
  def export, do: Process.get(@key)

  @doc "Installs previously exported state into the calling process."
  @spec import(term()) :: :ok
  def import(exported) do
    Process.put(@key, exported)

    :ok
  end

  @doc false
  def __dmex__ do
    {storage, config} = Process.get(@key)

    %{
      log: __MODULE__,
      config: config,
      storage: storage |> Map.put(:adapter, Storage) |> Map.put(:log, __MODULE__),
      notifier: %{
        adapter: DMEx.Notifier.Beam,
        repo: Storage,
        name: Module.concat(__MODULE__, "Notifications"),
        channel: "dmex_public"
      }
    }
  end
end
