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

  @key :dmex_test_stub

  @doc "Points this process's stub log at `opts`, with optional config overrides."
  @spec install(DMEx.Storage.opts(), keyword()) :: :ok
  def install(storage_opts, config_opts \\ []) do
    Process.put(@key, {storage_opts, DMEx.Config.log!(config_opts)})

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
    {storage_opts, config} = Process.get(@key)

    %{
      log: __MODULE__,
      repo: DMEx.Test.Storage,
      storage: DMEx.Test.Storage,
      notifier: DMEx.Notifier.Beam,
      app_key: "stub",
      channel: "dmex_stub",
      lock: {0, 0},
      prefix: nil,
      config: config,
      storage_opts: Map.put(storage_opts, :log, __MODULE__),
      notifier_opts: %{repo: DMEx.Test.Storage, log: __MODULE__, channel: "dmex_stub"}
    }
  end
end
