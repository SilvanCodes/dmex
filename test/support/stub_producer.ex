defmodule DMEx.Test.StubProducer do
  @moduledoc """
  A producer whose storage is an in-memory stub, resolved per process.

  `use DMEx.Producer` builds its configuration at compile time, which is exactly
  right in production and useless for a test that wants a fresh in-memory log
  per case. This module implements the same `__dmex__/0` contract but reads the
  stub's opts out of the calling process's dictionary, so every test gets its
  own log and the suite stays `async: true`.

  Call `install/2` in whichever process will run the plug.
  """

  @key :dmex_test_stub

  @doc "Points this process's stub producer at `opts`, with optional config overrides."
  @spec install(DMEx.Storage.opts(), keyword()) :: :ok
  def install(storage_opts, config_opts \\ []) do
    Process.put(@key, {storage_opts, DMEx.Config.producer!(config_opts)})

    :ok
  end

  @doc "The installed state, for handing to a process that did not install it."
  @spec export() :: {DMEx.Storage.opts(), DMEx.Config.producer()}
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
      producer: __MODULE__,
      repo: DMEx.Test.Storage,
      storage: DMEx.Test.Storage,
      app_key: "stub",
      channel: "dmex_stub",
      lock: {0, 0},
      prefix: nil,
      config: config,
      storage_opts: Map.put(storage_opts, :producer, __MODULE__)
    }
  end
end
