defmodule DMEx.BenchHelper do
  @moduledoc """
  Shared setup and fixtures for the benchmark suite.
  """

  alias DMEx.Test.Storage, as: Stub

  @doc "Starts the integration repo, for benchmarks that touch a real database."
  def start_repo! do
    Application.ensure_all_started(:ecto_sql)
    Application.ensure_all_started(:postgrex)

    case DMEx.Test.IntegrationRepo.start_link() do
      {:ok, pid} -> pid
      {:error, {:already_started, pid}} -> pid
    end
  end

  @doc """
  Starts the integration log, tolerating one already running.

  `mix bench.e2e` requires every file into one VM, so each has to be safe both
  standalone and second in line.
  """
  def start_log! do
    case DMEx.Test.IntegrationLog.start_link(otp_app: :dmex) do
      {:ok, pid} -> pid
      {:error, {:already_started, pid}} -> pid
    end
  end

  @doc """
  Starts a named DMEx.Supervisor bound to `url` as log `:bench`, tolerating one
  already running.

  The server binds an ephemeral port, so the map cannot be a literal.
  """
  def start_supervisor!(name, url) do
    case DMEx.Supervisor.start_link(
           name: name,
           repo: DMEx.Test.IntegrationRepo,
           otp_app: nil,
           logs: [bench: url]
         ) do
      {:ok, pid} -> pid
      {:error, {:already_started, pid}} -> pid
    end
  end

  @doc "Starts Bandit on an ephemeral port and returns the base URL."
  def start_server! do
    Application.ensure_all_started(:bandit)

    {:ok, server} =
      Bandit.start_link(
        plug: DMEx.Test.Server.Router,
        scheme: :http,
        ip: {127, 0, 0, 1},
        port: 0,
        startup_log: false
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)

    "http://127.0.0.1:#{port}"
  end

  @doc "Truncates `dmex_messages` and `dmex_cursors`, so a run does not measure the previous one's rows."
  def reset_log! do
    Ecto.Adapters.SQL.query!(
      DMEx.Test.IntegrationRepo,
      "TRUNCATE dmex_messages, dmex_cursors RESTART IDENTITY CASCADE",
      []
    )
  end

  @doc """
  `count` log rows whose `data` is roughly `payload_bytes` of JSON.

  Rows carry `data` and `metadata` as JSON *text*, exactly as a storage adapter
  returns them, because that is what the encoder splices.
  """
  def rows(count, payload_bytes) do
    note = String.duplicate("x", max(payload_bytes - 20, 1))

    for id <- 1..count do
      Stub.row(id, data: ~s({"order_id":#{id},"note":"#{note}"}))
    end
  end

  @doc "A ready-made run of `count` SSE frames, one row each, at `payload_bytes`."
  def frame_bytes(count, payload_bytes) do
    count
    |> rows(payload_bytes)
    |> Enum.map(&DMEx.Transport.SSE.Frame.encode_row/1)
    |> IO.iodata_to_binary()
  end

  @doc "Splits a binary into fixed-size chunks, as a socket would deliver it."
  def chunks(binary, size) do
    binary
    |> Stream.unfold(fn
      "" -> nil
      rest -> String.split_at(rest, min(size, byte_size(rest)))
    end)
    |> Enum.to_list()
  end

  @doc "Standard Benchee options, so every benchmark reports comparably."
  def opts(extra \\ []) do
    Keyword.merge(
      [
        time: 2,
        warmup: 0.5,
        memory_time: 0.5,
        print: [fast_warning: false]
      ],
      extra
    )
  end

  @doc "Prints a heading, so a combined run is readable."
  def section(title) do
    IO.puts("\n\n" <> String.duplicate("=", 72))
    IO.puts(title)
    IO.puts(String.duplicate("=", 72) <> "\n")
  end
end
