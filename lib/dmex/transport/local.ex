defmodule DMEx.Transport.Local do
  @moduledoc """
  Reading a log this application owns, in this VM.

  Chosen for a `logs:` entry that is a module rather than a URL. There is no
  wire, so there is no framing, no fingerprint and no heartbeat — only a read
  and a handler.

  **It is the same send loop.** `connect/1` starts a linked process that
  registers with `DMEx.Log.Registry` — the same registration a request process
  serving SSE makes — and runs `DMEx.Log.Stream.loop/2` with this module as its
  transport. So a local subscription gets `LISTEN`/`NOTIFY` wakeups,
  selection-based routing, pacing and `DMEx.Log.Watcher` reclaim by construction
  rather than reimplementation. Only `c:DMEx.Transport.write/2` differs.

  **Backpressure is a call, not a window.** `write/2` is a `GenServer.call/3`
  whose reply comes only after the pipeline has run the whole batch, so the loop
  reads nothing meanwhile. A handler that never returns therefore wedges its
  reader rather than the subscription, and the watcher reclaims it like any
  other stalled stream.

  **Selection is a list, not a filter.** A remote subscription encodes its
  message types into a `DMEx.Fingerprint` blob because they cross a wire and are
  expanded on the other side. Here both ends are the same node, so the declared
  modules go straight into the read as a type list.
  """

  @behaviour DMEx.Transport

  alias DMEx.Handler
  alias DMEx.Log.Stream, as: SendLoop

  ## Send side — the loop process writes into its owner

  @impl DMEx.Transport
  def write(%{owner: owner} = sink, rows) do
    :ok = GenServer.call(owner, {:rows, Enum.map(rows, &row/1)}, :infinity)

    {:ok, sink, %{}}
  catch
    # The owner went away mid-batch — unsubscribed, or shut down. There is
    # nothing to hand rows to any more, so the loop ends exactly as a closed
    # socket ends one.
    :exit, reason -> {:error, reason}
  end

  @impl DMEx.Transport
  def heartbeat(sink), do: {:ok, sink}

  # The pipeline takes the shape `Frame.parse/1` yields, so this decodes `data`
  # and `metadata` and nothing else.
  defp row(row) do
    %{
      "id" => row.id,
      "message_type" => row.message_type,
      "data" => JSON.decode!(row.data),
      "metadata" => JSON.decode!(row.metadata)
    }
  end

  ## Receive side — the subscription's view of that process

  @impl DMEx.Transport
  def connect(opts) do
    log = Keyword.fetch!(opts, :origin)
    owner = Keyword.fetch!(opts, :owner)
    config = log.__dmex__().config
    selection = selection(opts)

    cursor = resolve_cursor(log, Keyword.fetch!(opts, :cursor), Keyword.fetch!(opts, :config))

    reader =
      spawn_link(fn ->
        # Registered by the reader itself, so the entry dies with it — the same
        # reason the SSE side registers the request process.
        {:ok, status} = DMEx.Log.Registry.register(log, config.stall_timeout, selection)

        :telemetry.execute(
          [:dmex, :stream, :connect],
          %{cursor: cursor, matched: matched(selection)},
          %{log: log, selective: selection != :all}
        )

        SendLoop.loop(
          %{owner: owner},
          SendLoop.initial_state(log,
            transport: __MODULE__,
            cursor: cursor,
            status: status,
            selection: selection
          )
        )
      end)

    {:ok, %{log: log, reader: reader}}
  end

  # The reader is linked, so its death arrives as an exit the owner traps.
  @impl DMEx.Transport
  def handle_message({:EXIT, reader, reason}, %{reader: reader} = source) do
    {:disconnect, reason, source}
  end

  def handle_message(_message, _source), do: :unknown

  @impl DMEx.Transport
  def close(%{reader: reader}) do
    if Process.alive?(reader) do
      Process.unlink(reader)
      Process.exit(reader, :shutdown)
    end

    :ok
  end

  @impl DMEx.Transport
  def liveness(_source), do: :infinity

  ## Resolution

  # The same three answers `DMEx.Handler.Request` puts in a `last-event-id`
  # header and `DMEx.Log.Plug` resolves back out, without the round trip.
  defp resolve_cursor(log, :none, %{start: :latest}) do
    %{storage: storage, storage_opts: storage_opts} = log.__dmex__()

    case storage.max_id(storage_opts) do
      {:ok, max_id} -> max_id
      {:error, _reason} -> 0
    end
  end

  defp resolve_cursor(_log, :none, %{start: start}) when is_integer(start), do: start
  defp resolve_cursor(_log, :none, _config), do: 0
  defp resolve_cursor(_log, cursor, _config) when is_integer(cursor), do: cursor

  defp selection(opts) do
    declaration = opts |> Keyword.fetch!(:handler) |> Handler.declaration!()

    if Keyword.get(opts, :fingerprint) do
      MapSet.new(declaration.messages, & &1.type())
    else
      # A `nil` fingerprint is what `selective: false` compiles to, and it means
      # the same thing here as it does on the wire: the whole log.
      :all
    end
  end

  defp matched(:all), do: nil
  defp matched(selection), do: MapSet.size(selection)
end
