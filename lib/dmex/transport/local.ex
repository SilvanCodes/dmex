defmodule DMEx.Transport.Local do
  @moduledoc """
  Reading a log this application owns, in this VM.

  Chosen for a `logs:` entry that is a module rather than a URL. There is no
  wire, so there is no framing and no fingerprint — only a read and a handler.
  The loop's heartbeat branch still runs; `heartbeat/1` is a no-op because there
  is no reader that could mistake silence for death.

  **It is the same send loop.** `start_reader/1` starts a linked process that
  registers with `DMEx.Log.Streams` — the same registration a request process
  serving SSE makes — and runs `DMEx.Log.SendLoop.loop/2` with this module as its
  transport. So a local subscription gets `LISTEN`/`NOTIFY` wakeups,
  selection-based routing, pacing and `DMEx.Log.Watcher` reclaim by construction
  rather than reimplementation. Only `c:DMEx.Transport.write/2` and
  `c:DMEx.Transport.heartbeat/1` differ.

  **Backpressure is a call, not a window.** `write/2` is
  `DMEx.Transport.deliver/2`, whose reply comes only after the pipeline has run
  the whole batch, so the loop reads nothing meanwhile. The pipeline runs in the
  *subscription* process, so a handler that never returns wedges that process
  first and the reader behind it. `DMEx.Log.Watcher` reclaims the reader, which
  stops the log holding a registration and a deadline for a stream that is not
  draining — the same thing it does to a stalled request process.

  **Selection is a list, not a filter.** Both ends are the same node, so the
  handler's declared modules go straight into the read as a type list, where a
  remote subscription would encode a `DMEx.Handler.Fingerprint` blob and expand
  it on the other side — `DMEx.Log.Selection` owns both sets and says how they
  differ.
  """

  @behaviour DMEx.Transport

  alias DMEx.Handler
  alias DMEx.Handler.Request
  alias DMEx.Log.Selection
  alias DMEx.Log.SendLoop
  alias DMEx.Log.Streams
  alias DMEx.Storage
  alias DMEx.Transport

  ## Send side — the loop process writes into its owner

  @impl Transport
  def write(%{owner: owner} = sink, rows) do
    :ok = Transport.deliver(owner, Enum.map(rows, &row/1))

    {:ok, sink, %{}}
  catch
    # The owner went away mid-batch — unsubscribed, or shut down. There is
    # nothing to hand rows to any more, so the loop ends exactly as a closed
    # socket ends one.
    :exit, reason -> {:error, reason}
  end

  @impl Transport
  def heartbeat(sink), do: {:ok, sink}

  # The pipeline reads `id`, `type`, `data` and `metadata` — the same
  # shape a parsed frame has, minus the trip through JSON text.
  defp row(%{id: id, type: type, data: data, metadata: metadata}) do
    %{
      "id" => id,
      "type" => type,
      "data" => JSON.decode!(data),
      "metadata" => JSON.decode!(metadata)
    }
  end

  ## Receive side — the reader is that send loop, run for one subscription

  @impl Transport
  def start_reader(opts), do: spawn_link(fn -> read(opts) end)

  defp read(opts) do
    log = Keyword.fetch!(opts, :origin)
    owner = Keyword.fetch!(opts, :owner)
    config = log.__dmex__().config

    selection =
      opts
      |> Keyword.fetch!(:handler)
      |> Handler.declaration!()
      |> Map.fetch!(:messages)
      |> Selection.declared(Keyword.get(opts, :fingerprint))

    cursor = cursor!(log, Keyword.fetch!(opts, :cursor), Keyword.fetch!(opts, :config))

    # Registered by the reader itself, so the entry dies with it — the same
    # reason the SSE side registers the request process.
    {:ok, status} = Streams.register(log, config.stall_timeout, selection)

    Transport.connected(owner)

    # Announces the connect and builds the loop's state — the same call
    # `DMEx.Log.Plug` makes, so the two cannot report a connect differently.
    SendLoop.loop(
      %{owner: owner},
      SendLoop.connect(log,
        transport: __MODULE__,
        cursor: cursor,
        status: status,
        selection: selection
      )
    )
  end

  ## Resolution

  # The same two answers `DMEx.Handler.Request` puts in a `last-event-id` header
  # and `DMEx.Log.Plug` resolves back out, without the round trip — and from the
  # same function, so neither side spells the position differently.
  #
  # An error ends the reader rather than falling back to 0, which would turn a
  # momentary pool exhaustion into a whole-log replay. The subscription retries
  # on its own jittered schedule; the log's plug answers the same case with 503.
  defp cursor!(log, cursor, config) do
    case Request.position(cursor, config) do
      :now ->
        case Storage.max_id(log.__dmex__().storage) do
          {:ok, cursor} -> cursor
          {:error, reason} -> exit(reason)
        end

      cursor ->
        cursor
    end
  end
end
