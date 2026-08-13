defmodule DMEx.Transport.Local do
  @moduledoc """
  Reading a log this application owns, in this VM.

  Chosen for a `bindings:` entry that is a module rather than a URL. There is no
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
  alias DMEx.Transport

  ## Send side — the loop process writes into its owner

  @impl Transport
  def write(%{owner: owner} = sink, rows) do
    :ok = Transport.deliver(owner, Enum.map(rows, &row/1))

    {:ok, sink, %{}}
  catch
    # The owner went away mid-batch — unsubscribed, or shut down. The link has
    # usually killed this process before it matters; what this covers is the
    # owner stopping *normally*, whose exit signal a linked process ignores. So
    # the loop ends exactly as a closed socket ends one, rather than the reader
    # dying of a call to a process that is already gone.
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

    selection =
      opts
      |> Keyword.fetch!(:handler)
      |> Handler.declaration!()
      |> Map.fetch!(:messages)
      |> Selection.declared(Keyword.get(opts, :fingerprint))

    # The same ceremony `DMEx.Log.Plug` performs, from the same function: the
    # reader registers itself so the entry dies with it, and the position is
    # resolved under the deadline that registration arms.
    #
    # `DMEx.Handler.Request.position/2` is what turns a subscription's durable
    # cursor into the two answers a connect can ask for, and `DMEx.Log.Plug`
    # resolves a `last-event-id` header through the same pair — so neither side
    # spells "from now on" differently, without the round trip.
    position = Request.position(Keyword.fetch!(opts, :cursor), Keyword.fetch!(opts, :config))

    case SendLoop.start(log, transport: __MODULE__, position: position, selection: selection) do
      {:ok, state} ->
        Transport.connected(owner)

        SendLoop.loop(%{owner: owner}, state)

      # Failing to read the tip ends the reader, which the subscription answers
      # with its jittered retry — the plug's 503 with the other end's.
      {:error, reason} ->
        exit(reason)
    end
  end
end
