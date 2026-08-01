defmodule DMEx.Transport do
  @moduledoc """
  How rows get from a log to a handler — the two ends of one wire, or of none.

  `DMEx.Log.Stream` reads, paces and tracks a cursor; a transport decides what
  is *done* with each read's rows. `DMEx.Handler.Stream` owns a durable cursor,
  a pipeline and its checkpointing; a transport decides what *produces* the rows
  it runs. Everything protocol-specific — framing, sockets, decoding, liveness,
  the id chain — lives behind these callbacks.

  Both ends are on one module on purpose: they are the two halves of one wire
  format, and splitting them into separately-named modules would make it
  possible to configure a sender that no receiver understands.

  ## A transport is never configured, only inferred

  A `logs:` entry resolving to a **module** is a log this application owns, so
  it is read in this VM (`DMEx.Transport.Local`). One resolving to a **URL** is
  someone else's, so it is read over HTTP (`DMEx.Transport.SSE`). There is no
  `transport:` option anywhere, because the binding already says everything an
  option could: an application does not get to read its own log over a socket,
  and cannot read a URL any other way.

  ## What a transport must preserve

  **No buffer.** DMEx's backpressure is structural rather than monitored: at most
  one batch may be in flight, and producing the next one must be blocked until
  the current one has been handled. `DMEx.Transport.SSE` gets this from TCP —
  while the pipeline runs, the socket is not re-armed and the log's `chunk/2`
  blocks. `DMEx.Transport.Local` gets it from a synchronous call. A transport
  that queues rows to be picked up later has replaced a structural guarantee
  with a bound somebody has to choose.

  **Rows are handled in the process that owns the cursor.** `c:handle_message/2`
  runs inside `DMEx.Handler.Stream`, and the rows it returns are run inline
  there. A transport that ran the pipeline itself would put the handler callback
  and the cursor advance in different processes.
  """

  @typedoc "A transport's send-side state — a `Plug.Conn`, a pid, whatever it needs."
  @type sink :: term()

  @typedoc "A transport's receive-side state."
  @type source :: term()

  @doc """
  Sends one read's rows, and reports whatever it can measure about the write.

  The measurements are merged into `[:dmex, :stream, :write]`'s, on top of the
  row count the loop already knows — `%{bytes: n}` where bytes are a meaningful
  quantity, `%{}` where they are not.

  **This call is where backpressure lives.** It must not return until the rows
  have been taken, since the loop reads again as soon as it does.
  """
  @callback write(sink(), [DMEx.Storage.row()]) ::
              {:ok, sink(), map()} | {:error, term()}

  @doc """
  Signals liveness on a stream that has been idle for a whole heartbeat interval.

  A no-op for a transport whose reader cannot mistake silence for death.
  """
  @callback heartbeat(sink()) :: {:ok, sink()} | {:error, term()}

  @doc """
  Opens the receive side at `cursor`, or fails so the caller can back off.

  Takes the subscription's whole option list — `:handler`, `:subscription`,
  `:origin`, `:cursor`, `:config`, `:fingerprint`, `:storage`, `:storage_opts` —
  and uses what it needs. Runs inside `DMEx.Handler.Stream`, so anything it
  starts is linked to the process that owns the cursor.
  """
  @callback connect(keyword()) :: {:ok, source()} | {:error, term()}

  @doc """
  Interprets one process message.

    * `{:rows, rows, source}` — rows to run, in order, right now.
    * `{:noreply, source}` — consumed, nothing to run.
    * `:unknown` — not this transport's message.
    * `{:disconnect, reason, source}` — this connection is over; the stream
      flushes, closes and reconnects on its backoff schedule.
    * `{:stop, reason}` — fatal. Reconnecting would repeat whatever caused it.

  After running rows the stream calls this again with **`:continue`**, which
  asks whether one message carried anything beyond them. A transport that can
  produce rows and a terminal event from a single message — an HTTP response
  ending right after its last chunk — parks the terminal and returns it then,
  so it never discards rows that were already verified. One with nothing to
  park answers `:unknown` and the drain ends there.
  """
  @callback handle_message(term(), source()) ::
              {:rows, [DMEx.Handler.Pipeline.row()], source()}
              | {:noreply, source()}
              | :unknown
              | {:disconnect, term(), source()}
              | {:stop, term()}

  @doc "Releases whatever `c:connect/1` opened."
  @callback close(source()) :: :ok

  @doc """
  How long total silence may last before this connection is presumed dead, in ms.

  `:infinity` arms no liveness timer at all, which is the honest answer for a
  transport with no connection to lose.
  """
  @callback liveness(source()) :: pos_integer() | :infinity
end
