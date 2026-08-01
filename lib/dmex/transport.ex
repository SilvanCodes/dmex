defmodule DMEx.Transport do
  @moduledoc """
  How rows get from a log to a handler — the two ends of one wire, or of none.

  `DMEx.Log.Stream` reads, paces and tracks a cursor; a transport decides what
  is *done* with each read's rows. `DMEx.Handler.Stream` owns a durable cursor,
  a pipeline and its checkpointing; a transport decides what *produces* the rows
  it runs. Everything protocol-specific — framing, sockets, decoding, liveness,
  the id chain — lives behind these callbacks.

  Both ends are on one module on purpose: they are the two halves of one wire
  format, and splitting them would make it possible to configure a sender no
  receiver understands. A transport is inferred from a `logs:` binding, never
  configured — see `DMEx.Supervisor`.

  ## What a transport must preserve

  **No buffer.** At most one batch may be in flight, and producing the next must
  be blocked until the current one has been handled. A transport that queues
  rows for later has replaced a structural guarantee with a bound somebody has
  to choose.

  **Rows are handled in the process that owns the cursor.** `c:handle_message/2`
  runs inside `DMEx.Handler.Stream` and its rows are run inline there. A
  transport running the pipeline itself would put the handler callback and the
  cursor advance in different processes.
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

  It must not return until the rows have been taken, since the loop reads again
  as soon as it does.
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

  Takes the subscription's whole option list and uses what it needs. Runs inside
  `DMEx.Handler.Stream`, so anything it starts is linked to the process that
  owns the cursor.
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

  After running rows the stream calls this again with **`:continue`**, asking
  whether one message carried anything beyond them. A transport that can produce
  rows *and* a terminal event from a single message — an HTTP response ending
  right after its last chunk — parks the terminal and returns it then, so it
  never discards rows already verified. One with nothing to park answers
  `:unknown`.
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
