defmodule DMEx.Transport do
  @moduledoc """
  How rows get from a log to a handler — the two ends of one wire, or of none.

  `DMEx.Log.SendLoop` reads, paces and tracks a cursor; a transport decides what
  is *done* with each read's rows. `DMEx.Handler.Subscription` owns a durable cursor,
  a pipeline and its checkpointing; a transport decides what *produces* the rows
  it runs. Everything protocol-specific — framing, sockets, decoding, liveness,
  the id chain — lives behind these callbacks.

  Both ends are on one module on purpose: they are the two halves of one wire
  format, and the encoder and the parser have to agree byte for byte. One
  behaviour puts the thing they agree *on* next to both of its users —
  `DMEx.Transport.SSE.Frame` is written by `write/2` and read by the reader
  `c:start_reader/1` starts, and splitting the callbacks would put a grammar in
  one module and its only other user in another, free to drift.

  Nothing here guards against *mismatched* ends being paired, because nothing
  can pair them: a transport is inferred from a `logs:` binding, never
  configured — see `DMEx.Supervisor`.

  ## The receive side is a process

  `c:start_reader/1` spawns a process linked to the subscription. It connects,
  reads, and hands each batch over with `deliver/2` — a blocking call, so a
  reader is *inside* the pipeline's run and reads nothing meanwhile.

  Everything that can go wrong is an exit. So a subscription has one failure
  path — a linked process died — rather than a vocabulary of return values, and
  a connection ending mid-batch cannot take back the rows it already handed
  over, because the handover was a call that returned. There is no way to
  express "here are rows *and* the connection is finished" and therefore no way
  to lose the first half of it.

  ## What a transport must preserve

  **No buffer.** At most one batch may be in flight, and the reader must not
  produce the next until `deliver/2` returns — ARCHITECTURE invariant 8. A
  transport that queues rows for later has replaced a structural guarantee with
  a bound somebody has to choose.

  **Rows are handled in the process that owns the cursor.** `deliver/2` hands
  them to the subscription and they are run there. A reader running the pipeline
  itself would put the handler callback and the cursor advance in different
  processes.
  """

  @typedoc "A transport's send-side state — a `Plug.Conn`, a pid, whatever it needs."
  @type sink :: term()

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

  Unmeasured on purpose, unlike `c:write/2`: `[:dmex, :stream, :heartbeat]`
  carries no measurements at all, so there is no third element to return.
  """
  @callback heartbeat(sink()) :: {:ok, sink()} | {:error, term()}

  @doc """
  Starts this subscription's reader and returns its pid.

  Called inside `DMEx.Handler.Subscription`, and **must `spawn_link`**: the link
  is what makes the reader die with the subscription and its death arrive as an
  exit the subscription acts on.

  Takes the subscription's whole option list and uses what it needs, plus
  `:cursor` — where to resume — and `:owner`, the pid `deliver/2` and
  `connected/1` are called with.

  `:origin` is the option every transport reads, and it is deliberately the
  *general* name: only the transport knows whether the binding resolved to a URL
  or a log module, so each narrows it on the way in — `DMEx.Transport.SSE` keeps
  a `url`, `DMEx.Transport.Local` a `log`. That is specialization rather than
  drift; renaming either to `origin` would trade a name that says what the value
  *is* for one that says where the slot came from, and renaming `:origin` itself
  to `url` would be wrong for the local half, where it is a module.

  Failing to connect is an ordinary exit from the reader rather than a return
  value here, so opening a connection and losing one are the same event to the
  subscription.
  """
  @callback start_reader(keyword()) :: pid()

  ## Calling a transport

  # As in `DMEx.Storage`: nothing calls a transport module directly. The receive
  # side dispatches on the `:transport` in the subscription's options — the same
  # list a reader is started from, so the module travels with the value it acts
  # on. The send side takes it as its own argument instead, because a sink is a
  # `Plug.Conn` or a bare pid and cannot carry one.

  @doc "Starts the reader named by `opts`. See `c:start_reader/1`."
  @spec start_reader(keyword()) :: pid()
  def start_reader(opts), do: Keyword.fetch!(opts, :transport).start_reader(opts)

  @doc "Sends one read's rows. See `c:write/2`."
  @spec write(module(), sink(), [DMEx.Storage.row()]) :: {:ok, sink(), map()} | {:error, term()}
  def write(transport, sink, rows), do: transport.write(sink, rows)

  @doc "Signals liveness on an idle stream. See `c:heartbeat/1`."
  @spec heartbeat(module(), sink()) :: {:ok, sink()} | {:error, term()}
  def heartbeat(transport, sink), do: transport.heartbeat(sink)

  @doc """
  Hands one read's rows to the subscription, and waits for it to run them.

  The reader's whole output. Blocking is the backpressure: the call returns when
  the last row has been through `DMEx.Handler.Pipeline`, so a reader that reads
  again on return can never be ahead of the handler by more than the batch it
  already gave away.
  """
  @spec deliver(pid(), [DMEx.Handler.Pipeline.decoded_row()]) :: :ok
  def deliver(owner, rows), do: GenServer.call(owner, {:rows, rows}, :infinity)

  @doc """
  Announces that this reader's connection is established.

  What clears the subscription's reconnect backoff, so it must be sent once the
  connection is genuinely up — after the request is on the wire, after the
  registration is made — and never before. A reader that dies without sending it
  is a failed attempt, and the next one waits longer.
  """
  @spec connected(pid()) :: :ok
  def connected(owner), do: GenServer.cast(owner, :connected)

  @doc """
  Ends the reader on a failure that reconnecting would only repeat.

  The subscription stops with `reason` rather than retrying — a request this
  client got wrong is the case, where an unreachable log is not. Every other
  exit reason is an ordinary disconnect.
  """
  @spec fatal(term()) :: no_return()
  def fatal(reason), do: exit({:fatal, reason})
end
