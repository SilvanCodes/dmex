defmodule DMEx.Log.Plug do
  @moduledoc """
  Serves a log as an SSE stream.

  The HTTP half of `DMEx.Transport.SSE`: what a subscription bound to a URL
  connects to. A subscription in the same application reads the log directly and
  never comes here — see `DMEx.Transport.Local`.

  Mount it in a `Plug.Router`:

      forward "/stream", to: DMEx.Log.Plug, log: MyApp.Log

  The mount is a prefix `forward`, and two things hang off it:

    * **`GET /stream`** — the whole log, which is what a bare browser
      `EventSource` or `curl` asks for.
    * **`GET /stream/<blob>`** — the same log narrowed to the message types a
      consumer's filter admits. See `DMEx.Handler.Fingerprint` for the blob and
      `DMEx.message_types/1` for what it is resolved against.

  There is no application segment and no query string: one mount serves exactly
  one log.

  ## Selection is resolved once, at connect

  A filter cannot be pushed into a SQL index — membership is a computation, not
  something an index can seek on — so `DMEx.Log.Selection` expands it against
  the log's compiled message modules before the send loop starts, turning it
  into the concrete type list the read path's `message_type = ANY($1)` wants.
  The resulting set also routes wakeups, so an append only disturbs the
  connections that asked for that type.

  **A filter that matches nothing is a `404`.** An empty expansion is
  indistinguishable on the wire from a healthy stream on a quiet log, so the
  alternative is a connection that opens and says nothing forever. It is not
  proof of a client bug — a consumer deployed ahead of the log that will emit
  its types is legitimately in this state — which is why DMEx's own consumer
  retries a `404` instead of stopping.

  Nothing consumer-supplied is validated beyond that, and nothing is atomized: a
  name the log has never heard of simply matches nothing.

  ## The request process *is* the stream

  No per-connection `GenServer` and no log-side supervisor: this plug registers
  the request process for wakeups and runs the send loop inline for the life of
  the connection. In both Bandit and Cowboy the connection process owns the
  socket, so writing chunks from another process is awkward at best — and
  reclaiming a stalled connection means killing whichever process is blocked in
  the write, so it had better be the one the connection dies with.

  Registration happens before anything that can block, and hands back the
  connection's `DMEx.Log.Status`, which puts even the connect path under
  `DMEx.Log.Watcher`. A connect that then fails unregisters: the request process
  is reused for the next keep-alive request, so its entry cannot be left to die
  with it.

  ## Response headers

  Beyond the ordinary SSE set:

    * **`x-accel-buffering: no`** — nginx-specific; stops it buffering the
      stream. See `guides/deployment.md`.
    * **`dmex-matched-messages`** — on a selective stream only, how many of the
      application's types the filter admitted.

  `connection: keep-alive` is set only on HTTP/1.1, where it means something; it
  is illegal on HTTP/2.

  ## Errors

  A `400` for a malformed `Last-Event-ID` or a path segment that is not a
  decodable filter. A `404` for a filter matching none of the application's
  message types. A `405` with `allow: GET` for any other method. A `503` when the
  connect cursor cannot be resolved — an absent `Last-Event-ID` whose
  `c:DMEx.Storage.max_id/1` query fails.

  There is no `503`/`Retry-After` *admission control* — see `ROADMAP.md`.
  """

  alias DMEx.Config
  alias DMEx.Log.Params
  alias DMEx.Log.Selection
  alias DMEx.Log.Status
  alias DMEx.Log.Stream, as: SendLoop
  alias DMEx.Log.Streams
  alias DMEx.Transport.SSE.Frame

  require Logger

  @behaviour Plug

  # Sent only on a selective stream, saying how many of the application's types
  # the filter admitted.
  @matched "dmex-matched-messages"

  @impl Plug
  def init(opts) do
    log =
      Keyword.get(opts, :log) ||
        raise ArgumentError,
              "DMEx.Log.Plug requires a `:log`, e.g. " <>
                "`forward \"/stream\", to: DMEx.Log.Plug, log: MyApp.Log`"

    # Deliberately no database or registry access: under Phoenix's default
    # `init_mode` this runs at compile time.
    %{log: log}
  end

  # Anything but GET is a `405` with the `allow` header RFC 9110 requires.
  # Without this a POST to the mount would open a chunked stream and sit there,
  # which is a confusing way to learn about a typo in a router.
  @impl Plug
  def call(%Plug.Conn{method: method} = conn, _opts) when method != "GET" do
    conn
    |> Plug.Conn.put_resp_header("allow", "GET")
    |> respond(405, "only GET is supported on a DMEx stream")
  end

  def call(conn, %{log: log}) do
    config = log.__dmex__().config

    case Params.parse(conn.path_info, conn.req_headers) do
      {:ok, params} -> select(conn, log, params, config)
      {:error, error} -> bad_request(conn, error)
    end
  end

  ## Selection

  # Expanded once, by `DMEx.Log.Selection`, which owns every question asked of
  # the result from here on. `size/1` is `nil` for a whole-log selection, which
  # is the one that can never be empty.
  defp select(conn, log, params, config) do
    selection = Selection.expand(log, params.selection)

    if Selection.size(selection) == 0 do
      not_found(conn)
    else
      stream(conn, log, params, config, selection)
    end
  end

  ## Streaming

  defp stream(conn, log, params, config, selection) do
    # The request process registers *itself*, so a stream's entry dies with it.
    # Before the first read, so an append landing in between is picked up by
    # that read; before the first *write*, so the connect path is under the
    # watcher too, since resolving a cursor and sending headers can block
    # exactly as the send loop can. The selection rides along so
    # `DMEx.Log.Dispatcher` can decide whether an append is worth waking this
    # connection for without messaging it.
    {:ok, status} = Streams.register(log, config.stall_timeout, selection)

    case resolve_cursor(log, params.cursor) do
      {:ok, cursor} ->
        open(conn, log, config, selection, status, cursor)

      {:error, reason} ->
        # The one path that registers and never becomes a stream, so the only
        # one that has to undo it: over HTTP/1.1 keep-alive this process is the
        # *connection* process and lives on to serve the next request. Left
        # behind, the entry is armed for a stall nothing will ever clear, and
        # the watcher reclaims a connection that never streamed.
        Streams.unregister(log)

        unavailable(conn, reason)
    end
  end

  defp open(conn, log, config, selection, status, cursor) do
    # Re-armed because `resolve_cursor/2` above can query — an absent
    # `Last-Event-ID` costs a `max_id`, which takes a pool checkout like any
    # other read. The registration's arm covers that query; this one gives the
    # header write that follows its own full budget rather than what the query
    # left.
    Status.arm(status, config.stall_timeout)

    conn =
      conn
      |> put_stream_headers(selection)
      |> Plug.Conn.send_chunked(200)

    # Announces the connect and builds the loop's state — the same call
    # `DMEx.Transport.Local` makes, so the two cannot report a connect
    # differently. Before `advertise_retry/3`, which may find the socket gone.
    state =
      SendLoop.connect(log,
        transport: DMEx.Transport.SSE,
        cursor: cursor,
        status: status,
        selection: selection
      )

    case advertise_retry(conn, config, status) do
      {:ok, conn} -> SendLoop.loop(conn, state)
      {:error, _reason} -> conn
    end
  end

  # The only cursor needing a query on the connect path: an absent
  # `Last-Event-ID` means "from now on", and the tip is the log's to name.
  #
  # An error is fatal to the connect rather than a fallback to 0. Falling back
  # would turn a pool timeout into a whole-log replay — and since every
  # subscription with no cursor yet arrives here, that is the default path, not
  # an exotic one. A 503 costs the consumer one jittered reconnect.
  defp resolve_cursor(log, :now) do
    %{storage: storage, storage_opts: opts} = log.__dmex__()

    storage.max_id(opts)
  end

  defp resolve_cursor(_log, cursor) when is_integer(cursor), do: {:ok, cursor}

  # A frame of its own at stream open, which dispatches nothing: an SSE parser
  # applies `retry:` the moment it parses the line, while an event is dispatched
  # only if the data buffer is non-empty when the blank line arrives.
  defp advertise_retry(conn, config, status) do
    Status.arm(status, config.stall_timeout)

    Plug.Conn.chunk(conn, Frame.encode_retry(Config.effective_heartbeat(config)))
  end

  defp put_stream_headers(conn, selection) do
    conn
    |> Plug.Conn.put_resp_content_type("text/event-stream")
    |> Plug.Conn.put_resp_header("cache-control", "no-cache")
    |> Plug.Conn.put_resp_header("x-accel-buffering", "no")
    |> put_keep_alive()
    |> put_matched(selection)
  end

  # `nil` is a whole-log stream, which sends no header at all.
  defp put_matched(conn, selection) do
    case Selection.size(selection) do
      nil -> conn
      matched -> Plug.Conn.put_resp_header(conn, @matched, Integer.to_string(matched))
    end
  end

  defp put_keep_alive(conn) do
    if Plug.Conn.get_http_protocol(conn) == :"HTTP/1.1" do
      Plug.Conn.put_resp_header(conn, "connection", "keep-alive")
    else
      conn
    end
  end

  defp bad_request(conn, error) do
    respond(conn, 400, Params.describe(error))
  end

  defp not_found(conn) do
    respond(
      conn,
      404,
      "this filter matched none of the message types this application knows about"
    )
  end

  # 503, not 500: the request was well-formed and the log's storage is momentarily
  # unable to answer it, which is exactly the case a consumer should retry.
  defp unavailable(conn, reason) do
    Logger.error("[dmex] could not resolve a connect cursor: #{inspect(reason)}")

    respond(conn, 503, "could not resolve the requested position; retry")
  end

  defp respond(conn, status, body) do
    conn
    |> Plug.Conn.put_resp_content_type("text/plain")
    |> Plug.Conn.send_resp(status, body)
    |> Plug.Conn.halt()
  end
end
