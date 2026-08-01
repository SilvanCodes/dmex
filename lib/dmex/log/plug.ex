defmodule DMEx.Log.Plug do
  @moduledoc """
  Serves a log as an SSE stream.

  The HTTP half of `DMEx.Transport.SSE`: what a subscription bound to a URL
  connects to. A subscription in the same application reads the log directly and
  never comes here — see `DMEx.Transport.Local`.

  Mount it wherever you mount plugs — a Phoenix router, a `Plug.Router`, or a
  bare Bandit/Cowboy pipeline:

      forward "/stream", to: DMEx.Log.Plug, init_opts: [log: MyApp.Log]

  The mount is a prefix `forward`, and two things hang off it:

    * **`GET /stream`** — the whole log, which is what a bare browser
      `EventSource` or `curl` asks for.
    * **`GET /stream/<blob>`** — the same log narrowed to the message types a
      consumer's filter admits. See `DMEx.Fingerprint` for the blob and
      `DMEx.Log.Messages` for what it is resolved against.

  There is no application segment and no query string: a log application owns
  exactly one log, and its hostname already identifies the app.

  ## Selection is resolved once, at connect

  A filter cannot be pushed into a SQL index — membership is a computation, not
  something an index can seek on — so the plug expands it against the log's
  compiled message modules before the send loop starts, turning it into the
  concrete type list the read path's `message_type = ANY($1)` predicate wants.
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
  `DMEx.Log.Watcher`.

  ## Response headers

  Beyond the ordinary SSE set:

    * **`x-accel-buffering: no`** — nginx proxies with `proxy_buffering on` by
      default, holding response bytes until its buffer fills. On an SSE stream
      that converts pacing into "whenever the buffer happens to fill" and
      swallows heartbeats, so a healthy stream looks dead to both the consumer
      and the proxy's idle timeout. nginx-specific and ignored elsewhere.
    * **`dmex-matched-messages`** — on a selective stream only, how many of the
      log's types the filter admitted.

  `connection: keep-alive` is set only on HTTP/1.1, where it means something; it
  is illegal on HTTP/2.

  ## Errors

  A `400` for a malformed `Last-Event-ID` or a path segment that is neither a
  reserved name nor a decodable filter. A `404` for a reserved sub-resource that
  is not implemented, or a filter matching none of this log's message types.

  There is no `503`/`Retry-After` admission control: DMEx has no aggregate
  budget to enforce one against. `DMEx.Log.Watcher` is where one would be
  enforced if it existed, and this is where it would be asked, before
  registering. See `ROADMAP.md`.
  """

  alias DMEx.Config
  alias DMEx.Fingerprint
  alias DMEx.Log.Messages
  alias DMEx.Log.Params
  alias DMEx.Log.Status
  alias DMEx.Log.Stream, as: SendLoop
  alias DMEx.Transport.SSE.Frame

  @behaviour Plug

  @impl Plug
  def init(opts) do
    log =
      Keyword.get(opts, :log) ||
        raise ArgumentError,
              "DMEx.Log.Plug requires a `:log`, e.g. " <>
                "`forward \"/stream\", to: DMEx.Log.Plug, " <>
                "init_opts: [log: MyApp.Log]`"

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

    # `conn.path_info` is what the mount handed us — Plug's `forward` strips the
    # prefix — so the one-line `forward "/stream", to: DMEx.Log.Plug` mount
    # routes `/stream/<blob>` with no routing change at all.
    case Params.parse(conn.path_info, conn.req_headers) do
      {:ok, params} -> select(conn, log, params, config)
      {:error, {:reserved, _name} = error} -> not_found(conn, Params.describe(error))
      {:error, error} -> bad_request(conn, error)
    end
  end

  ## Selection

  # Expanded once against the log's compiled message modules rather than against
  # what its log happens to contain — see `DMEx.Log.Messages` for why those are
  # not interchangeable.
  defp select(conn, log, %{selection: :all} = params, config) do
    stream(conn, log, params, config, :all)
  end

  defp select(conn, log, params, config) do
    selection =
      log
      |> Messages.types()
      |> Enum.filter(&Fingerprint.member?(params.selection, &1))
      |> MapSet.new()

    if MapSet.size(selection) == 0 do
      # Deliberately loud, and retryable rather than fatal — see the moduledoc.
      not_found(
        conn,
        "this filter matched none of the message types #{inspect(log)} knows about"
      )
    else
      stream(conn, log, params, config, selection)
    end
  end

  ## Streaming

  defp stream(conn, log, params, config, selection) do
    # The request process registers *itself*, so the entry dies with the
    # connection. Before the first read, so an append landing in between is
    # picked up by that read; before the first *write*, so the connect path is
    # under the watcher too, since resolving a cursor and sending headers can
    # block exactly as the send loop can. The selection rides along so
    # `DMEx.Log.Dispatcher` can decide whether an append is worth waking this
    # connection for without messaging it.
    {:ok, status} =
      DMEx.Log.Registry.register(log, config.stall_timeout, selection)

    cursor = resolve_cursor(log, params.cursor)

    Status.arm(status, config.stall_timeout)

    conn =
      conn
      |> put_stream_headers(selection)
      |> Plug.Conn.send_chunked(200)

    :telemetry.execute(
      [:dmex, :stream, :connect],
      %{cursor: cursor, matched: matched(selection)},
      %{log: log, selective: selection != :all}
    )

    case advertise_retry(conn, config, status) do
      {:ok, conn} ->
        SendLoop.loop(
          conn,
          SendLoop.initial_state(log,
            transport: DMEx.Transport.SSE,
            cursor: cursor,
            status: status,
            selection: selection
          )
        )

      {:error, _reason} ->
        conn
    end
  end

  # The only cursor needing a query on the connect path, and what lets a
  # consumer skip history without a preflight request.
  defp resolve_cursor(log, :latest) do
    %{storage: storage, storage_opts: opts} = log.__dmex__()

    case storage.max_id(opts) do
      {:ok, max_id} -> max_id
      {:error, _reason} -> 0
    end
  end

  defp resolve_cursor(_log, cursor) when is_integer(cursor), do: cursor

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
    |> put_resp_headers(matched_headers(selection))
  end

  defp matched_headers(:all), do: []
  defp matched_headers(selection), do: Params.matched_header(MapSet.size(selection))

  defp matched(:all), do: nil
  defp matched(selection), do: MapSet.size(selection)

  defp put_keep_alive(conn) do
    if Plug.Conn.get_http_protocol(conn) == :"HTTP/1.1" do
      Plug.Conn.put_resp_header(conn, "connection", "keep-alive")
    else
      conn
    end
  end

  defp put_resp_headers(conn, headers) do
    Enum.reduce(headers, conn, fn {name, value}, acc ->
      Plug.Conn.put_resp_header(acc, name, value)
    end)
  end

  defp bad_request(conn, error) do
    respond(conn, 400, Params.describe(error))
  end

  defp not_found(conn, body) do
    respond(conn, 404, body)
  end

  defp respond(conn, status, body) do
    conn
    |> Plug.Conn.put_resp_content_type("text/plain")
    |> Plug.Conn.send_resp(status, body)
    |> Plug.Conn.halt()
  end
end
