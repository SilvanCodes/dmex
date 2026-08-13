defmodule DMEx.Log.Plug do
  @moduledoc """
  Serves a log as an SSE stream.

  The HTTP half of `DMEx.Transport.SSE`: what a subscription bound to a URL
  connects to. A subscription in the same application reads the log directly and
  never comes here — see `DMEx.Transport.Local`.

  Mount it in a `Plug.Router`:

      forward "/stream", to: DMEx.Log.Plug, log_module: MyApp.Log

  The mount is a prefix `forward`, and two things hang off it:

    * **`GET /stream`** — the whole log, which is what a bare browser
      `EventSource` or `curl` asks for.
    * **`GET /stream/<blob>`** — the same log narrowed to the message types a
      consumer's filter admits. See `DMEx.Handler.Fingerprint` for the blob and
      `DMEx.Message.types/0` for what it is resolved against.

  [ARCHITECTURE](ARCHITECTURE.md)'s wire contract covers the rest of the URL
  vocabulary.

  ## `log_module:`, and not `log:`

  A log **module**, never a name from `DMEx.Supervisor`'s `logs:`. The two are
  different kinds of thing and the option says which: a `logs:` name is a
  *reader's* alias for a log, scoped to the reading application and deliberately
  transport-transparent, while this is the local resource the mount serves. A
  producer needs no alias for its own log — on the network it is identified by
  this mount's path and hostname.

  Which is also why a name could not be accepted here even if the words matched.
  Resolving one needs a running supervisor and its registry, and this runs at
  compile time (see `init/1`); a mount serving a log needs no `DMEx.Supervisor`
  at all today, since a log module carries its own supervision subtree. See
  [ARCHITECTURE](ARCHITECTURE.md) on why DMEx does not proxy.

  ## Selection is resolved once, at connect

  A filter cannot be pushed into a SQL index — membership is a computation, not
  something an index can seek on — so `DMEx.Log.Selection` expands it against
  the log's compiled message modules before the send loop starts.
  `DMEx.Message.types/0` covers what the resulting set is then used for.

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
  the connection. ARCHITECTURE's "the request process is the stream" covers why
  the process holding the socket has to be the one the loop runs in.

  Registration happens before anything that can block, and hands back the
  connection's `DMEx.Log.Status`, which puts even the connect path under
  `DMEx.Log.Watcher`. It is undone on the way out, whichever way that is: the
  request process is reused for the next keep-alive request, so an entry left
  behind is armed for a stall nothing will ever clear, and the watcher reclaims
  a connection that is by then serving somebody else. A read failure is the
  case that makes this more than a connect concern — the socket is healthy and
  the process returns normally.

  ## Response headers

  Beyond the ordinary SSE set: **`x-accel-buffering: no`**, nginx-specific and
  stopping it buffering the stream (see [the deployment guide](deployment.md)), and,
  on a
  selective stream only, **`dmex-matched-types`**, how many of the application's
  types the filter admitted — [ARCHITECTURE](ARCHITECTURE.md)'s wire contract covers
  what it is
  for.

  `connection: keep-alive` is set only on HTTP/1.1, where it means something; it
  is illegal on HTTP/2.

  ## Errors

  A `400` for a malformed `Last-Event-ID` or a path segment that is not a
  decodable filter. A `404` for a filter matching none of the application's
  message types. A `405` with `allow: GET` for any other method. A `503` when the
  connect cursor cannot be resolved — an absent `Last-Event-ID` whose
  `c:DMEx.Storage.max_id/1` query fails.

  There is no `503`/`Retry-After` *admission control* — see [ROADMAP](ROADMAP.md).
  """

  @behaviour Plug

  alias DMEx.Config
  alias DMEx.Log.Params
  alias DMEx.Log.Selection
  alias DMEx.Log.SendLoop
  alias DMEx.Log.Status
  alias DMEx.Log.Streams
  alias DMEx.Transport.SSE.Frame

  require Logger

  @matched "dmex-matched-types"

  @impl Plug
  def init(opts) do
    DMEx.validate_keys!(
      opts,
      [:log_module],
      "`DMEx.Log.Plug`",
      " A mount is `forward \"/stream\", to: DMEx.Log.Plug, log_module: MyApp.Log`. " <>
        "Pacing and stall settings belong on the log module — one log, one set.",
      [:log_module]
    )

    # Deliberately no database or registry access: under Phoenix's default
    # `init_mode` this runs at compile time.
    %{log: Keyword.fetch!(opts, :log_module)}
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
    case Params.parse(conn.path_info, conn.req_headers) do
      {:ok, params} -> select(conn, log, params, log.__dmex__().config)
      {:error, error} -> bad_request(conn, error)
    end
  end

  ## Selection

  # Expanded once, by `DMEx.Log.Selection`, which owns every question asked of
  # the result from here on. `size/1` is `nil` for a whole-log selection, which
  # is the one that can never be empty.
  defp select(conn, log, params, config) do
    selection = Selection.expand(params.selection)

    if Selection.size(selection) == 0 do
      not_found(conn)
    else
      stream(conn, log, params, config, selection)
    end
  end

  ## Streaming

  # The request process registers *itself*, so a stream's entry dies with it —
  # `DMEx.Log.SendLoop.start/2` does that and resolves the cursor under the
  # deadline the registration arms. The selection rides along for
  # `DMEx.Log.Dispatcher`'s routing; see `DMEx.Log.Streams` on what an entry
  # carries.
  defp stream(conn, log, params, config, selection) do
    case SendLoop.start(log, transport: DMEx.Transport.SSE, position: params.cursor, selection: selection) do
      {:ok, state} -> open(conn, config, selection, state)
      {:error, reason} -> unavailable(conn, reason)
    end
  after
    # Every path out of the stream, not just a failed connect: the entry
    # outliving the loop on a keep-alive process is a kill signal aimed at
    # whatever that process does next. A killed process needs nothing here —
    # `Registry` drops the entry with its owner.
    Streams.unregister(log)
  end

  defp open(conn, config, selection, state) do
    # Re-armed because resolving the cursor can query — an absent `Last-Event-ID`
    # costs a `max_id`, which takes a pool checkout like any other read. The
    # registration's arm covered that query; this one gives the header write that
    # follows its own full budget rather than what the query left.
    Status.arm(state.status, config.stall_timeout)

    conn =
      conn
      |> put_stream_headers(selection)
      |> Plug.Conn.send_chunked(200)

    case advertise_retry(conn, config, state.status) do
      {:ok, conn} -> SendLoop.loop(conn, state)
      {:error, _reason} -> conn
    end
  end

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

  defp bad_request(conn, error), do: respond(conn, 400, Params.describe(error))

  defp not_found(conn) do
    respond(conn, 404, "this filter matched none of the message types this application knows about")
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
