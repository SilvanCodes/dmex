defmodule DMEx.Log.Plug do
  @moduledoc """
  Serves a log as an SSE stream.

  The HTTP half of `DMEx.Transport.SSE`: what a subscription bound to a URL
  connects to. A subscription in the same application reads the log directly and
  never comes here — see `DMEx.Transport.Local`.

  Mount it in a `Plug.Router`:

      forward "/stream", to: DMEx.Log.Plug, log_module: MyApp.Log

  A prefix `forward`: `GET /stream` is the whole log, and `GET /stream/<blob>` the
  same log narrowed to the message types a consumer's filter admits — see
  `DMEx.Handler.Fingerprint` for the blob, and
  [ARCHITECTURE](ARCHITECTURE.md)'s wire contract for the rest of the URL
  vocabulary.

  ## `log_module:`, and not `log:`

  A log **module**, never a name from `DMEx.Supervisor`'s `bindings:`: a bound
  name is a *reader's* alias for a log, and resolving one needs a running
  supervisor and its registry where this runs at compile time (see `init/1`).

  ## Selection is resolved once, at connect

  Membership in a filter is a computation, not something a SQL index can seek on,
  so `DMEx.Log.Selection` expands it before the send loop starts. Nothing
  consumer-supplied is validated beyond that, and nothing is atomized: a name the
  log has never heard of simply matches nothing.

  **A filter that matches nothing is a `404`.** An empty expansion is
  indistinguishable on the wire from a healthy stream on a quiet log, so the
  alternative is a connection that opens and says nothing forever. It is not
  proof of a client bug — a consumer deployed ahead of the log that will emit
  its types is legitimately in this state — which is why DMEx's own consumer
  retries a `404` instead of stopping.

  ## The request process *is* the stream

  ARCHITECTURE's "the request process is the stream" covers why the loop runs
  inline in the process holding the socket.

  Registration happens before anything that can block, and hands back the
  connection's `DMEx.Log.Status`, which puts even the connect path under
  `DMEx.Log.Watcher`. It is undone on the way out, whichever way that is: the
  request process is reused for the next keep-alive request, so an entry left
  behind is armed for a stall nothing will ever clear, and the watcher reclaims
  a connection that is by then serving somebody else. A read failure is the
  case that makes this more than a connect concern — the socket is healthy and
  the process returns normally.

  ## Response headers and errors

  Beyond the ordinary SSE set: **`x-accel-buffering: no`**, so nginx does not
  buffer the stream (see [the deployment guide](deployment.md)), and, on a
  selective stream only, **`dmex-matched-types`**, how many of the application's
  types the filter admitted. `connection: keep-alive` goes out on HTTP/1.1 only,
  where it means something; it is illegal on HTTP/2.

  A `400` for a malformed `Last-Event-ID` or an undecodable filter segment, a
  `404` for a filter matching nothing, a `405` with `allow: GET` for anything but
  GET, and a `503` when the connect cursor cannot be resolved. There is no
  `503`/`Retry-After` *admission control* — see [ROADMAP](ROADMAP.md).
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

  # Plug is an optional dependency, so a host that serves no log — a pure
  # consumer — compiles this module without it. Nothing here may *expand* a Plug
  # construct for that to hold, which is why `call/2` matches a bare map below
  # rather than `%Plug.Conn{}`: a struct pattern needs the module at compile time
  # where a remote call only needs it at runtime.
  @compile {:no_warn_undefined, [Plug, Plug.Conn]}

  @matched "dmex-matched-types"

  @impl Plug
  def init(opts) do
    Config.keys!(
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
  @impl Plug
  def call(%{method: method} = conn, _opts) when method != "GET" do
    conn
    |> Plug.Conn.put_resp_header("allow", "GET")
    |> respond(405, "only GET is supported on a DMEx stream")
  end

  def call(conn, %{log: log}) do
    case Params.parse(conn.path_info, conn.req_headers) do
      {:ok, params} -> select(conn, log, params)
      {:error, error} -> bad_request(conn, error)
    end
  end

  ## Selection

  # Expanded once, by `DMEx.Log.Selection`, which owns every question asked of
  # the result from here on. `size/1` is `nil` for a whole-log selection, which
  # is the one that can never be empty.
  defp select(conn, log, params) do
    selection = Selection.expand(params.selection)

    if Selection.size(selection) == 0 do
      not_found(conn)
    else
      stream(conn, log, params, selection)
    end
  end

  ## Streaming

  # The request process registers *itself*, so a stream's entry dies with it —
  # `DMEx.Log.SendLoop.start/2` does that under the deadline the registration
  # arms; see `DMEx.Log.Streams` on what an entry carries.
  defp stream(conn, log, params, selection) do
    case SendLoop.start(log, transport: DMEx.Transport.SSE, position: params.cursor, selection: selection) do
      {:ok, state} -> open(conn, selection, state)
      {:error, reason} -> unavailable(conn, reason)
    end
  after
    # Every path out of the stream, not just a failed connect: the entry
    # outliving the loop on a keep-alive process is a kill signal aimed at
    # whatever that process does next. A killed process needs nothing here —
    # `Registry` drops the entry with its owner.
    Streams.unregister(log)
  end

  defp open(conn, selection, %{config: config} = state) do
    # Re-armed because resolving the cursor can query — an absent `Last-Event-ID`
    # costs a `max_id`, which takes a pool checkout like any other read. This
    # gives the header write that follows its own full budget rather than what
    # that query left.
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
