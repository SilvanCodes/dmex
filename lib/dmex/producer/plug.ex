defmodule DMEx.Producer.Plug do
  @moduledoc """
  Serves a producer's log as an SSE stream.

  Mount it wherever you mount plugs — a Phoenix router, a `Plug.Router`, or a
  bare Bandit/Cowboy pipeline:

      forward "/stream", to: DMEx.Producer.Plug, init_opts: [producer: MyApp.Producer]

  The route is singular and carries no application segment or query string: a
  producer application owns exactly one log and streams all of it, so there is
  nothing for a path or query parameter to select between, and the app's own
  hostname already identifies the app.

  ## The request process *is* the stream

  There is no per-connection `GenServer` and no producer-side supervisor. This
  plug registers the request process for wakeups and then runs the send loop
  inline for the life of the connection.

  That is deliberate. In both Bandit and Cowboy the connection process owns the
  socket, so writing chunks from another process is awkward at best — and
  reclaiming a stalled connection means killing whichever process is blocked in
  the write, so it had better be the one the connection dies with.

  Registration therefore happens here, before anything that can block: it hands
  back the connection's `DMEx.Producer.Status`, which is what puts even the
  connect path under `DMEx.Producer.Watcher`.

  ## Response headers

  Beyond the ordinary SSE set, the `200` carries two things worth knowing about:

    * **`x-accel-buffering: no`** — nginx proxies with `proxy_buffering on` by
      default, which holds response bytes until its buffer fills. On an SSE
      stream that converts `batch_window` pacing into "whenever the buffer
      happens to fill" and swallows heartbeats entirely, so a healthy stream
      looks dead to both the consumer and the proxy's idle timeout. The header
      is nginx-specific and ignored elsewhere, and it is the difference between
      DMEx working behind a default nginx and not.

    * **the echoed pacing values** — `dmex-batch-window` and
      `dmex-max-batch-size` as actually applied, so a clamped consumer can tell
      that it was clamped. See `DMEx.Producer.Params`.

  `connection: keep-alive` is set only on HTTP/1.1, where it means something; it
  is a connection-specific header and illegal on HTTP/2.

  ## Errors

  A malformed `Last-Event-ID` or pacing header is a `400` with a `text/plain`
  body naming the header. A well-formed pacing value out of range is *not* an
  error — it is clamped, and the echo says so.

  There is no `503`/`Retry-After` admission control: DMEx has no aggregate
  budget to enforce one against, so a producer never turns a connect attempt
  away for load reasons. `DMEx.Producer.Watcher` is where one would be enforced
  if it existed — it already holds the live set — and this is where it would be
  asked, before registering. See `ROADMAP.md`.
  """

  alias DMEx.Config
  alias DMEx.Frame
  alias DMEx.Producer.Params
  alias DMEx.Producer.Status
  alias DMEx.Producer.Stream, as: SendLoop

  @behaviour Plug

  @impl Plug
  def init(opts) do
    producer =
      Keyword.get(opts, :producer) ||
        raise ArgumentError,
              "DMEx.Producer.Plug requires a `:producer`, e.g. " <>
                "`forward \"/stream\", to: DMEx.Producer.Plug, " <>
                "init_opts: [producer: MyApp.Producer]`"

    # Deliberately no database or registry access: under Phoenix's default
    # `init_mode` this runs at compile time.
    %{producer: producer}
  end

  @impl Plug
  def call(conn, %{producer: producer}) do
    config = producer.__dmex__().config

    case Params.parse(conn.req_headers, config) do
      {:ok, params} -> stream(conn, producer, params, config)
      {:error, error} -> bad_request(conn, error)
    end
  end

  ## Streaming

  defp stream(conn, producer, params, config) do
    # The request process registers *itself*, which is the whole point of the
    # request being the stream: when the connection dies the entry goes with it.
    # Registering before the first read means a publish landing in between is
    # picked up by that read rather than missed — and registering before the
    # first *write* puts the connect path under the watcher too, since resolving
    # a cursor and sending headers can block exactly as the send loop can.
    # The status comes back with the registration and lives in that entry, so it
    # is cleaned up when this process dies without anything watching for it.
    {:ok, status} = DMEx.Registry.register(producer, config.stall_timeout)

    cursor = resolve_cursor(producer, params.cursor)

    Status.arm(status, config.stall_timeout)

    conn =
      conn
      |> put_stream_headers(params)
      |> Plug.Conn.send_chunked(200)

    :telemetry.execute(
      [:dmex, :stream, :connect],
      %{cursor: cursor},
      %{
        producer: producer,
        batch_window: params.batch_window,
        max_batch_size: params.max_batch_size
      }
    )

    case advertise_retry(conn, config, params, status) do
      {:ok, conn} ->
        SendLoop.loop(conn, SendLoop.initial_state(producer, params, cursor, status))

      {:error, _reason} ->
        conn
    end
  end

  # `latest` is the only cursor needing a query on the connect path, and it is
  # what lets a consumer skip history without a preflight request or a second
  # parameter.
  defp resolve_cursor(producer, :latest) do
    %{storage: storage, storage_opts: opts} = producer.__dmex__()

    case storage.max_id(opts) do
      {:ok, max_id} -> max_id
      {:error, _reason} -> 0
    end
  end

  defp resolve_cursor(_producer, cursor) when is_integer(cursor), do: cursor

  # Sent in a frame of its own at stream open, which is well-defined and
  # dispatches nothing: an SSE parser applies `retry:` the moment it parses the
  # line, while an event is only dispatched if the data buffer is non-empty when
  # the blank line arrives.
  defp advertise_retry(conn, config, params, status) do
    effective = Config.effective_heartbeat(config.heartbeat_ms, params.batch_window)

    Status.arm(status, config.stall_timeout)

    Plug.Conn.chunk(conn, Frame.encode_retry(effective))
  end

  defp put_stream_headers(conn, params) do
    conn
    |> Plug.Conn.put_resp_content_type("text/event-stream")
    |> Plug.Conn.put_resp_header("cache-control", "no-cache")
    |> Plug.Conn.put_resp_header("x-accel-buffering", "no")
    |> put_keep_alive()
    |> put_resp_headers(Params.resp_headers(params))
  end

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
    conn
    |> Plug.Conn.put_resp_content_type("text/plain")
    |> Plug.Conn.send_resp(400, Params.describe(error))
    |> Plug.Conn.halt()
  end
end
