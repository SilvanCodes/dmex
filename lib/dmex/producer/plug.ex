defmodule DMEx.Producer.Plug do
  @moduledoc """
  Serves a producer's log as an SSE stream.

  Mount it wherever you mount plugs — a Phoenix router, a `Plug.Router`, or a
  bare Bandit/Cowboy pipeline:

      forward "/stream", to: DMEx.Producer.Plug, init_opts: [producer: MyApp.Producer]

  The mount is a prefix `forward`, and one thing hangs off it:

    * **`GET /stream`** — the producer's whole log, which is what a bare browser
      `EventSource`, `curl`, or anything written before selection existed asks
      for.
    * **`GET /stream/<blob>`** — the same log narrowed to the message types a
      consumer's filter admits. See `DMEx.Fingerprint` for the blob and
      `DMEx.Producer.Messages` for what it is resolved against.

  There is still no application segment and no query string: a producer
  application owns exactly one log, and the app's own hostname already
  identifies the app.

  ## Selection is resolved once, at connect

  A filter cannot be pushed into a SQL index — membership is a computation, not
  something an index can seek on — so the plug expands it against the producer's
  compiled message modules before the send loop starts, turning it into the
  concrete type list the read path's `message_type = ANY($1)` predicate wants.
  The resulting set is also what routes wakeups, so a publish only ever disturbs
  the connections that asked for that type.

  **A filter that matches nothing is a `404`.** An empty expansion is
  indistinguishable on the wire from a healthy stream on a quiet log, so the
  alternative is a connection that opens and says nothing forever. It is not
  proof of a client bug — a consumer deployed ahead of the producer that will
  emit its types is legitimately in this state — which is why DMEx's own
  consumer retries a `404` instead of stopping on it.

  Nothing consumer-supplied is validated beyond that, and emphatically nothing
  is atomized: a name the producer has never heard of simply matches nothing.

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
      stream that converts `max_batch_frequency` pacing into "whenever the buffer
      happens to fill" and swallows heartbeats entirely, so a healthy stream
      looks dead to both the consumer and the proxy's idle timeout. The header
      is nginx-specific and ignored elsewhere, and it is the difference between
      DMEx working behind a default nginx and not.

    * **`dmex-matched-messages`** — on a selective stream only, how many of the
      producer's own types the filter admitted. The blob is opaque and cannot be
      reversed, so this is the one thing that makes a subscription legible from
      outside.

  `connection: keep-alive` is set only on HTTP/1.1, where it means something; it
  is a connection-specific header and illegal on HTTP/2.

  ## Errors

  A malformed `Last-Event-ID` is a `400` with a `text/plain` body naming the
  header, and so is a path segment that is neither a reserved name nor a
  decodable filter. Nothing else about a request is negotiable, because nothing
  else about one is DMEx's: pacing is producer configuration and never appears
  in a request at all.

  A `404` means the segment named something that does not exist here: a reserved
  sub-resource that is not implemented, or a filter matching none of this
  producer's message types.

  There is no `503`/`Retry-After` admission control: DMEx has no aggregate
  budget to enforce one against, so a producer never turns a connect attempt
  away for load reasons. `DMEx.Producer.Watcher` is where one would be enforced
  if it existed — it already holds the live set — and this is where it would be
  asked, before registering. See `ROADMAP.md`.
  """

  alias DMEx.Config
  alias DMEx.Fingerprint
  alias DMEx.Producer.Messages
  alias DMEx.Producer.Params
  alias DMEx.Producer.Status
  alias DMEx.Producer.Stream, as: SendLoop
  alias DMEx.SSE.Frame

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

  # A stream is a read and nothing else, so anything but GET is a `405` with the
  # `allow` header RFC 9110 requires. Without this a POST to the mount would open
  # a chunked stream and sit there, which is a confusing way to learn about a
  # typo in a router.
  @impl Plug
  def call(%Plug.Conn{method: method} = conn, _opts) when method != "GET" do
    conn
    |> Plug.Conn.put_resp_header("allow", "GET")
    |> respond(405, "only GET is supported on a DMEx stream")
  end

  def call(conn, %{producer: producer}) do
    config = producer.__dmex__().config

    # `conn.path_info` is what the mount handed us — Plug's `forward` strips the
    # prefix — so the one-line `forward "/stream", to: DMEx.Producer.Plug` mount
    # routes `/stream/<blob>` with no routing change at all.
    case Params.parse(conn.path_info, conn.req_headers) do
      {:ok, params} -> select(conn, producer, params, config)
      {:error, {:reserved, _name} = error} -> not_found(conn, Params.describe(error))
      {:error, error} -> bad_request(conn, error)
    end
  end

  ## Selection

  # Expanded once, here, against the producer's own compiled message modules
  # rather than against what its log happens to contain — see
  # `DMEx.Producer.Messages` for why those two are not interchangeable.
  #
  # A filter cannot be pushed into a SQL index; membership is a computation, not
  # something an index can seek on. Turning it into a concrete type list at
  # connect time is what lets the read below become an ordinary indexed
  # `message_type = ANY($1)` predicate.
  defp select(conn, producer, %{selection: :all} = params, config) do
    stream(conn, producer, params, config, :all)
  end

  defp select(conn, producer, params, config) do
    selection =
      producer
      |> Messages.types()
      |> Enum.filter(&Fingerprint.member?(params.selection, &1))
      |> MapSet.new()

    if MapSet.size(selection) == 0 do
      # Deliberately loud. An empty expansion is indistinguishable on the wire
      # from a healthy stream on a quiet log, so answering it with an open
      # connection that says nothing forever is the worst of the options.
      #
      # It is *not* proof of a client bug: a consumer deployed ahead of the
      # producer that will emit its types is legitimately in this state, which
      # is why `DMEx.Consumer.Stream` retries a 404 rather than treating it as
      # fatal the way it treats a 400.
      not_found(
        conn,
        "this filter matched none of the message types #{inspect(producer)} knows about"
      )
    else
      stream(conn, producer, params, config, selection)
    end
  end

  ## Streaming

  defp stream(conn, producer, params, config, selection) do
    # The request process registers *itself*, which is the whole point of the
    # request being the stream: when the connection dies the entry goes with it.
    # Registering before the first read means a publish landing in between is
    # picked up by that read rather than missed — and registering before the
    # first *write* puts the connect path under the watcher too, since resolving
    # a cursor and sending headers can block exactly as the send loop can.
    # The status comes back with the registration and lives in that entry, so it
    # is cleaned up when this process dies without anything watching for it.
    # The selection rides in the registry entry, so `DMEx.Producer.Dispatcher`
    # can decide whether a publish is worth waking this connection for without
    # messaging it — which matters most exactly when it is wedged.
    {:ok, status} =
      DMEx.Producer.Registry.register(producer, config.stall_timeout, selection)

    cursor = resolve_cursor(producer, params.cursor)

    Status.arm(status, config.stall_timeout)

    conn =
      conn
      |> put_stream_headers(selection)
      |> Plug.Conn.send_chunked(200)

    :telemetry.execute(
      [:dmex, :stream, :connect],
      %{cursor: cursor, matched: matched(selection)},
      %{producer: producer, selective: selection != :all}
    )

    case advertise_retry(conn, config, status) do
      {:ok, conn} ->
        SendLoop.loop(conn, SendLoop.initial_state(producer, cursor, status, selection))

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
