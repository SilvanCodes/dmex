defmodule DMEx.Transport.SSE do
  @moduledoc """
  Server-Sent Events over HTTP: the transport for a log that is somebody else's.

  Both ends of the wire DMEx defines. The send side encodes rows as
  `DMEx.Transport.SSE.Frame` blocks and chunks them down a `Plug.Conn`; the
  receive side is a reader process owning a Mint connection, decoding what comes
  back (`DMEx.Transport.SSE.Decoder`) and handing rows to its subscription.

  Chosen for a `bindings:` entry that is a URL. Everything true of a *wire* and of
  nothing else lives here:

    * the request contract (`DMEx.Handler.Request`)
    * HTTP status policy: `2xx` proceeds, `404` retries, other `4xx` is fatal,
      everything else reconnects
    * liveness, `missed_heartbeats` × the interval the log advertises as `retry:`

  **The loop is sequential, so nothing is ever parked.** Responses are handled
  in the order they arrive and rows go out with `DMEx.Transport.deliver/2` as
  they decode, so a response that ends the connection is reached only after
  everything ahead of it has already been run.
  """

  @behaviour DMEx.Transport

  alias DMEx.Handler.Request
  alias DMEx.Transport
  alias DMEx.Transport.SSE.Decoder
  alias DMEx.Transport.SSE.Frame

  require Logger

  # Mint and Plug are optional dependencies: an append-only host carries no Mint
  # and never reaches the receive side, a pure consumer no Plug and never the
  # send side. See `DMEx.Log.Plug` for the same declaration on the HTTP door.
  @compile {:no_warn_undefined, [Mint.HTTP, Plug.Conn]}

  # Only used before a log has advertised its own interval, which in practice is
  # the first chunk of the first response.
  @assumed_retry 60_000

  ## Send side

  @impl Transport
  def write(conn, rows) do
    frames = Enum.map(rows, &Frame.encode_row/1)

    with {:ok, conn} <- Plug.Conn.chunk(conn, frames) do
      {:ok, conn, %{bytes: IO.iodata_length(frames)}}
    end
  end

  @impl Transport
  def heartbeat(conn), do: Plug.Conn.chunk(conn, Frame.encode_heartbeat())

  ## Receive side

  @impl Transport
  def start_reader(opts), do: spawn_link(fn -> read(opts) end)

  defp read(opts) do
    owner = Keyword.fetch!(opts, :owner)
    config = Keyword.fetch!(opts, :config)
    uri = opts |> Keyword.fetch!(:origin) |> URI.parse()

    {conn, ref} = open(uri, opts)

    # After the request is on the wire and not before: this is what clears the
    # subscription's backoff, so it has to mean a connection that happened.
    Transport.connected(owner)

    loop(%{
      conn: conn,
      ref: ref,
      owner: owner,
      url: URI.to_string(uri),
      handler: Keyword.fetch!(opts, :handler),
      subscription: Keyword.fetch!(opts, :subscription),
      config: config,
      decoder: Decoder.new(config.max_frame_bytes)
    })
  end

  # Exits rather than returning an error: to the subscription a connection that
  # never opened and one that opened and died are the same event, and both are
  # answered by the same jittered retry.
  defp open(uri, opts) do
    case Mint.HTTP.connect(Request.scheme(uri), uri.host, uri.port, mode: :active) do
      {:ok, conn} -> request(conn, uri, opts)
      {:error, reason} -> exit(reason)
    end
  end

  defp request(conn, uri, opts) do
    path = Request.path(uri, Keyword.get(opts, :fingerprint))
    headers = Request.headers(Keyword.fetch!(opts, :cursor), Keyword.fetch!(opts, :config))

    case Mint.HTTP.request(conn, "GET", path, headers, nil) do
      {:ok, conn, ref} ->
        {conn, ref}

      # The socket opened and the request did not, so it is closed here rather
      # than left to the link to reclaim.
      {:error, conn, reason} ->
        _ = Mint.HTTP.close(conn)

        exit(reason)
    end
  end

  # The `after` clause is the liveness deadline, re-armed by every message: a
  # reader's mailbox carries nothing but this socket, so "no message" is
  # "no bytes" — which is what silence means here, heartbeat comments included.
  defp loop(state) do
    receive do
      message -> loop(stream(message, state))
    after
      liveness(state) -> exit({:silent_for, liveness(state)})
    end
  end

  defp stream(message, state) do
    case Mint.HTTP.stream(state.conn, message) do
      :unknown -> state
      {:ok, conn, responses} -> Enum.reduce(responses, %{state | conn: conn}, &response/2)
      {:error, _conn, reason, _responses} -> exit(reason)
    end
  end

  defp liveness(state) do
    # Until the log has advertised its own, assume a generous interval rather
    # than drop a healthy connection from a log that paces itself slowly.
    state.config.missed_heartbeats * (Decoder.retry(state.decoder) || @assumed_retry)
  end

  defp response({:status, ref, status}, %{ref: ref} = state) do
    cond do
      status in 200..299 ->
        state

      status == 404 ->
        Logger.warning(
          "DMEx #{state.subscription} (#{inspect(state.handler)}) got 404 from " <>
            "#{state.url}: its message types match nothing this log knows. Either the " <>
            "log has not deployed them yet, or this subscription is pointed at the " <>
            "wrong log. Retrying."
        )

        exit({:no_matching_types, state.url})

      status in 400..499 ->
        # This client sent something the log could not parse. Reconnecting would
        # send it again, so stop loudly instead.
        Transport.fatal({:bad_request, status, state.url})

      true ->
        exit({:http_status, status})
    end
  end

  defp response({:data, ref, chunk}, %{ref: ref} = state) do
    case Decoder.push(state.decoder, chunk) do
      {:ok, [], decoder} ->
        %{state | decoder: decoder}

      {:ok, rows, decoder} ->
        :ok = Transport.deliver(state.owner, rows)

        %{state | decoder: decoder}

      # Reconnecting from the durable cursor costs only a re-read.
      {:error, reason, _decoder} ->
        exit({:decode_failed, reason})
    end
  end

  defp response({:done, ref}, %{ref: ref}), do: exit(:closed)

  defp response({:error, ref, reason}, %{ref: ref}), do: exit(reason)

  defp response(_other, state), do: state
end
