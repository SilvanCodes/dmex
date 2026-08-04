defmodule DMEx.Transport.SSE do
  @moduledoc """
  Server-Sent Events over HTTP: the transport for a log that is somebody else's.

  Both ends of the wire DMEx defines. The send side encodes rows as
  `DMEx.Transport.SSE.Frame` blocks and chunks them down a `Plug.Conn`; the
  receive side owns a Mint connection, decodes what comes back, and turns HTTP
  into the small vocabulary `DMEx.Handler.Stream` acts on.

  Chosen for a `logs:` entry that is a URL. Everything true of a *wire* and of
  nothing else lives here:

    * framing and decoding (`DMEx.Transport.SSE.Frame`,
      `DMEx.Transport.SSE.Decoder`)
    * the request contract (`DMEx.Handler.Request`)
    * HTTP status policy: `2xx` proceeds, `404` retries, other `4xx` is fatal,
      everything else reconnects
    * liveness, `missed_heartbeats` × the interval the log advertises as `retry:`

  **Backpressure.** Mint re-arms `active: :once` only when the owner returns, so
  the receive window is what blocks the log's `chunk/2` — ARCHITECTURE
  invariant 8.
  """

  @behaviour DMEx.Transport

  alias DMEx.Handler.Request
  alias DMEx.Transport.SSE.Decoder
  alias DMEx.Transport.SSE.Frame

  require Logger

  # Only used before a log has advertised its own interval, which in practice is
  # the first chunk of the first response.
  @assumed_retry 60_000

  ## Send side

  @impl DMEx.Transport
  def write(conn, rows) do
    frames = Enum.map(rows, &Frame.encode_row/1)

    case Plug.Conn.chunk(conn, frames) do
      {:ok, conn} -> {:ok, conn, %{bytes: IO.iodata_length(frames)}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DMEx.Transport
  def heartbeat(conn), do: Plug.Conn.chunk(conn, Frame.encode_heartbeat())

  ## Receive side

  @impl DMEx.Transport
  def connect(opts) do
    subscription = Keyword.fetch!(opts, :subscription)
    handler = Keyword.fetch!(opts, :handler)
    config = Keyword.fetch!(opts, :config)
    cursor = Keyword.fetch!(opts, :cursor)
    uri = opts |> Keyword.fetch!(:origin) |> URI.parse()

    with {:ok, conn} <- Mint.HTTP.connect(Request.scheme(uri), uri.host, uri.port, mode: :active),
         path = Request.path(uri, Keyword.get(opts, :fingerprint)),
         {:ok, conn, ref} <-
           Mint.HTTP.request(conn, "GET", path, Request.headers(cursor, config), nil) do
      {:ok,
       %{
         conn: conn,
         ref: ref,
         url: URI.to_string(uri),
         handler: handler,
         subscription: subscription,
         config: config,
         decoder: Decoder.new(config.max_frame_bytes),
         # Responses from one socket message not acted on yet — see
         # `responses/3`.
         pending: []
       }}
    else
      {:error, reason} ->
        {:error, reason}

      # The socket opened and the request did not. Closed here rather than left
      # for the caller, which has no source to close yet.
      {:error, conn, reason} ->
        _ = Mint.HTTP.close(conn)

        {:error, reason}
    end
  end

  @impl DMEx.Transport
  def close(%{conn: conn}) do
    _ = Mint.HTTP.close(conn)

    :ok
  end

  @impl DMEx.Transport
  def liveness(source) do
    # Until the log has advertised its own, assume a generous interval rather
    # than drop a healthy connection from a log that paces itself slowly.
    interval = Decoder.retry(source.decoder) || @assumed_retry

    source.config.missed_heartbeats * interval
  end

  # Nothing parked is `:unknown`, which is what `c:DMEx.Transport.drain/1`
  # specifies and what `DMEx.Transport.Local` answers — otherwise the stream
  # re-arms its liveness timer once per batch for nothing.
  @impl DMEx.Transport
  def drain(%{pending: []}), do: :unknown

  def drain(source) do
    responses(source.pending, %{source | pending: []}, [])
  end

  @impl DMEx.Transport
  def handle_message(message, source) do
    case Mint.HTTP.stream(source.conn, message) do
      :unknown ->
        :unknown

      {:ok, conn, responses} ->
        responses(responses, %{source | conn: conn}, [])

      {:error, conn, reason, _responses} ->
        {:disconnect, reason, %{source | conn: conn}}
    end
  end

  # One socket message can carry several responses, and rows accumulate across
  # them so the owner runs a whole read's worth in one go — the same shape one
  # `chunk/2` on the other end produced.
  #
  # A terminal response among them does **not** discard the rows ahead of it:
  # those are complete and decoded, and dropping them costs a re-read of rows
  # the subscription already has in hand. So rows are returned first and the
  # terminal is parked in `:pending`, collected with `:continue`.
  defp responses([], source, rows), do: yield(rows, source)

  defp responses([event | rest] = remaining, source, rows) do
    case response(event, source) do
      {:cont, source} -> responses(rest, source, rows)
      {:rows, decoded, source} -> responses(rest, source, Enum.reverse(decoded, rows))
      {:halt, terminal} when rows == [] -> terminal
      {:halt, _terminal} -> yield(rows, %{source | pending: remaining})
    end
  end

  defp yield([], source), do: {:noreply, source}
  defp yield(rows, source), do: {:rows, Enum.reverse(rows), source}

  defp response({:status, ref, status}, %{ref: ref} = source) do
    cond do
      status in 200..299 ->
        {:cont, source}

      status == 404 ->
        Logger.warning(
          "DMEx #{source.subscription} (#{inspect(source.handler)}) got 404 from " <>
            "#{source.url}: its message types match nothing this log knows. Either the " <>
            "log has not deployed them yet, or this subscription is pointed at the " <>
            "wrong log. Retrying."
        )

        {:halt, {:disconnect, {:no_matching_types, source.url}, source}}

      status in 400..499 ->
        # This client sent something the log could not parse. Reconnecting would
        # send it again, so stop loudly instead.
        {:halt, {:stop, {:bad_request, status, source.url}}}

      true ->
        {:halt, {:disconnect, {:http_status, status}, source}}
    end
  end

  defp response({:data, ref, chunk}, %{ref: ref} = source) do
    case Decoder.push(source.decoder, chunk) do
      {:ok, decoded, decoder} ->
        {:rows, decoded, %{source | decoder: decoder}}

      {:error, reason, _decoder} ->
        # Reconnecting from the durable cursor costs only a re-read.
        {:halt, {:disconnect, {:decode_failed, reason}, source}}
    end
  end

  defp response({:done, ref}, %{ref: ref} = source), do: {:halt, {:disconnect, :closed, source}}

  defp response({:error, ref, reason}, %{ref: ref} = source) do
    {:halt, {:disconnect, reason, source}}
  end

  defp response(_other, source), do: {:cont, source}
end
