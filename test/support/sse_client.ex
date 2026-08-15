defmodule DMEx.Test.SSEClient do
  @moduledoc """
  A bare `Mint.HTTP` SSE client, shared by the integration suite and the benches.

  The wire contract's claim is that any SSE-capable client can read a DMEx
  stream, so the client reading one in a test must be an ordinary HTTP client
  and never a DMEx consumer.

  A 200 is a stream that never ends, so a read is bounded by a window rather
  than by end of response: what comes back is whatever arrived before the stop
  condition, which is how a streaming response is observed at all.
  """

  @default_timeout 200

  @doc """
  Issues `GET path` against `host:port` and returns `{status, headers, body}`.

  `headers` are sent verbatim — nothing is added, so a caller that omits
  `accept` really omits it.

  Options:

    * `:until` — the stop condition.
      * `:done` (the default) — the response's `{:done, ref}`, or the deadline.
        An error response completes and returns early; a stream does not, and
        reads the whole window.
      * `:headers` — as soon as a 200's status and headers have arrived, since
        for a stream that is all there is to wait for. A non-200 is still read
        to completion, for its body.
      * `:deadline` — read the whole window whatever arrives, for a test whose
        subject is *when* bytes appear rather than what they say.
    * `:timeout` — the window in milliseconds. Defaults to `#{@default_timeout}`.
  """
  @spec request(String.t(), :inet.port_number(), String.t(), keyword(), keyword()) ::
          {non_neg_integer(), list(), binary()}
  def request(host, port, path, headers \\ [], opts \\ []) do
    until = Keyword.get(opts, :until, :done)
    deadline = System.monotonic_time(:millisecond) + Keyword.get(opts, :timeout, @default_timeout)

    {:ok, conn} = Mint.HTTP.connect(:http, host, port, mode: :active)
    {:ok, conn, ref} = Mint.HTTP.request(conn, "GET", path, headers, nil)

    read(conn, ref, until, deadline, {nil, [], []})
  end

  @doc "The value of one response header, or `nil`."
  @spec header(list(), String.t()) :: String.t() | nil
  def header(headers, name) do
    case List.keyfind(headers, name, 0) do
      {^name, value} -> value
      nil -> nil
    end
  end

  defp read(conn, ref, until, deadline, acc) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      message ->
        case Mint.HTTP.stream(conn, message) do
          {:ok, conn, responses} ->
            {acc, done?} = absorb(responses, ref, acc)

            if stop?(until, acc, done?) do
              close(conn, acc)
            else
              read(conn, ref, until, deadline, acc)
            end

          # A message this connection did not own — keep waiting rather than
          # treating it as end of stream.
          :unknown ->
            read(conn, ref, until, deadline, acc)

          {:error, _conn, _reason, _responses} ->
            {status, headers, body} = acc

            {status, headers, IO.iodata_to_binary(body)}
        end
    after
      timeout -> close(conn, acc)
    end
  end

  defp stop?(:deadline, _acc, _done?), do: false
  defp stop?(:done, _acc, done?), do: done?

  defp stop?(:headers, {status, headers, _body}, done?) do
    done? or (status == 200 and headers != [])
  end

  defp absorb(responses, ref, {status, headers, body}) do
    {status, headers, body, done?} =
      Enum.reduce(responses, {status, headers, body, false}, fn
        {:status, ^ref, value}, {_s, h, b, d} -> {value, h, b, d}
        {:headers, ^ref, value}, {s, _h, b, d} -> {s, value, b, d}
        {:data, ^ref, chunk}, {s, h, b, d} -> {s, h, [b, chunk], d}
        {:done, ^ref}, {s, h, b, _d} -> {s, h, b, true}
        _other, acc -> acc
      end)

    {{status, headers, body}, done?}
  end

  defp close(conn, {status, headers, body}) do
    Mint.HTTP.close(conn)

    {status, headers, IO.iodata_to_binary(body)}
  end
end
