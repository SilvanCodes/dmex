defmodule DMEx.Handler.Request do
  @moduledoc """
  What a subscription says on connect.

  The reading half of the contract `DMEx.Log.Params` implements on the log's,
  and split out for the same reason: it is pure, so both ends of the wire
  contract are data rather than buried in a process. Used by
  `DMEx.Transport.SSE`, the only transport with a request to make.

  One header goes out, `last-event-id`, and it is one SSE already defines.
  **DMEx adds no request headers of its own**, so this request is
  byte-identical to what a browser's `EventSource` sends for the same URL.

  The durable cursor is always the authority; `start:` answers only "where does
  this handler begin when it has no cursor yet". `:beginning` sends **no header
  at all** rather than `0` — what a fresh `EventSource` does, and what the log
  reads as the whole log.
  """

  alias DMEx.Config

  @typedoc "A durable cursor, or `:none` when this handler has never committed one."
  @type cursor :: non_neg_integer() | :none

  @doc "The request headers for a connect at `cursor`."
  @spec headers(cursor(), Config.handler()) :: [{String.t(), String.t()}]
  def headers(cursor, config) do
    [{"accept", "text/event-stream"}] ++ last_event_id(cursor, config)
  end

  @doc """
  Builds the `last-event-id` header for a cursor, or `[]` for none.

  Exposed separately because the vocabulary — an integer, the literal `latest`,
  or absent — is worth reading in one place.
  """
  @spec last_event_id(cursor(), Config.handler()) :: [{String.t(), String.t()}]
  def last_event_id(:none, %{start: start}) do
    case start do
      :beginning -> []
      :latest -> [{"last-event-id", "latest"}]
      id when is_integer(id) -> [{"last-event-id", Integer.to_string(id)}]
    end
  end

  def last_event_id(cursor, _config) when is_integer(cursor) do
    [{"last-event-id", Integer.to_string(cursor)}]
  end

  @doc """
  Builds the path to request: the log's stream URL, plus the selection segment.

  `fingerprint` is `nil` for a whole-log subscription, which requests the bare
  `/stream`. Otherwise it is the handler's compile-time `DMEx.Fingerprint` blob,
  appended as a path segment rather than a query parameter — see the wire
  contract in `ARCHITECTURE.md` for why.
  """
  @spec path(URI.t(), String.t() | nil) :: String.t()
  def path(uri, fingerprint \\ nil)

  def path(%URI{path: nil}, fingerprint), do: join("/", fingerprint)
  def path(%URI{path: path}, fingerprint), do: join(path, fingerprint)

  defp join(path, nil), do: path
  defp join(path, fingerprint), do: String.trim_trailing(path, "/") <> "/" <> fingerprint

  @doc "The scheme Mint should connect with."
  @spec scheme(URI.t()) :: :http | :https
  def scheme(%URI{scheme: "https"}), do: :https
  def scheme(%URI{}), do: :http
end
