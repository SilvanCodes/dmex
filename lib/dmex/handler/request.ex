defmodule DMEx.Handler.Request do
  @moduledoc """
  What a subscription says on connect, and what it hears back.

  The handler's half of the contract `DMEx.Log.Params` implements on the
  log's — and split out for the same reason: it is pure, so it can be
  tested without a socket, and having both ends of the wire contract expressed
  as data rather than buried in a process makes it obvious when one drifts from
  the other.

  One header goes out, and it is one SSE already defines:

  | header | meaning |
  | --- | --- |
  | `last-event-id` | where to resume from |

  **DMEx adds no request headers of its own.** Pacing is the log's
  configuration, so there is nothing to ask for and nothing to hear back — which
  means this request is byte-identical to what a browser's `EventSource` sends
  for the same URL. Everything that distinguishes one subscription from another
  is in the path.

  ## The resume position

  The durable cursor is always the authority. `start:` answers only "where does
  this handler begin when it has no cursor yet", and once one exists it is
  ignored entirely.

  `:beginning` sends **no header at all** rather than `0` — which is also what a
  fresh browser `EventSource` does, and what the log reads as "the whole
  log". One vocabulary, one code path on the log.
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
  The `last-event-id` header for a cursor, or `[]` for none.

  Exposed separately because the vocabulary — an integer, the literal `latest`,
  or absent — is the part most worth reading in one place.
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
  The path to request: the log's stream URL, plus the selection segment.

  `fingerprint` is `nil` for a whole-log subscription, which requests the bare
  `/stream` — what a browser `EventSource` pointed at the plain URL sends, and
  what a handler declared `selective: false` asks for. Otherwise it is the
  handler's compile-time `DMEx.Fingerprint` blob, appended as a path segment.

  A path segment rather than `?filter=`, and the reason is not aesthetic: caches
  and proxies routinely drop, reorder or key-strip query strings and nothing
  does that to a path. Losing the selection would silently convert a selective
  subscription back into a whole-log one, which is a bandwidth incident rather
  than an error. base64url needs no percent-encoding in a path either, so the
  placement costs nothing.
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
