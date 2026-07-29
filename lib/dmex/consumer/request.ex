defmodule DMEx.Consumer.Request do
  @moduledoc """
  What a subscription says on connect, and what it hears back.

  The consumer's half of the contract `DMEx.Producer.Params` implements on the
  producer's — and split out for the same reason: it is pure, so it can be
  tested without a socket, and having both ends of the wire contract expressed
  as data rather than buried in a process makes it obvious when one drifts from
  the other.

  Three headers go out and two come back:

  | header | out | back |
  | --- | --- | --- |
  | `last-event-id` | where to resume from | — |
  | `dmex-batch-window` | what pacing is asked for | what was applied |
  | `dmex-max-batch-size` | what pacing is asked for | what was applied |

  ## Asked for, not granted

  The pacing sent is what the controller decided to **request**. The producer
  may clamp it to its own bounds and echoes back what it actually applied, and
  that echo — not the request — is what the controller reasons about next. Send
  the granted values instead and every re-parameterization becomes a no-op on
  the wire while the controller believes it took effect.

  ## The resume position

  The durable cursor is always the authority. `start:` answers only "where does
  this handler begin when it has no cursor yet", and once one exists it is
  ignored entirely.

  `:beginning` sends **no header at all** rather than `0` — which is also what a
  fresh browser `EventSource` does, and what the producer reads as "the whole
  log". One vocabulary, one code path on the producer.
  """

  alias DMEx.Config

  @typedoc "A pacing pair, either requested or granted."
  @type pacing :: %{batch_window: pos_integer(), max_batch_size: pos_integer()}

  @typedoc "A durable cursor, or `:none` when this handler has never committed one."
  @type cursor :: non_neg_integer() | :none

  @doc "The request headers for a connect at `cursor`, asking for `requested`."
  @spec headers(pacing(), cursor(), Config.Consumer.t()) :: [{String.t(), String.t()}]
  def headers(requested, cursor, %Config.Consumer{} = config) do
    [
      {"accept", "text/event-stream"},
      {"dmex-batch-window", Integer.to_string(requested.batch_window)},
      {"dmex-max-batch-size", Integer.to_string(requested.max_batch_size)}
    ] ++ last_event_id(cursor, config)
  end

  @doc """
  The `last-event-id` header for a cursor, or `[]` for none.

  Exposed separately because the vocabulary — an integer, the literal `latest`,
  or absent — is the part most worth reading in one place.
  """
  @spec last_event_id(cursor(), Config.Consumer.t()) :: [{String.t(), String.t()}]
  def last_event_id(:none, %Config.Consumer{start: start}) do
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
  Adopts the pacing the producer echoed as the new operating point.

  A header that is absent or unparseable leaves the previous value in place: a
  producer that echoes nothing is one this consumer has no news about, which is
  not the same as one that granted zero.
  """
  @spec adopt(pacing(), [{String.t(), String.t()}]) :: pacing()
  def adopt(pacing, headers) when is_list(headers) do
    Enum.reduce(headers, pacing, fn
      {"dmex-batch-window", value}, acc -> put_int(acc, :batch_window, value)
      {"dmex-max-batch-size", value}, acc -> put_int(acc, :max_batch_size, value)
      _other, acc -> acc
    end)
  end

  @doc """
  The path to request: the producer's stream URL, plus the selection segment.

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

  defp put_int(pacing, key, value) do
    case Integer.parse(value) do
      {int, ""} when int > 0 -> Map.put(pacing, key, int)
      _ -> pacing
    end
  end
end
