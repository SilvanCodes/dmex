defmodule DMEx.Handler.Request do
  @moduledoc """
  What a subscription says on connect.

  The consumer half of the contract `DMEx.Log.Params` validates on the log side,
  kept pure so both ends are data rather than buried in a process. Used by
  `DMEx.Transport.SSE`.

  The only header DMEx contributes is `last-event-id`, alongside the `accept`
  every SSE client sends.

  `stream_from:` reaches here already resolved by `DMEx.Handler.Stream.init/1`,
  so it is a positive id or a `DateTime` and never `:current`. The two are the
  same request in different units, and only one of them needs arithmetic:

    * an **id names a message**, so `stream_from: 42` must deliver 42 — the
      exclusive cursor `41`. This is the only place in DMEx that conversion
      happens, and it needs no clamp because `DMEx.Config` has already rejected
      anything below 1.
    * an **instant names the gap between two messages**, which is what an
      exclusive cursor already is, so it goes on the wire as-is and the log
      resolves it against its own rows.
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
  Builds the `last-event-id` header for a cursor.

  Exposed separately because the vocabulary — an integer or an instant — is
  worth reading in one place.
  """
  @spec last_event_id(cursor(), Config.handler()) :: [{String.t(), String.t()}]
  def last_event_id(cursor, config) do
    [{"last-event-id", encode(position(cursor, config))}]
  end

  @doc """
  Where this subscription is asking to start, as the wire will express it.

  A durable cursor wins over `stream_from:` whenever there is one — that is the
  whole of resumption. `DMEx.Transport.SSE` needs the same answer for its
  predecessor check as the header carries, so both derive from here rather than
  each recomputing the `- 1`.
  """
  @spec position(cursor(), Config.handler()) :: non_neg_integer() | DateTime.t()
  def position(:none, %{stream_from: %DateTime{} = at}), do: at
  def position(:none, %{stream_from: id}) when is_integer(id), do: id - 1
  def position(cursor, _config) when is_integer(cursor), do: cursor

  defp encode(%DateTime{} = at), do: DateTime.to_iso8601(at)
  defp encode(cursor) when is_integer(cursor), do: Integer.to_string(cursor)

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
