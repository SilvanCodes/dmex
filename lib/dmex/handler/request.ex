defmodule DMEx.Handler.Request do
  @moduledoc """
  What a subscription says on connect.

  The consumer half of the contract `DMEx.Log.Params` validates on the log side,
  kept pure so both ends are data rather than buried in a process. Used by
  `DMEx.Transport.SSE` — there is no request to build without a wire, and
  `position/2` is the one part `DMEx.Transport.Local` shares.

  The only header DMEx contributes is `last-event-id`, alongside the `accept`
  every SSE client sends. It carries one non-negative integer — the exclusive
  cursor to resume after — or it is absent, which asks the log for its tip.

  An **id names a message**, so `stream_from: 42` must deliver 42 — the
  exclusive cursor `41`. This is the only place in DMEx that conversion happens,
  and it needs no clamp because `DMEx.Config` has already rejected anything
  below 1.
  """

  alias DMEx.Config

  @typedoc "A durable cursor, or `:none` when this handler has never committed one."
  @type cursor :: non_neg_integer() | :none

  @typedoc "An exclusive cursor, or `:now` for the log's tip at connect."
  @type position :: non_neg_integer() | :now

  @doc "The request headers for a connect at `cursor`."
  @spec headers(cursor(), Config.handler()) :: [{String.t(), String.t()}]
  def headers(cursor, config) do
    [{"accept", "text/event-stream"}] ++ last_event_id(cursor, config)
  end

  @doc """
  Builds the `last-event-id` header for a cursor, or none at all.

  An empty list is the whole of "from now on": the header's absence is what says
  it, so there is no token to define and a browser `EventSource` that has never
  received a frame asks for the same thing.
  """
  @spec last_event_id(cursor(), Config.handler()) :: [{String.t(), String.t()}]
  def last_event_id(cursor, config) do
    case position(cursor, config) do
      :now -> []
      cursor -> [{"last-event-id", Integer.to_string(cursor)}]
    end
  end

  @doc """
  Where this subscription is asking to start.

  A durable cursor wins over `stream_from:` whenever there is one — that is the
  whole of resumption. `DMEx.Transport.Local` resolves the same two answers
  against the log directly rather than over a wire, so the id form's `- 1` is
  computed here and nowhere else.
  """
  @spec position(cursor(), Config.handler()) :: position()
  def position(:none, %{stream_from: nil}), do: :now
  def position(:none, %{stream_from: id}) when is_integer(id), do: id - 1
  def position(cursor, _config) when is_integer(cursor), do: cursor

  @doc """
  Builds the path to request: the log's stream URL, plus the selection segment.

  `fingerprint` is `nil` for a whole-log subscription, which requests the bare
  `/stream`. Otherwise it is the handler's compile-time `DMEx.Handler.Fingerprint` blob,
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
