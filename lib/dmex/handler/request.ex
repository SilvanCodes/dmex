defmodule DMEx.Handler.Request do
  @moduledoc """
  What a subscription says on connect.

  The consumer half of the contract `DMEx.Log.Params` validates on the log side,
  kept pure so both ends are data. **Nothing here converts anything**: a durable
  cursor, a configured `last_seen:` and the header are one domain in one unit,
  so `position/2` is a choice between two of them and no arithmetic at all.
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

  # The header's absence is the whole of "from now on".
  defp last_event_id(cursor, config) do
    case position(cursor, config) do
      :now -> []
      cursor -> [{"last-event-id", Integer.to_string(cursor)}]
    end
  end

  @doc """
  Where this subscription is asking to start: a durable cursor wins over
  `last_seen:` whenever there is one.
  """
  @spec position(cursor(), Config.handler()) :: position()
  def position(:none, config), do: config.last_seen || :now
  def position(cursor, _config) when is_integer(cursor), do: cursor

  @doc """
  Builds the path to request: the log's stream URL, plus the selection segment —
  `nil` for a whole-log subscription, which requests the bare `/stream`. It is a
  path segment rather than a query parameter; the wire contract in
  [ARCHITECTURE](ARCHITECTURE.md) has why.
  """
  @spec path(URI.t(), String.t() | nil) :: String.t()
  def path(%URI{path: path}, fingerprint), do: join(path || "/", fingerprint)

  defp join(path, nil), do: path
  defp join(path, fingerprint), do: String.trim_trailing(path, "/") <> "/" <> fingerprint

  @doc "The scheme Mint should connect with."
  @spec scheme(URI.t()) :: :http | :https
  def scheme(%URI{scheme: "https"}), do: :https
  def scheme(%URI{}), do: :http
end
