defmodule DMEx.Log.Params do
  @moduledoc """
  What a connect request said, validated.

  A path segment and one request header describe a subscription, and that is the
  whole of it:

  | carried in | meaning | absent | invalid |
  | --- | --- | --- | --- |
  | the path segment | which message types | the whole log | `400`, or `404` if reserved |
  | `last-event-id` | resume position, as `previous/current`, a bare id, or an instant | from the beginning | `400` |

  **DMEx defines no request headers.** See the wire contract in
  `ARCHITECTURE.md`, which also covers why the selection goes in the path.

  This module takes a path and request headers rather than a `Plug.Conn`, which
  keeps it pure, free of any optional dependency, and testable without a socket.

  ## The `Last-Event-ID` vocabulary

  The SSE specification puts no numeric constraint on the header, so the log
  accepts a small vocabulary and rejects everything else:

    * **`<previous>/<current>`** — the ordinary case, since that is what a
      frame's `id:` carries and what any SSE client echoes back verbatim. Only
      `current` is used; the predecessor is a consumer-side integrity check (see
      `DMEx.Transport.SSE.Frame`) and means nothing to a log.
    * **an integer id** — resume after it.
    * **an ISO-8601 instant** — resume after the last row appended before it,
      resolved against the log's own clock by `c:DMEx.Storage.cursor_at/2`. How
      a consumer positions itself in time with no preflight request, and — since
      a consumer sends its own `utc_now()` — how it skips history entirely.
    * **absent** — from id 0, the whole log, which is what a fresh browser
      `EventSource` sends.

  There is deliberately **no tip token**. A consumer wanting the tip sends the
  current instant, which resolves to the tip because every row is before it; a
  `latest` would be a second spelling of a position the vocabulary already has.

  Validation is strict: `~r/\\A\\d+\\z/` per half, so `"5abc"` is a `400`, and an
  instant must carry an explicit offset. The two forms cannot collide — ISO-8601
  contains no `/`.
  """

  alias DMEx.Handler.Fingerprint

  defstruct [:cursor, selection: :all]

  @typedoc """
  A parsed connect request.

  `:selection` is `:all` for a bare `GET /stream`, or the filter decoded from
  the path segment.
  """
  @type t :: %__MODULE__{
          cursor: non_neg_integer() | {:at, DateTime.t()},
          selection: :all | Fingerprint.t()
        }

  @typedoc """
  Why a request was rejected.

  `:invalid` names the offending header or path segment and its value, and is
  answered with a `400`. `:reserved` means the segment is a named sub-resource
  of the log rather than a filter, and is answered with a `404` until one is
  implemented.
  """
  @type error ::
          {:invalid, source :: String.t(), value :: String.t()}
          | {:reserved, name :: String.t()}

  @last_event_id "last-event-id"
  @matched "dmex-matched-messages"

  # Named sub-resources of the log, matched before any blob decode. Closed
  # list: adding one is a wire-contract change, not a routing detail.
  @reserved ~w(stats)

  # What a filter path segment is called when a `400` has to name it.
  @filter "filter"

  @digits ~r/\A\d+\z/

  @doc """
  Parses a request's path and headers.

  `path_info` is what the mount handed the plug — `[]` for a bare
  `GET /stream`, one segment for anything under it.

  Returns `{:error, {:invalid, source, value}}` for a header or segment that is
  present but outside its vocabulary, which the caller answers with a `400`, and
  `{:error, {:reserved, name}}` for a reserved sub-resource.
  """
  @spec parse([String.t()], [{String.t(), String.t()}]) :: {:ok, t()} | {:error, error()}
  def parse(path_info, req_headers)
      when is_list(path_info) and is_list(req_headers) do
    with {:ok, selection} <- selection(path_info),
         {:ok, cursor} <- cursor(req_headers) do
      {:ok, %__MODULE__{selection: selection, cursor: cursor}}
    end
  end

  @doc """
  Builds the header saying how many of the log's types a filter admitted.

  Sent only on a selective stream.
  """
  @spec matched_header(non_neg_integer()) :: [{String.t(), String.t()}]
  def matched_header(count) when is_integer(count) and count >= 0 do
    [{@matched, Integer.to_string(count)}]
  end

  @doc "What a `400` or `404` should say back to the client."
  @spec describe(error()) :: String.t()
  def describe({:invalid, @filter, value}) do
    "invalid filter segment: #{inspect(value)}. It must be a DMEx message-type " <>
      "filter as unpadded base64url — see guides/fingerprint-spec.md."
  end

  def describe({:invalid, header, value}) do
    "invalid #{header} header: #{inspect(value)}"
  end

  def describe({:reserved, name}) do
    "#{inspect(name)} is a reserved sub-resource of this stream and is not implemented"
  end

  ## Internals

  # Reserved names first, before any decode. `GET /stream` and `GET /stream/`
  # are the same resource: a trailing slash would otherwise be a permanent
  # `400`, and a `400` is fatal to a consumer.
  defp selection([]), do: {:ok, :all}
  defp selection([""]), do: {:ok, :all}

  defp selection([name]) when name in @reserved, do: {:error, {:reserved, name}}

  defp selection([blob]) do
    case Fingerprint.decode(blob) do
      {:ok, filter} -> {:ok, filter}
      {:error, :invalid} -> {:error, {:invalid, @filter, blob}}
    end
  end

  defp selection(segments), do: {:error, {:invalid, @filter, Enum.join(segments, "/")}}

  defp cursor(req_headers) do
    case fetch(req_headers, @last_event_id) do
      :error -> {:ok, 0}
      {:ok, value} -> resume_position(value)
      {:error, value} -> {:error, {:invalid, @last_event_id, value}}
    end
  end

  # Digits first, since the id forms are the common case and an instant cannot
  # begin with a bare run of digits followed by nothing else.
  defp resume_position(value) do
    case String.split(value, "/", parts: 2) do
      [current] ->
        if Regex.match?(@digits, current) do
          integer(current, @last_event_id)
        else
          instant(current)
        end

      [previous, current] ->
        with {:ok, _previous} <- integer(previous, @last_event_id) do
          integer(current, @last_event_id)
        end
    end
  end

  # Tagged rather than bare, so a caller cannot confuse a position it must
  # resolve against storage with one already in the cursor's own domain.
  defp instant(value) do
    case DateTime.from_iso8601(value) do
      {:ok, at, _offset} -> {:ok, {:at, at}}
      {:error, _reason} -> {:error, {:invalid, @last_event_id, value}}
    end
  end

  defp integer(value, header) do
    if Regex.match?(@digits, value) do
      {:ok, String.to_integer(value)}
    else
      {:error, {:invalid, header, value}}
    end
  end

  # HTTP header names are case-insensitive and Plug downcases them. A header
  # sent twice is a `400`: there is no sensible way to pick a resume position.
  defp fetch(req_headers, name) do
    case for {^name, value} <- req_headers, do: value do
      [] -> :error
      [value] -> {:ok, value}
      values -> {:error, Enum.join(values, ", ")}
    end
  end
end
