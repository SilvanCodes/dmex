defmodule DMEx.Log.Params do
  @moduledoc """
  What a connect request said, validated.

  A path segment and one request header describe a subscription, and that is the
  whole of it:

  | carried in | meaning | absent | invalid |
  | --- | --- | --- | --- |
  | the path segment | which message types | the whole log | `400` |
  | `last-event-id` | resume position, as `previous/current` or a bare id | from the beginning | `400` |

  **There are no DMEx request headers.** Pacing is log configuration, so a
  connect request carries nothing about it — which means DMEx's own consumer and
  a browser's `EventSource` send byte-identical requests, and everything the
  log needs to answer one is in the request line plus a header SSE already
  defines.

  This module takes a path and request headers rather than a `Plug.Conn`, which
  keeps it pure, free of any optional dependency, and testable without a socket.

  ## Why the selection is in the URL, and in the path half of it

  A type selector is *identity* — a different selection is a different resource,
  not the same one with a modifier — so it belongs in the path rather than in a
  query string or a header, and three things follow from that:

    * **A browser `EventSource` can select.** It sets no request headers beyond
      `Last-Event-ID`, but the URL is entirely its own. In a header this would
      have left every browser client receiving its log's whole log and
      discarding most of it.
    * **Intermediaries treat paths as load-bearing and query strings as
      negotiable.** Caches and proxies routinely drop, reorder or key-strip
      query strings; nothing does that to a path. Losing the selection silently
      converts a selective subscription back into a whole-log one, which is a
      bandwidth incident rather than an error.
    * **A selection is stable, so the URL is stable.** A message set changes
      when somebody ships code, so one URL per deployment is a grouping that
      survives long enough to be worth having in a proxy log, a cache key or a
      dashboard.

  ## The `/stream` namespace

  Putting a blob in the path makes `/stream/...` a namespace, and a namespace can
  collide. **Reserved names are matched first**, before any decode is attempted,
  which is what makes the guarantee structural rather than probabilistic. The
  reserved list is closed and lives in this module.

  It could hardly collide anyway — the shortest valid blob is a 64-bit seed plus
  three fingerprints, and every reserved name is a short readable word — but
  "hardly" is not a guarantee and ordering is.

  ## The `Last-Event-ID` vocabulary

  The SSE specification puts no numeric constraint on the header, so the
  log accepts a small vocabulary and rejects everything else:

    * **`<previous>/<current>`** — the ordinary case, because that is what a
      frame's `id:` field carries and what any SSE client echoes back verbatim.
      Only `current` is used; the predecessor is a consumer-side integrity check
      (see `DMEx.SSE.Frame`) and means nothing to a log, which resumes from
      a position rather than verifying one.
    * **an integer id** — resume after it. What a client constructs by hand, and
      what everything sent before the chain existed looks like.
    * **`latest`** — start from the tip, resolved against the log's current
      `max(id)`. This is how a consumer that only cares about what happens from
      now on skips history, with no preflight request and no second parameter;
    * **absent** — from id 0, the whole log, which is also what a fresh
      browser `EventSource` sends.

  Validation is deliberately strict — `~r/\\A\\d+\\z/` per half rather than
  `Integer.parse/1`, which would accept `"5abc"` and `"+5"`. A vocabulary this
  small is worth validating exactly.
  """

  alias DMEx.Fingerprint

  defstruct [:cursor, selection: :all]

  @typedoc """
  A parsed connect request.

  `:selection` is `:all` for a bare `GET /stream` — what a browser `EventSource`
  pointed at the plain URL asks for, and what a handler declared
  `selective: false` requests — or the filter decoded from the path segment.
  """
  @type t :: %__MODULE__{
          cursor: non_neg_integer() | :latest,
          selection: :all | Fingerprint.t()
        }

  @typedoc """
  Why a request was rejected.

  `:invalid` names the offending header or path segment and its value, and is
  answered with a `400`. `:reserved` means the segment is a named sub-resource
  of the log rather than a filter, and is answered with a `404` until one
  of them is implemented.
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

  # HTTP header names are case-insensitive and Plug downcases them, so nothing
  # here should ever compare them any other way.
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
  The header saying how many of the log's own types a filter admitted.

  Sent only on a selective stream. The blob is opaque — it cannot be reversed,
  and it says nothing at all in a proxy log — so this is the one thing that
  makes a subscription legible from outside: a count of `0` where ten types were
  selected is an encoding mismatch, visible at connect rather than through
  silence.
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

  # Reserved names first, before any decode is attempted. That ordering is what
  # makes the namespace guarantee structural rather than probabilistic.
  # `GET /stream` and `GET /stream/` are the same resource. A trailing slash in a
  # log URL is otherwise a permanent `400`, and a `400` is fatal to a
  # consumer — one stray character would silently kill a subscription forever.
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
      {:ok, "latest"} -> {:ok, :latest}
      {:ok, value} -> resume_position(value)
      {:error, value} -> {:error, {:invalid, @last_event_id, value}}
    end
  end

  # A frame's `id:` is `previous/current`, and a client echoes it back whole. The
  # predecessor is the consumer's own integrity check and carries nothing a
  # log needs, so it is validated and discarded.
  defp resume_position(value) do
    case String.split(value, "/", parts: 2) do
      [current] ->
        integer(current, @last_event_id)

      [previous, current] ->
        with {:ok, _previous} <- integer(previous, @last_event_id) do
          integer(current, @last_event_id)
        end
    end
  end

  defp integer(value, header) do
    if Regex.match?(@digits, value) do
      {:ok, String.to_integer(value)}
    else
      {:error, {:invalid, header, value}}
    end
  end

  # A header sent twice is a client bug of the same kind as a malformed value:
  # there is no sensible way to pick between two resume positions.
  defp fetch(req_headers, name) do
    case for {^name, value} <- req_headers, do: value do
      [] -> :error
      [value] -> {:ok, value}
      values -> {:error, Enum.join(values, ", ")}
    end
  end
end
