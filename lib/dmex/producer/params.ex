defmodule DMEx.Producer.Params do
  @moduledoc """
  What a connect request said, validated and clamped.

  A path segment and three request headers describe a subscription, and the
  split between them is the design's own rule: **the URL says what the
  subscription *is*; headers say how it is *paced*.**

  | carried in | meaning | absent | invalid |
  | --- | --- | --- | --- |
  | the path segment | which message types | the whole log | `400` |
  | `last-event-id` | resume position | from the beginning | `400` |
  | `dmex-batch-window` | minimum ms between frames | producer default | `400` |
  | `dmex-max-batch-size` | maximum rows per frame | producer default | `400` |

  This module takes a path and request headers rather than a `Plug.Conn`, which
  keeps it pure, free of any optional dependency, and testable without a socket.

  ## Why the selection is in the URL, and in the path half of it

  A type selector is *identity* — a different selection is a different resource,
  not the same one with a modifier — so it belongs in the path rather than in a
  query string or a header, and three things follow from that:

    * **A browser `EventSource` can select.** It sets no request headers beyond
      `Last-Event-ID`, but the URL is entirely its own. In a header this would
      have left every browser client receiving its producer's whole log and
      discarding most of it.
    * **Intermediaries treat paths as load-bearing and query strings as
      negotiable.** Caches and proxies routinely drop, reorder or key-strip
      query strings; nothing does that to a path. Losing the selection silently
      converts a selective subscription back into a whole-log one, which is a
      bandwidth incident rather than an error.
    * **Selection changes at deploy cadence, not connection cadence.** Pacing is
      re-negotiated repeatedly *within* one subscription's lifetime, so pacing
      in a URL would mint new URLs continuously; a message list changes when
      somebody ships code. One URL per deployment is the stable grouping the
      rule is protecting.

  ## The `/stream` namespace

  Putting a blob in the path makes `/stream/...` a namespace, and a namespace can
  collide. **Reserved names are matched first**, before any decode is attempted,
  which is what makes the guarantee structural rather than probabilistic. The
  reserved list is closed and lives in this module.

  It could hardly collide anyway — the shortest valid blob is a 64-bit seed plus
  three fingerprints, and every reserved name is a short readable word — but
  "hardly" is not a guarantee and ordering is.

  ## Why pacing is clamped but resumption is rejected

  A well-formed pacing value outside the producer's bounds is **clamped**. Those
  bounds exist to protect the producer, and a consumer asking for more than it
  may have should get a working stream at the permitted rate rather than a `400`
  it has no way to interpret. The producer then echoes what it actually applied
  (see `resp_headers/1`), so an adaptive consumer can tell "the producer is
  honouring my request and I still cannot keep up" from "the producer capped me
  and my request never took effect" — opposite situations calling for opposite
  reactions.

  A malformed value of any kind is a `400`, because a client sending garbage has
  a bug worth hearing about rather than being handed an arbitrary slice of the
  log.

  ## The `Last-Event-ID` vocabulary

  The SSE specification puts no numeric constraint on the header, so the
  producer accepts a small vocabulary and rejects everything else:

    * **an integer id** — resume after it, the ordinary case;
    * **`latest`** — start from the tip, resolved against the log's current
      `max(id)`. This is how a consumer that only cares about what happens from
      now on skips history, with no preflight request and no second parameter;
    * **absent** — from id 0, the whole log, which is also what a fresh
      browser `EventSource` sends.

  Validation is deliberately strict: `~r/\\A\\d+\\z/` rather than
  `Integer.parse/1`, which would accept `"5abc"` and `"+5"`. A vocabulary of two
  forms plus absent is small enough to validate exactly.
  """

  alias DMEx.Config
  alias DMEx.Fingerprint

  defstruct [:cursor, :batch_window, :max_batch_size, selection: :all]

  @typedoc """
  A parsed connect request.

  `:selection` is `:all` for a bare `GET /stream` — v1's behaviour, and what a
  browser `EventSource` pointed at the plain URL asks for — or the filter
  decoded from the path segment.
  """
  @type t :: %__MODULE__{
          cursor: non_neg_integer() | :latest,
          batch_window: pos_integer(),
          max_batch_size: pos_integer(),
          selection: :all | Fingerprint.t()
        }

  @typedoc """
  Why a request was rejected.

  `:invalid` names the offending header or path segment and its value, and is
  answered with a `400`. `:reserved` means the segment is a named sub-resource
  of the producer rather than a filter, and is answered with a `404` until one
  of them is implemented.
  """
  @type error ::
          {:invalid, source :: String.t(), value :: String.t()}
          | {:reserved, name :: String.t()}

  @last_event_id "last-event-id"
  @batch_window "dmex-batch-window"
  @max_batch_size "dmex-max-batch-size"
  @matched "dmex-matched-messages"

  # Named sub-resources of the producer, matched before any blob decode. Closed
  # list: adding one is a wire-contract change, not a routing detail.
  @reserved ~w(stats)

  # What a filter path segment is called when a `400` has to name it.
  @filter "filter"

  # HTTP header names are case-insensitive and Plug downcases them, so nothing
  # here should ever compare them any other way.
  @digits ~r/\A\d+\z/

  @doc """
  Parses and clamps a request's path and headers against a producer's configuration.

  `path_info` is what the mount handed the plug — `[]` for a bare
  `GET /stream`, one segment for anything under it.

  Returns `{:error, {:invalid, source, value}}` for a header or segment that is
  present but outside its vocabulary, which the caller answers with a `400`, and
  `{:error, {:reserved, name}}` for a reserved sub-resource.
  """
  @spec parse([String.t()], [{String.t(), String.t()}], Config.Producer.t()) ::
          {:ok, t()} | {:error, error()}
  def parse(path_info, req_headers, %Config.Producer{} = config)
      when is_list(path_info) and is_list(req_headers) do
    with {:ok, selection} <- selection(path_info),
         {:ok, cursor} <- cursor(req_headers),
         {:ok, batch_window} <- pacing(req_headers, @batch_window, config.batch_window),
         {:ok, max_batch_size} <- pacing(req_headers, @max_batch_size, config.max_batch_size) do
      {:ok,
       %__MODULE__{
         selection: selection,
         cursor: cursor,
         batch_window: Config.clamp(:batch_window, batch_window, config),
         max_batch_size: Config.clamp(:max_batch_size, max_batch_size, config)
       }}
    end
  end

  @doc """
  The pacing values actually applied, for the `200`'s response headers.

  Echoing the effective values rather than the requested ones closes the
  adaptive controller's loop with no extra round trip: it gets its real
  operating point rather than its hoped-for one. This cannot be inferred from
  `retry:`, which carries `heartbeat_ms + batch_window` as a single sum.
  """
  @spec resp_headers(t()) :: [{String.t(), String.t()}]
  def resp_headers(%__MODULE__{} = params) do
    [
      {@batch_window, Integer.to_string(params.batch_window)},
      {@max_batch_size, Integer.to_string(params.max_batch_size)}
    ]
  end

  @doc """
  The header saying how many of the producer's own types a filter admitted.

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
  defp selection([]), do: {:ok, :all}

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
      {:ok, value} -> integer(value, @last_event_id)
      {:error, value} -> {:error, {:invalid, @last_event_id, value}}
    end
  end

  defp pacing(req_headers, header, default) do
    case fetch(req_headers, header) do
      :error -> {:ok, default}
      {:ok, value} -> integer(value, header)
      {:error, value} -> {:error, {:invalid, header, value}}
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
