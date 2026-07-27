defmodule DMEx.Producer.Params do
  @moduledoc """
  What a connect request said, validated and clamped.

  Three request headers describe a subscription, and the split between them is
  the design's own rule: **the URL says what the subscription *is*; headers say
  how it is *paced*.**

  | header | meaning | absent | invalid |
  | --- | --- | --- | --- |
  | `last-event-id` | resume position | from the beginning | `400` |
  | `dmex-batch-window` | minimum ms between frames | producer default | `400` |
  | `dmex-max-batch-size` | maximum rows per frame | producer default | `400` |

  This module takes request headers rather than a `Plug.Conn`, which keeps it
  pure, free of any optional dependency, and testable without a socket.

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

  defstruct [:cursor, :batch_window, :max_batch_size]

  @type t :: %__MODULE__{
          cursor: non_neg_integer() | :latest,
          batch_window: pos_integer(),
          max_batch_size: pos_integer()
        }

  @typedoc "Why a request was rejected: the offending header and its value."
  @type error :: {:invalid, header :: String.t(), value :: String.t()}

  @last_event_id "last-event-id"
  @batch_window "dmex-batch-window"
  @max_batch_size "dmex-max-batch-size"

  # HTTP header names are case-insensitive and Plug downcases them, so nothing
  # here should ever compare them any other way.
  @digits ~r/\A\d+\z/

  @doc """
  Parses and clamps a request's headers against a producer's configuration.

  Returns `{:error, {:invalid, header, value}}` for a header that is present but
  outside its vocabulary, which the caller answers with a `400`.
  """
  @spec parse([{String.t(), String.t()}], Config.Producer.t()) :: {:ok, t()} | {:error, error()}
  def parse(req_headers, %Config.Producer{} = config) when is_list(req_headers) do
    with {:ok, cursor} <- cursor(req_headers),
         {:ok, batch_window} <- pacing(req_headers, @batch_window, config.batch_window),
         {:ok, max_batch_size} <- pacing(req_headers, @max_batch_size, config.max_batch_size) do
      {:ok,
       %__MODULE__{
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

  @doc "The header name a `400` should name back to the client."
  @spec describe(error()) :: String.t()
  def describe({:invalid, header, value}) do
    "invalid #{header} header: #{inspect(value)}"
  end

  ## Internals

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
