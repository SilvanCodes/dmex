defmodule DMEx.Log.Params do
  @moduledoc """
  What a connect request said, validated.

  A path segment and one request header describe a subscription, and that is the
  whole of it:

  | carried in | meaning | absent | invalid |
  | --- | --- | --- | --- |
  | the path segment | which message types | the whole log | `400` |
  | `last-event-id` | resume after this id | from now on | `400` |

  **DMEx defines no request headers.** See the wire contract in
  [ARCHITECTURE](ARCHITECTURE.md), which also covers why the selection goes in the
  path.

  This module takes a path and request headers rather than a `Plug.Conn`, which
  keeps it pure, free of any optional dependency, and testable without a socket.

  ## The `Last-Event-ID` vocabulary

  One integer, or nothing:

    * **a non-negative integer id** — resume after it, which is exactly what a
      client echoing back a frame's `id:` asks for. `0` is the whole log.
    * **absent** — from the log's tip at connect, so nothing already appended is
      delivered. The header's absence is the only spelling of "from now on";
      a `latest` token would be a second one.

  Validation is strict: `~r/\\A\\d+\\z/`, so `"5abc"` is a `400`.
  """

  alias DMEx.Handler.Fingerprint

  defstruct [:cursor, selection: :all]

  @typedoc """
  A parsed connect request.

  `:selection` is `:all` for a bare `GET /stream`, or the filter decoded from
  the path segment. `:cursor` is `:now` for a request with no `last-event-id`.
  """
  @type t :: %__MODULE__{
          cursor: non_neg_integer() | :now,
          selection: :all | Fingerprint.t()
        }

  @typedoc "Why a request was rejected: the offending header or path segment, and its value."
  @type error :: {:invalid, source :: String.t(), value :: String.t()}

  @last_event_id "last-event-id"

  # What a filter path segment is called when a `400` has to name it.
  @filter "filter"

  @digits ~r/\A\d+\z/

  @doc """
  Parses a request's path and headers.

  `path_info` is what the mount handed the plug — `[]` for a bare
  `GET /stream`, one segment for anything under it.

  Returns `{:error, {:invalid, source, value}}` for a header or segment that is
  present but outside its vocabulary, which the caller answers with a `400`.
  """
  @spec parse([String.t()], [{String.t(), String.t()}]) :: {:ok, t()} | {:error, error()}
  def parse(path_info, req_headers) when is_list(path_info) and is_list(req_headers) do
    with {:ok, selection} <- selection(path_info),
         {:ok, cursor} <- cursor(req_headers) do
      {:ok, %__MODULE__{selection: selection, cursor: cursor}}
    end
  end

  # Not an ExDoc link: this goes on the wire to a client whose author may not be
  # reading hexdocs, so it names the file in the repository.
  @doc "What a `400` should say back to the client."
  @spec describe(error()) :: String.t()
  def describe({:invalid, @filter, value}) do
    "invalid filter segment: #{inspect(value)}. It must be a DMEx message-type " <>
      "filter as unpadded base64url — see guides/fingerprint-spec.md."
  end

  def describe({:invalid, header, value}) do
    "invalid #{header} header: #{inspect(value)}"
  end

  ## Internals

  # Plug drops empty path segments, so `GET /stream/` arrives here as `[]` and
  # is the same resource as the bare mount without needing a clause of its own.
  defp selection([]), do: {:ok, :all}

  defp selection([blob]) do
    case Fingerprint.decode(blob) do
      {:ok, filter} -> {:ok, filter}
      {:error, :invalid} -> {:error, {:invalid, @filter, blob}}
    end
  end

  defp selection(segments), do: {:error, {:invalid, @filter, Enum.join(segments, "/")}}

  defp cursor(req_headers) do
    case fetch(req_headers, @last_event_id) do
      :error -> {:ok, :now}
      {:ok, value} -> integer(value, @last_event_id)
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
  # sent twice arrives joined, which is outside every vocabulary here and so is
  # the `400` it should be: there is no sensible way to pick a resume position.
  defp fetch(req_headers, name) do
    case for {^name, value} <- req_headers, do: value do
      [] -> :error
      values -> {:ok, Enum.join(values, ", ")}
    end
  end
end
