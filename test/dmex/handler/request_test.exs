defmodule DMEx.Handler.RequestTest do
  @moduledoc """
  The consumer's half of the wire contract, tested with no socket in sight —
  the mirror of `DMEx.Log.ParamsTest`, which does the same for the
  log's half. Between them the two ends of every header are pinned in one
  place each.
  """

  use DMEx.Case, async: true

  alias DMEx.Config
  alias DMEx.Handler.Request
  alias DMEx.Log.Params

  defp config(opts \\ []), do: Config.handler!(opts)

  defp header(headers, name) do
    case List.keyfind(headers, name, 0) do
      {^name, value} -> value
      nil -> nil
    end
  end

  describe "headers/2" do
    test "asks for the stream" do
      headers = Request.headers(0, config())

      assert header(headers, "accept") == "text/event-stream"
    end

    # The whole of it. A subscription is identified by its URL and positioned by
    # `last-event-id`; there is nothing left to negotiate, so this request is
    # byte-identical to what a browser's EventSource sends for the same URL.
    test "adds no DMEx headers of its own" do
      names = Request.headers(42, config()) |> Enum.map(&elem(&1, 0)) |> Enum.sort()

      assert names == ["accept", "last-event-id"]
    end
  end

  describe "the resume position" do
    test "a durable cursor is sent as itself" do
      assert Request.last_event_id(1_042, config()) == [{"last-event-id", "1042"}]
    end

    test "a cursor of zero is still a cursor" do
      # Not the same as having none: a handler that committed 0 has a position.
      assert Request.last_event_id(0, config()) == [{"last-event-id", "0"}]
    end

    test "the durable cursor wins over start:, whatever start: says" do
      for start <- [:beginning, :latest, 99] do
        assert Request.last_event_id(7, config(start: start)) == [{"last-event-id", "7"}]
      end
    end

    # No header at all rather than `0` — which is also what a fresh browser
    # EventSource sends, so the log needs only one code path.
    test "start: :beginning with no cursor sends no header" do
      assert Request.last_event_id(:none, config(start: :beginning)) == []
    end

    test "start: :latest with no cursor sends the literal" do
      assert Request.last_event_id(:none, config(start: :latest)) == [{"last-event-id", "latest"}]
    end

    test "start: an id with no cursor sends that id" do
      assert Request.last_event_id(:none, config(start: 500)) == [{"last-event-id", "500"}]
    end

    test "headers/2 carries it through" do
      assert :none |> Request.headers(config(start: :latest)) |> header("last-event-id") ==
               "latest"

      refute :none |> Request.headers(config(start: :beginning)) |> header("last-event-id")
    end
  end

  describe "the URL" do
    test "uses the path it was given" do
      assert Request.path(URI.parse("https://orders.internal/stream")) == "/stream"
    end

    test "falls back to the root" do
      assert Request.path(URI.parse("https://orders.internal")) == "/"
    end

    test "picks the scheme Mint needs" do
      assert Request.scheme(URI.parse("https://orders.internal/stream")) == :https
      assert Request.scheme(URI.parse("http://orders.internal/stream")) == :http
    end
  end

  describe "against the log" do
    # The point of having both halves as pure modules: what one sends, the other
    # accepts — asserted rather than assumed.
    test "everything this sends, DMEx.Log.Params parses" do
      assert {:ok, params} = Params.parse([], Request.headers(42, config()))

      assert params.cursor == 42
      assert params.selection == :all
    end

    test "start: :beginning round-trips as the whole log" do
      headers = Request.headers(:none, config(start: :beginning))

      assert {:ok, %{cursor: 0}} = Params.parse([], headers)
    end

    test "start: :latest round-trips as the tip" do
      headers = Request.headers(:none, config(start: :latest))

      assert {:ok, %{cursor: :latest}} = Params.parse([], headers)
    end
  end
end
