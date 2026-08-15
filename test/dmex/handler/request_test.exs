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
  alias DMEx.Test.SSEClient

  @accept {"accept", "text/event-stream"}

  defp config(opts \\ []), do: Config.handler!(opts)

  describe "headers/2" do
    test "asks for the stream" do
      headers = Request.headers(0, config())

      assert SSEClient.header(headers, "accept") == "text/event-stream"
    end

    # The whole of it. A subscription is identified by its URL and positioned by
    # `last-event-id`; there is nothing left to negotiate, so this request is
    # byte-identical to what a browser's EventSource sends for the same URL.
    test "adds no DMEx headers of its own" do
      names = 42 |> Request.headers(config()) |> Enum.map(&elem(&1, 0)) |> Enum.sort()

      assert names == ["accept", "last-event-id"]
    end

    test "with nothing to resume from, only accept" do
      names = :none |> Request.headers(config()) |> Enum.map(&elem(&1, 0))

      assert names == ["accept"]
    end
  end

  describe "the resume position" do
    test "a durable cursor is sent as itself" do
      assert Request.headers(1_042, config()) == [@accept, {"last-event-id", "1042"}]
    end

    test "a cursor of zero is still a cursor" do
      # Not the same as having none: a handler that committed 0 has a position.
      assert Request.headers(0, config()) == [@accept, {"last-event-id", "0"}]
    end

    test "the durable cursor wins over last_seen:, whatever it says" do
      for last_seen <- [nil, 0, 99] do
        assert Request.headers(7, config(last_seen: last_seen)) ==
                 [@accept, {"last-event-id", "7"}]
      end
    end

    # One domain, one unit: the option is already the cursor the header carries,
    # so nothing is computed anywhere between them.
    test "last_seen: with no cursor is sent as itself" do
      assert Request.headers(:none, config(last_seen: 499)) == [@accept, {"last-event-id", "499"}]
    end

    test "last_seen: 0 is the whole log" do
      assert Request.headers(:none, config(last_seen: 0)) == [@accept, {"last-event-id", "0"}]
    end

    # The absence of the header is the whole of "from now on" — there is no
    # token, so an unset `last_seen` sends nothing at all.
    test "last_seen: unset with no cursor sends no header" do
      assert Request.headers(:none, config()) == [@accept]
      assert Request.position(:none, config()) == :now
    end
  end

  describe "the URL" do
    test "uses the path it was given" do
      assert Request.path(URI.parse("https://orders.internal/stream"), nil) == "/stream"
    end

    test "falls back to the root" do
      assert Request.path(URI.parse("https://orders.internal"), nil) == "/"
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

    test "last_seen: 0 round-trips as the whole log" do
      headers = Request.headers(:none, config(last_seen: 0))

      assert {:ok, %{cursor: 0}} = Params.parse([], headers)
    end

    # The round trip is an echo: what the option says is what the log parses.
    test "last_seen: a cursor round-trips unchanged" do
      headers = Request.headers(:none, config(last_seen: 41))

      assert {:ok, %{cursor: 41}} = Params.parse([], headers)
    end

    test "last_seen: unset round-trips as the log's tip" do
      headers = Request.headers(:none, config())

      assert {:ok, %{cursor: :now}} = Params.parse([], headers)
    end
  end
end
