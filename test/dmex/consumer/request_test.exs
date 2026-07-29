defmodule DMEx.Consumer.RequestTest do
  @moduledoc """
  The consumer's half of the wire contract, tested with no socket in sight —
  the mirror of `DMEx.Producer.ParamsTest`, which does the same for the
  producer's half. Between them the two ends of every header are pinned in one
  place each.
  """

  use DMEx.Case, async: true

  alias DMEx.Config
  alias DMEx.Consumer.Request
  alias DMEx.Producer.Params

  defp config(opts \\ []), do: Config.consumer!(opts)

  defp pacing(window \\ 200, size \\ 500), do: %{batch_window: window, max_batch_size: size}

  defp header(headers, name) do
    case List.keyfind(headers, name, 0) do
      {^name, value} -> value
      nil -> nil
    end
  end

  describe "headers/3" do
    test "asks for the stream and for its pacing" do
      headers = Request.headers(pacing(300, 250), 0, config())

      assert header(headers, "accept") == "text/event-stream"
      assert header(headers, "dmex-batch-window") == "300"
      assert header(headers, "dmex-max-batch-size") == "250"
    end

    # Sending the granted values instead makes every re-parameterization a no-op
    # on the wire while the controller believes it took effect — which is
    # exactly the bug this separation exists to make visible.
    test "sends what is requested, which is not what was last granted" do
      headers = Request.headers(pacing(50, 10), 0, config(batch_window: 200, max_batch_size: 500))

      assert header(headers, "dmex-batch-window") == "50"
      assert header(headers, "dmex-max-batch-size") == "10"
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
    # EventSource sends, so the producer needs only one code path.
    test "start: :beginning with no cursor sends no header" do
      assert Request.last_event_id(:none, config(start: :beginning)) == []
    end

    test "start: :latest with no cursor sends the literal" do
      assert Request.last_event_id(:none, config(start: :latest)) == [{"last-event-id", "latest"}]
    end

    test "start: an id with no cursor sends that id" do
      assert Request.last_event_id(:none, config(start: 500)) == [{"last-event-id", "500"}]
    end

    test "headers/3 carries it through" do
      assert Request.headers(pacing(), :none, config(start: :latest))
             |> header("last-event-id") == "latest"

      refute Request.headers(pacing(), :none, config(start: :beginning))
             |> header("last-event-id")
    end
  end

  describe "adopt/2" do
    test "takes the producer's echo as the new operating point" do
      granted =
        Request.adopt(pacing(200, 500), [
          {"dmex-batch-window", "50"},
          {"dmex-max-batch-size", "5000"}
        ])

      assert granted == %{batch_window: 50, max_batch_size: 5_000}
    end

    test "keeps what it had when the producer echoes nothing" do
      # A producer that says nothing is one this consumer has no news about,
      # which is not the same as one that granted zero.
      assert Request.adopt(pacing(200, 500), [{"content-type", "text/event-stream"}]) ==
               pacing(200, 500)
    end

    test "ignores an unparseable or non-positive echo" do
      for value <- ["", "lots", "-1", "0", "1.5", "50abc"] do
        assert Request.adopt(pacing(200, 500), [{"dmex-batch-window", value}]) ==
                 pacing(200, 500),
               "expected #{inspect(value)} to be ignored"
      end
    end

    test "adopts each header independently" do
      assert Request.adopt(pacing(200, 500), [{"dmex-max-batch-size", "10"}]) ==
               %{batch_window: 200, max_batch_size: 10}
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

  describe "against the producer" do
    # The point of having both halves as pure modules: what one sends, the other
    # accepts — asserted rather than assumed.
    test "everything this sends, DMEx.Producer.Params parses" do
      headers = Request.headers(pacing(300, 250), 42, config())

      assert {:ok, params} = Params.parse([], headers, Config.producer!([]))

      assert params.cursor == 42
      assert params.batch_window == 300
      assert params.max_batch_size == 250
    end

    test "and what it echoes back, this adopts" do
      {:ok, params} =
        Params.parse(
          [],
          Request.headers(pacing(1, 999_999), :none, config()),
          Config.producer!([])
        )

      granted = Request.adopt(pacing(1, 999_999), Params.resp_headers(params))

      # Clamped by the producer, and the consumer now reasons about the clamped
      # figures rather than about what it asked for.
      assert granted == %{batch_window: 50, max_batch_size: 5_000}
    end

    test "start: :beginning round-trips as the whole log" do
      headers = Request.headers(pacing(), :none, config(start: :beginning))

      assert {:ok, %{cursor: 0}} = Params.parse([], headers, Config.producer!([]))
    end

    test "start: :latest round-trips as the tip" do
      headers = Request.headers(pacing(), :none, config(start: :latest))

      assert {:ok, %{cursor: :latest}} = Params.parse([], headers, Config.producer!([]))
    end
  end
end
