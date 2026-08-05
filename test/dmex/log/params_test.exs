defmodule DMEx.Log.ParamsTest do
  use DMEx.Case, async: true

  alias DMEx.Handler.Fingerprint
  alias DMEx.Log.Params

  describe "Last-Event-ID" do
    test "absent means from now on" do
      # Which is also what a fresh browser EventSource sends, and what the log
      # resolves to its own tip.
      assert {:ok, %Params{cursor: :now}} = Params.parse([], [])
    end

    test "an integer id resumes after it" do
      for {header, expected} <- [
            {"0", 0},
            {"1", 1},
            {"42", 42},
            {"9007199254740993", 9_007_199_254_740_993}
          ] do
        assert {:ok, %Params{cursor: ^expected}} = parse("last-event-id", header)
      end
    end

    # There is deliberately no tip token: the header's absence is one already.
    test "`latest` is not vocabulary" do
      assert {:error, {:invalid, "last-event-id", "latest"}} = parse("last-event-id", "latest")
    end

    # One integer plus absent is small enough to validate exactly, and a client
    # sending something outside it has a bug worth hearing about rather than
    # being handed an arbitrary slice of the log.
    test "anything else is rejected" do
      rejected = [
        "-1",
        "+5",
        "1.0",
        "1e3",
        " 42",
        "42 ",
        "",
        "LATEST",
        "Latest",
        "abc",
        "42abc",
        "0x2a",
        "١٢٣",
        # Timestamps are not vocabulary at all any more, nor is the old
        # `previous/current` pair.
        "2026-08-03T10:00:00Z",
        "2026-08-03",
        "1041/1042"
      ]

      for value <- rejected do
        assert {:error, {:invalid, "last-event-id", ^value}} =
                 parse("last-event-id", value),
               "expected #{inspect(value)} to be rejected"
      end
    end

    test "the header sent twice is rejected" do
      # There is no sensible way to pick between two resume positions.
      headers = [{"last-event-id", "1"}, {"last-event-id", "2"}]

      assert {:error, {:invalid, "last-event-id", "1, 2"}} = Params.parse([], headers)
    end
  end

  # There are no DMEx request headers at all. A connect carries a path and
  # `last-event-id`, which means DMEx's own consumer and a browser's EventSource
  # send byte-identical requests for the same subscription.
  describe "pacing is not in the request" do
    test "a stray pacing header is simply an unknown header, not an error" do
      assert {:ok, %Params{cursor: :now, selection: :all}} =
               Params.parse([], [{"dmex-batch-window", "nonsense"}])
    end
  end

  describe "describe/1" do
    test "names the offending header and value" do
      assert Params.describe({:invalid, "last-event-id", "abc"}) ==
               ~s(invalid last-event-id header: "abc")
    end

    test "a bad filter segment says what a filter is" do
      message = Params.describe({:invalid, "filter", "abc"})

      assert message =~ "invalid filter segment"
      assert message =~ "fingerprint-spec.md"
    end
  end

  describe "the selection path segment" do
    test "no segment at all is the whole log" do
      # v1's behaviour exactly, and what a browser EventSource pointed at the
      # plain URL asks for. It stays the default for anything that does not
      # know selection exists.
      assert {:ok, %Params{selection: :all}} = Params.parse([], [])
    end

    test "a filter blob decodes into a selection" do
      {:ok, filter} = Fingerprint.build(["A.B", "C.D"])
      blob = Fingerprint.encode(filter)

      assert {:ok, %Params{selection: %Fingerprint{}} = params} =
               Params.parse([blob], [])

      assert Fingerprint.member?(params.selection, "A.B")
      assert Fingerprint.member?(params.selection, "C.D")
    end

    test "a segment that is not a decodable blob is a 400" do
      assert {:error, {:invalid, "filter", "not-a-blob!"}} =
               Params.parse(["not-a-blob!"], [])
    end

    test "more segments than one is a 400" do
      assert {:error, {:invalid, "filter", "a/b"}} = Params.parse(["a", "b"], [])
    end

    test "the selection is parsed before the headers are" do
      # A request wrong in two ways should be reported on its path, since that
      # is the one that decides which resource was even asked for.
      assert {:error, {:invalid, "filter", "nope!"}} =
               Params.parse(["nope!"], [{"last-event-id", "garbage"}])
    end
  end

  defp parse(header, value), do: Params.parse([], [{header, value}])
end
