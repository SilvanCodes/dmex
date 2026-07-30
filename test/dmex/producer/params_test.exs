defmodule DMEx.Producer.ParamsTest do
  use DMEx.Case, async: true

  alias DMEx.Fingerprint
  alias DMEx.Producer.Params

  describe "Last-Event-ID" do
    test "absent means the whole log" do
      # Which is also what a fresh browser EventSource sends.
      assert {:ok, %Params{cursor: 0}} = Params.parse([], [])
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

    test "`latest` starts from the tip" do
      assert {:ok, %Params{cursor: :latest}} = parse("last-event-id", "latest")
    end

    # A vocabulary of two forms plus absent is small enough to validate exactly,
    # and a client sending something outside it has a bug worth hearing about
    # rather than being handed an arbitrary slice of the log.
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
        "١٢٣"
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
      assert {:ok, %Params{cursor: 0, selection: :all}} =
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

    test "a reserved name says it is reserved rather than malformed" do
      assert Params.describe({:reserved, "stats"}) =~ "reserved sub-resource"
    end
  end

  describe "the selection path segment" do
    test "no segment at all is the whole log" do
      # v1's behaviour exactly, and what a browser EventSource pointed at the
      # plain URL asks for. It stays the default for anything that does not
      # know selection exists.
      assert {:ok, %Params{selection: :all}} = Params.parse([], [])
    end

    # A trailing slash in a producer URL would otherwise be a permanent 400, and
    # a 400 is fatal to a consumer — one stray character killing a subscription
    # forever is not a defensible reading of "GET /stream/".
    test "an empty segment is the same resource as no segment" do
      assert {:ok, %Params{selection: :all}} = Params.parse([""], [])
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

    # Reserved names are matched *before* any decode is attempted, which is what
    # makes the namespace guarantee structural rather than probabilistic. The two
    # vocabularies could hardly collide anyway — the shortest valid blob is a
    # 64-bit seed plus three fingerprints — but "hardly" is not a guarantee.
    test "a reserved name is a named sub-resource, not a candidate filter" do
      assert {:error, {:reserved, "stats"}} = Params.parse(["stats"], [])
    end

    test "the selection is parsed before the headers are" do
      # A request wrong in two ways should be reported on its path, since that
      # is the one that decides which resource was even asked for.
      assert {:error, {:invalid, "filter", "nope!"}} =
               Params.parse(["nope!"], [{"last-event-id", "garbage"}])
    end
  end

  describe "matched_header/1" do
    test "reports how many of the producer's types a filter admitted" do
      # The blob is opaque and cannot be reversed, so this is the only thing
      # that makes a subscription legible from outside: a count of 0 where ten
      # types were selected is an encoding mismatch, visible at connect rather
      # than through silence.
      assert Params.matched_header(7) == [{"dmex-matched-messages", "7"}]
      assert Params.matched_header(0) == [{"dmex-matched-messages", "0"}]
    end
  end

  defp parse(header, value), do: Params.parse([], [{header, value}])
end
