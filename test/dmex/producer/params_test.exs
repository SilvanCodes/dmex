defmodule DMEx.Producer.ParamsTest do
  use DMEx.Case, async: true

  alias DMEx.Config
  alias DMEx.Fingerprint
  alias DMEx.Producer.Params

  setup do
    %{config: Config.producer!([])}
  end

  describe "Last-Event-ID" do
    test "absent means the whole log", %{config: config} do
      # Which is also what a fresh browser EventSource sends.
      assert {:ok, %Params{cursor: 0}} = Params.parse([], [], config)
    end

    test "an integer id resumes after it", %{config: config} do
      for {header, expected} <- [
            {"0", 0},
            {"1", 1},
            {"42", 42},
            {"9007199254740993", 9_007_199_254_740_993}
          ] do
        assert {:ok, %Params{cursor: ^expected}} = parse(config, "last-event-id", header)
      end
    end

    test "`latest` starts from the tip", %{config: config} do
      assert {:ok, %Params{cursor: :latest}} = parse(config, "last-event-id", "latest")
    end

    # A vocabulary of two forms plus absent is small enough to validate exactly,
    # and a client sending something outside it has a bug worth hearing about
    # rather than being handed an arbitrary slice of the log.
    test "anything else is rejected", %{config: config} do
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
                 parse(config, "last-event-id", value),
               "expected #{inspect(value)} to be rejected"
      end
    end

    test "the header sent twice is rejected", %{config: config} do
      # There is no sensible way to pick between two resume positions.
      headers = [{"last-event-id", "1"}, {"last-event-id", "2"}]

      assert {:error, {:invalid, "last-event-id", "1, 2"}} = Params.parse([], headers, config)
    end
  end

  describe "pacing headers" do
    test "absent means the producer's defaults", %{config: config} do
      assert {:ok, %Params{batch_window: 200, max_batch_size: 500}} = Params.parse([], [], config)
    end

    test "a well-formed request is honoured when it is in range", %{config: config} do
      assert {:ok, %Params{batch_window: 300}} = parse(config, "dmex-batch-window", "300")
      assert {:ok, %Params{max_batch_size: 250}} = parse(config, "dmex-max-batch-size", "250")
    end

    # Clamped, not rejected: the bounds protect the producer, and a consumer
    # asking for more than it may have should get a working stream at the
    # permitted rate rather than a 400 it cannot interpret.
    test "an out-of-range request is clamped rather than rejected", %{config: config} do
      assert {:ok, %Params{batch_window: 50}} = parse(config, "dmex-batch-window", "0")
      assert {:ok, %Params{batch_window: 5_000}} = parse(config, "dmex-batch-window", "999999")
      assert {:ok, %Params{max_batch_size: 1}} = parse(config, "dmex-max-batch-size", "0")

      assert {:ok, %Params{max_batch_size: 5_000}} =
               parse(config, "dmex-max-batch-size", "999999")
    end

    test "a malformed value is rejected", %{config: config} do
      for header <- ["dmex-batch-window", "dmex-max-batch-size"],
          value <- ["abc", "-1", "", "1.5", "+5", " 1"] do
        assert {:error, {:invalid, ^header, ^value}} = parse(config, header, value),
               "expected #{header}: #{inspect(value)} to be rejected"
      end
    end

    test "respects a producer's own bounds", %{config: _default} do
      config =
        Config.producer!(
          batch_window_floor: 100,
          max_batch_size: 100,
          max_batch_size_ceiling: 100
        )

      assert {:ok, %Params{batch_window: 100}} = parse(config, "dmex-batch-window", "10")
      assert {:ok, %Params{max_batch_size: 100}} = parse(config, "dmex-max-batch-size", "1000")
    end
  end

  describe "resp_headers/1" do
    test "echoes the effective values, not the requested ones" do
      config = Config.producer!([])

      {:ok, params} =
        Params.parse(
          [],
          [{"dmex-batch-window", "1"}, {"dmex-max-batch-size", "999999"}],
          config
        )

      # Without this echo a clamped consumer cannot distinguish "the producer is
      # honouring my request and I still can't keep up" from "the producer
      # capped me" — opposite situations calling for opposite reactions.
      assert Params.resp_headers(params) == [
               {"dmex-batch-window", "50"},
               {"dmex-max-batch-size", "5000"}
             ]
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

  describe "validation order" do
    test "reports the resume position before the pacing" do
      # Resumption is a correctness mechanism and pacing is a tuning knob, so
      # the more consequential one is the one a client hears about first.
      config = Config.producer!([])
      headers = [{"last-event-id", "nope"}, {"dmex-batch-window", "also-nope"}]

      assert {:error, {:invalid, "last-event-id", "nope"}} = Params.parse([], headers, config)
    end
  end

  describe "the selection path segment" do
    test "no segment at all is the whole log", %{config: config} do
      # v1's behaviour exactly, and what a browser EventSource pointed at the
      # plain URL asks for. It stays the default for anything that does not
      # know selection exists.
      assert {:ok, %Params{selection: :all}} = Params.parse([], [], config)
    end

    test "a filter blob decodes into a selection", %{config: config} do
      {:ok, filter} = Fingerprint.build(["A.B", "C.D"])
      blob = Fingerprint.encode(filter)

      assert {:ok, %Params{selection: %Fingerprint{}} = params} =
               Params.parse([blob], [], config)

      assert Fingerprint.member?(params.selection, "A.B")
      assert Fingerprint.member?(params.selection, "C.D")
    end

    test "a segment that is not a decodable blob is a 400", %{config: config} do
      assert {:error, {:invalid, "filter", "not-a-blob!"}} =
               Params.parse(["not-a-blob!"], [], config)
    end

    test "more segments than one is a 400", %{config: config} do
      assert {:error, {:invalid, "filter", "a/b"}} = Params.parse(["a", "b"], [], config)
    end

    # Reserved names are matched *before* any decode is attempted, which is what
    # makes the namespace guarantee structural rather than probabilistic. The two
    # vocabularies could hardly collide anyway — the shortest valid blob is a
    # 64-bit seed plus three fingerprints — but "hardly" is not a guarantee.
    test "a reserved name is a named sub-resource, not a candidate filter", %{config: config} do
      assert {:error, {:reserved, "stats"}} = Params.parse(["stats"], [], config)
    end

    test "the selection is parsed before the headers are", %{config: config} do
      # A request wrong in two ways should be reported on its path, since that
      # is the one that decides which resource was even asked for.
      assert {:error, {:invalid, "filter", "nope!"}} =
               Params.parse(["nope!"], [{"last-event-id", "garbage"}], config)
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

  defp parse(config, header, value), do: Params.parse([], [{header, value}], config)
end
