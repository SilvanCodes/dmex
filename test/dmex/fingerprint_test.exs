defmodule DMEx.FingerprintTest do
  @moduledoc """
  The one invariant that matters here is **no false negatives**.

  A false positive costs one unwanted message type crossing the wire, which the
  consumer's skip path drops. A false negative is silent message loss, and
  nothing downstream can detect or recover from it — so it is asserted as a
  property over generated sets, not sampled with a few examples.

  The fixed vectors exist for the same reason. Log and consumer must agree
  on the encoding *exactly*, and disagreement does not produce a garbled filter
  that fails loudly — it produces false negatives. A vector pinned to a literal
  string is what makes changing the hash, the width or the byte order break a
  test rather than break production silently.
  """

  use DMEx.Case, async: true
  use ExUnitProperties

  alias DMEx.Fingerprint

  # Shaped like the real thing — fully-qualified module names — since that is
  # what the size claims are quoted against.
  defp types(count, prefix \\ "MyApp.Messages.Type") do
    for i <- 1..count, do: "#{prefix}#{i}"
  end

  defp round_trip!(types) do
    {:ok, filter} = Fingerprint.build(types)
    {:ok, decoded} = filter |> Fingerprint.encode() |> Fingerprint.decode()

    decoded
  end

  describe "no false negatives" do
    property "every member of any set survives an encode/decode round trip" do
      check all(
              types <-
                uniq_list_of(string(:alphanumeric, min_length: 1, max_length: 40),
                  min_length: 1,
                  max_length: 200
                ),
              max_runs: 200
            ) do
        filter = round_trip!(types)

        for type <- types do
          assert Fingerprint.member?(filter, type),
                 "#{inspect(type)} was lost from a set of #{length(types)}"
        end
      end
    end

    # The property generates modest sets; this walks the range the request-line
    # ceiling is quoted over, including the awkward small sizes where the
    # peeling constant rather than the asymptotic one governs.
    test "holds across the practical range, including the degenerate small end" do
      for count <- [1, 2, 3, 4, 5, 10, 50, 200, 1_000, 2_000] do
        types = types(count)
        filter = round_trip!(types)

        assert Enum.all?(types, &Fingerprint.member?(filter, &1)),
               "a type was lost at n=#{count}"
      end
    end

    property "unicode and punctuation in a type name are not special" do
      # The key is hashed as UTF-8 bytes, so nothing about the string's content
      # can matter. Worth pinning, since a non-Elixir implementer will reach for
      # whatever their language calls a string hash.
      check all(
              types <-
                uniq_list_of(string(:utf8, min_length: 1, max_length: 30),
                  min_length: 1,
                  max_length: 50
                ),
              max_runs: 100
            ) do
        filter = round_trip!(types)

        assert Enum.all?(types, &Fingerprint.member?(filter, &1))
      end
    end
  end

  describe "the encoding is pinned" do
    # If this test fails, the wire format changed. That is a breaking change to
    # every non-Elixir consumer, and it must never happen by accident.
    test "a known type list encodes to exactly this blob" do
      {:ok, filter} = Fingerprint.build(["A.B", "C.D", "E.F"])

      assert Fingerprint.encode(filter) ==
               "m2c9OqXXKs0AAAAAAAAAAAAAAAB7wNzAAAAAAAAAtPQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    end

    test "the blob is a pure function of the set, not of the order it was written in" do
      # Handler clause order must not mint a new URL: the seed is derived from
      # the *sorted* type list precisely so that reordering `handle_message/2`
      # clauses is a no-op on the wire.
      a = ["B.C", "A.B", "D.E"]
      b = ["D.E", "A.B", "B.C"]

      assert encode(a) == encode(b)
    end

    test "the same set encodes identically across runs" do
      # No randomness anywhere: the same code compiles to the same URL on every
      # machine and every build, which is what makes the blob diffable and
      # correlatable in a proxy log.
      assert encode(types(50)) == encode(types(50))
    end

    test "duplicates collapse, since two clauses matching one struct are one selection" do
      assert encode(["A.B", "C.D"]) == encode(["A.B", "C.D", "A.B"])
    end

    test "a different set encodes differently" do
      refute encode(["A.B"]) == encode(["A.C"])
    end

    defp encode(types) do
      {:ok, filter} = Fingerprint.build(types)

      Fingerprint.encode(filter)
    end
  end

  describe "decode/1 rejects what it cannot trust" do
    test "anything that is not base64url" do
      assert Fingerprint.decode("not base64!!") == {:error, :invalid}
      assert Fingerprint.decode("") == {:error, :invalid}

      # Standard base64's alphabet, which would have needed percent-encoding in
      # a path and is therefore not what DMEx emits.
      assert Fingerprint.decode("a+b/c=") == {:error, :invalid}
    end

    test "a payload too short to hold a seed" do
      assert Fingerprint.decode(Base.url_encode64("short", padding: false)) == {:error, :invalid}
    end

    test "a seed with no fingerprints at all" do
      assert Fingerprint.decode(Base.url_encode64(<<0::64>>, padding: false)) ==
               {:error, :invalid}
    end

    test "an array that is not a whole number of fingerprints" do
      assert Fingerprint.decode(Base.url_encode64(<<0::64, 1, 2, 3>>, padding: false)) ==
               {:error, :invalid}
    end

    test "an array that does not divide into three equal blocks" do
      # Two fingerprints cannot be three blocks, and the block length is what
      # every hash position is derived from — so this is unusable rather than
      # merely odd.
      assert Fingerprint.decode(Base.url_encode64(<<0::64, 0::16, 0::16>>, padding: false)) ==
               {:error, :invalid}
    end

    test "a blob that decodes cleanly is accepted even if it was never built by us" do
      # Worth stating: there is no integrity check and there cannot usefully be
      # one. A well-formed blob under a *different* encoding decodes fine and
      # silently matches the wrong things, which is exactly why the constants
      # are pinned rather than negotiated.
      assert {:ok, %Fingerprint{}} =
               Fingerprint.decode(Base.url_encode64(<<7::64, 0::48>>, padding: false))
    end
  end

  describe "build/1" do
    test "an empty selection is an error rather than a filter matching nothing" do
      assert Fingerprint.build([]) == {:error, :empty}
    end

    test "the array grows with the set, at roughly the documented rate" do
      # ~2.46 bytes per type at xor16 is the claim the request-line ceiling
      # rests on. The +32 slack dominates at small n, so this is asserted where
      # the asymptotic constant actually governs.
      {:ok, filter} = Fingerprint.build(types(2_000))

      bytes_per_type = byte_size(filter.fingerprints) / 2_000

      assert_in_delta bytes_per_type, 2.46, 0.15
    end

    test "a thousand types still fit a conservative request line" do
      {:ok, filter} = Fingerprint.build(types(1_000))

      # base64url expands by 4/3, and this is what lands in every access-log
      # line of every intermediary.
      assert byte_size(Fingerprint.encode(filter)) < 4_096
    end
  end

  describe "member?/2" do
    test "a non-member is usually rejected" do
      # Not "always": inclusion is heuristic. This is a sanity check that the
      # filter discriminates at all — the actual rate is measured by
      # bench/fingerprint_sweep.exs, which has the sample size for it.
      filter = round_trip!(types(100))
      strangers = types(10_000, "Other.Messages.Type")

      admitted = Enum.count(strangers, &Fingerprint.member?(filter, &1))

      assert admitted < 10, "#{admitted}/10000 non-members admitted — far above 2^-16"
    end
  end
end
