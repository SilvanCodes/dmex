# A one-off sweep over `DMEx.Handler.Fingerprint`, measuring what the selection design
# asserts rather than what it costs:
#
#     MIX_ENV=test mix bench.fingerprint
#
# Three claims this sweep checks:
#
#   1. **No false negatives, ever.** The whole scheme was chosen for this. A
#      single miss anywhere in the range is a hard failure here, not a data
#      point — it would be silent message loss in production.
#   2. **A spurious-type rate near 2^-16.** Membership is tested on the *type
#      string*, not per row, so a false positive admits one entire message type
#      for the whole life of a connection. This is what justifies pinning the
#      width at 16 rather than 8.
#   3. **~2.46 bytes per type**, which is what puts the request-line ceiling in
#      the low thousands of types and therefore what makes the URL viable.
#
# Not a benchee benchmark: there is nothing here to time. It is an analysis, and
# it prints a CSV plus a summary.

alias DMEx.Handler.Fingerprint

# Two separate things, and conflating them is what makes the number useless.
#
# The **ratio** models the roadmap's worked example: a log knowing ~10x
# the types a consumer selects. That is the operationally meaningful figure —
# how much extra traffic a real subscription admits.
#
# The **rate** is a property of the filter, and estimating 2^-16 from a 10N
# universe is hopeless: at n=100 that is 1000 probes and 0.015 expected hits.
# So every point is additionally probed against a large fixed stranger set, and
# those are pooled across the sweep to get an estimate with real power.
universe_multiple = 10
rate_probes = 50_000

# Enough points to see a trend across 1..2000 without building 2000 filters,
# weighted toward the small end where the +32 slack still governs.
counts =
  Enum.uniq(
    Enum.to_list(1..20) ++
      Enum.to_list(25..100//5) ++
      Enum.to_list(120..500//20) ++
      Enum.to_list(550..2000//50)
  )

member = fn i -> "MyApp.Messages.Type#{i}" end
stranger = fn i -> "OtherApp.Events.Kind#{i}" end

# Probe key-hashes are SHA-256 of a string and independent of any filter, so
# they are computed once for the whole sweep. Without this the sweep would
# re-hash the same few hundred thousand strings at every point, and the run
# would be minutes rather than seconds.
max_strangers = max(universe_multiple * Enum.max(counts), rate_probes)
stranger_hashes = for i <- 1..max_strangers, do: Fingerprint.key_hash(stranger.(i))
member_hashes = for i <- 1..Enum.max(counts), do: Fingerprint.key_hash(member.(i))

IO.puts("""
DMEx.Handler.Fingerprint sweep — xor16
  set sizes:  #{length(counts)} points over 1..#{Enum.max(counts)}
  universe:   N selected + #{universe_multiple}N unselected (the ratio column)
  rate:       #{rate_probes} non-members per point, pooled (the rate estimate)
  probes:     #{max_strangers} distinct non-members, hashed once
""")

IO.puts("n,slots,raw_bytes,blob_chars,bytes_per_type,recovered,recovered_over_encoded,false_positives")

results =
  Enum.map(counts, fn n ->
    types = Enum.map(1..n, member)

    {:ok, filter} = Fingerprint.build(types)

    # Through the wire form, not the in-memory struct: an encoding bug that only
    # showed up after a round trip is exactly the kind this is here to catch.
    {:ok, filter} = filter |> Fingerprint.encode() |> Fingerprint.decode()

    blob_chars = byte_size(Fingerprint.encode(filter))
    raw_bytes = byte_size(filter.fingerprints)

    # Claim 1. Not a statistic — an invariant.
    missing =
      member_hashes
      |> Enum.take(n)
      |> Enum.count(&(not Fingerprint.member_hash?(filter, &1)))

    if missing > 0 do
      raise """
      FALSE NEGATIVE at n=#{n}: #{missing} of #{n} selected types were not recovered.

      This is silent message loss. Stop and fix the filter before anything ships.
      """
    end

    # Claim 2, from the reader's side: of everything the log knows, how
    # much did this filter admit? N is the floor; anything above it is spurious.
    false_positives =
      stranger_hashes
      |> Enum.take(universe_multiple * n)
      |> Enum.count(&Fingerprint.member_hash?(filter, &1))

    recovered = n + false_positives

    # The same question at a sample size that can actually answer it. Disjoint
    # from nothing — it is a superset of the probes above — so it is pooled on
    # its own rather than added to them.
    rate_hits =
      stranger_hashes
      |> Enum.take(rate_probes)
      |> Enum.count(&Fingerprint.member_hash?(filter, &1))

    row = %{
      n: n,
      slots: Fingerprint.slots(filter),
      raw_bytes: raw_bytes,
      blob_chars: blob_chars,
      bytes_per_type: raw_bytes / n,
      recovered: recovered,
      ratio: recovered / n,
      false_positives: false_positives,
      rate_hits: rate_hits,
      rate_probed: rate_probes
    }

    IO.puts(
      "#{row.n},#{row.slots},#{row.raw_bytes},#{row.blob_chars}," <>
        "#{Float.round(row.bytes_per_type, 3)},#{row.recovered}," <>
        "#{Float.round(row.ratio, 6)},#{row.false_positives}"
    )

    row
  end)

## Summary

total_probed = results |> Enum.map(& &1.rate_probed) |> Enum.sum()
total_hits = results |> Enum.map(& &1.rate_hits) |> Enum.sum()
expected = 1 / 65_536

# Pooled across the sweep, since no single point has the sample size. Normal
# approximation on a Poisson count.
observed = total_hits / total_probed
stderr = :math.sqrt(max(total_hits, 1)) / total_probed

worst = Enum.max_by(results, & &1.ratio)
largest = List.last(results)

# The request line is the real ceiling: nginx's large_client_header_buffers and
# Apache's LimitRequestLine both default to about 8 KB.
ceiling = trunc(8_192 / (largest.blob_chars / largest.n))

IO.puts("""

── Summary ─────────────────────────────────────────────────────────────

  False negatives          0 across #{Enum.sum(Enum.map(results, & &1.n))} membership tests
                           (the invariant; anything else raises above)

  False-positive rate      #{Float.round(observed * 1.0e6, 2)} per million
                           ± #{Float.round(stderr * 1.0e6, 2)} (pooled over #{total_probed} probes, #{total_hits} hits)
    expected at xor16      #{Float.round(expected * 1.0e6, 2)} per million

  recovered/encoded        1.0 at #{Enum.count(results, &(&1.ratio == 1.0))}/#{length(results)} points
    worst point            #{Float.round(worst.ratio, 5)} at n=#{worst.n} (#{worst.false_positives} spurious type(s))

  Bytes per type           #{Float.round(largest.bytes_per_type, 3)} raw at n=#{largest.n}
    claimed                2.46 (asymptotic; the excess is the
                           +#{largest.slots - ceil(1.23 * largest.n)} slots of peeling slack, which vanishes with n)
    with base64url         #{Float.round(largest.blob_chars / largest.n, 3)} chars/type

  Largest set measured     n=#{largest.n} -> #{largest.blob_chars} chars in the request line
  Request-line ceiling     ~#{ceiling} types at an 8 KB limit

────────────────────────────────────────────────────────────────────────

What to read into it: the ratio column should sit flat at 1.0 with rare
single-type excursions. That flatness *is* the argument for xor16 — at xor8
the same universe would admit a spurious type roughly every other point, and
one unlucky draw means a connection receives an entire high-volume type it
never asked for, for as long as it stays connected.
""")
