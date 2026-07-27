defmodule DMEx.Consumer.MeterTest do
  use DMEx.Case, async: true

  alias DMEx.Config
  alias DMEx.Consumer.Meter

  defp meter(opts \\ []), do: Meter.new(Config.consumer!(opts))

  defp feed(meter, durations, opts \\ []) do
    step = Keyword.get(opts, :step, 1)
    start = Keyword.get(opts, :from, 0)

    durations
    |> Enum.with_index()
    |> Enum.reduce(meter, fn {duration, i}, acc ->
      Meter.sample(acc, duration, start + i * step)
    end)
  end

  describe "before any samples" do
    test "has no estimate and is not warm" do
      assert Meter.mean(meter()) == nil
      assert Meter.drain_rate(meter()) == nil
      refute Meter.warm?(meter())
    end

    test "reports no utilization rather than dividing by nothing" do
      assert Meter.utilization(meter(), 1_000) == 0.0
    end
  end

  describe "the estimate" do
    test "starts at the first sample rather than at zero" do
      # Seeding an EWMA at 0 would make the first dozen samples meaningless.
      assert Meter.mean(feed(meter(), [10])) == 10.0
    end

    test "converges toward a steady service time" do
      mean = Meter.mean(feed(meter(), List.duplicate(4, 60)))

      assert_in_delta mean, 4.0, 0.01
    end

    test "is immune to one slow row" do
      steady = feed(meter(), List.duplicate(10, 60))
      spiked = Meter.sample(steady, 1_000, 100)

      # alpha 0.2, so one outlier moves it by a fifth of the gap rather than
      # replacing the estimate.
      assert Meter.mean(spiked) < 220
    end

    test "follows a genuine change in handler speed" do
      slowed = feed(meter(), List.duplicate(10, 40) ++ List.duplicate(50, 40))

      assert_in_delta Meter.mean(slowed), 50.0, 1.0
    end

    test "drain rate is the reciprocal, in rows per millisecond" do
      assert_in_delta Meter.drain_rate(feed(meter(), List.duplicate(4, 60))), 0.25, 0.01
    end
  end

  describe "warm-up" do
    test "is not warm below the minimum sample count" do
      refute Meter.warm?(feed(meter(min_samples: 20), List.duplicate(5, 19)))
    end

    test "is warm at it" do
      assert Meter.warm?(feed(meter(min_samples: 20), List.duplicate(5, 20)))
    end

    test "has an estimate before it is warm, just not a trusted one" do
      # The controller reads warm?/1, not mean/1, precisely so that an untrusted
      # estimate cannot drive a re-parameterization.
      early = feed(meter(min_samples: 20), List.duplicate(5, 3))

      assert Meter.mean(early)
      refute Meter.warm?(early)
    end
  end

  describe "idleness" do
    # Indexed by sample, not by clock. This is the whole reason service time is
    # the primitive: an hour of silence must not decay the estimate toward zero
    # and make every quiet consumer re-learn its own capacity.
    test "an hour of silence does not move the estimate" do
      warm = feed(meter(), List.duplicate(10, 30))
      before = Meter.mean(warm)

      # No samples for an hour, then one more at the same speed.
      later = Meter.sample(warm, 10, 3_600_000)

      assert_in_delta Meter.mean(later), before, 0.01
    end

    test "utilization falls toward zero on a quiet stream" do
      warm = feed(meter(), List.duplicate(1, 20))

      assert Meter.utilization(warm, 3_600_000) < 0.01
    end
  end

  describe "staleness" do
    # After a long silence the estimate is stale rather than wrong: it is kept
    # as the starting point, since a handler's speed rarely changes for free and
    # it is the only evidence there is — but it must be re-warmed before it may
    # drive anything.
    test "a long gap keeps the mean but drops the confidence" do
      warm = feed(meter(min_samples: 5, sample_staleness: 1_000), List.duplicate(10, 10))
      assert Meter.warm?(warm)

      stale = Meter.sample(warm, 10, 100_000)

      assert_in_delta Meter.mean(stale), 10.0, 0.01
      refute Meter.warm?(stale)
    end

    test "re-warming costs only the rows that were arriving anyway" do
      warm = feed(meter(min_samples: 5, sample_staleness: 1_000), List.duplicate(10, 10))
      stale = Meter.sample(warm, 10, 100_000)

      rewarmed = feed(stale, List.duplicate(10, 5), from: 100_001)

      assert Meter.warm?(rewarmed)
    end

    test "a gap inside the staleness window changes nothing" do
      warm = feed(meter(min_samples: 5, sample_staleness: 10_000), List.duplicate(10, 10))

      assert Meter.warm?(Meter.sample(warm, 10, 5_000))
    end
  end

  describe "utilization" do
    test "is the fraction of the window the pipeline had work" do
      # Ten 1ms samples spread across 100ms of wall clock.
      busy = feed(meter(), List.duplicate(1, 10), step: 10)

      assert_in_delta Meter.utilization(busy, 100), 0.1, 0.05
    end

    test "never exceeds one, however the samples fall" do
      saturated = feed(meter(), List.duplicate(100, 10), step: 1)

      assert Meter.utilization(saturated, 10) <= 1.0
    end
  end
end
