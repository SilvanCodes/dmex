defmodule DMEx.Consumer.ControlTest do
  use DMEx.Case, async: true

  alias DMEx.Config
  alias DMEx.Consumer.Control
  alias DMEx.Consumer.Meter

  defp config(opts \\ []), do: Config.consumer!(opts)

  # A meter warm at a given service time, without pretending to run a pipeline.
  defp meter(service_ms, opts \\ []) do
    config = config(opts)

    Enum.reduce(1..config.min_samples, Meter.new(config), fn i, acc ->
      Meter.sample(acc, service_ms, i)
    end)
  end

  defp pacing(window \\ 200, size \\ 500), do: %{batch_window: window, max_batch_size: size}

  defp decide(control, opts) do
    Control.decide(control, %{
      meter: Keyword.get(opts, :meter, meter(1)),
      granted: Keyword.get(opts, :granted, pacing()),
      requested: Keyword.get(opts, :requested, pacing()),
      config: Keyword.get(opts, :config, config()),
      now: Keyword.fetch!(opts, :now)
    })
  end

  # Pressure held from `since` until `now`.
  defp pressured(since, now) do
    Control.new()
    |> Control.observe(:high, since)
    |> Control.observe(:high, now)
  end

  defp quiet(since, now) do
    Control.new()
    |> Control.observe(:low, since)
    |> Control.observe(:low, now)
  end

  describe "when nothing is wrong" do
    test "an idle stream never adapts" do
      assert {:hold, _} = decide(quiet(0, 100), now: 100)
    end

    test "a stream keeping up never adapts" do
      control = Control.observe(Control.new(), :low, 0)

      assert {:hold, _} = decide(control, now: 5_000)
    end

    # A single burst must not trip it: the producer has to have had a full
    # window to deliver a frame the consumer had no room for.
    test "one moment of pressure is not enough" do
      control = Control.observe(Control.new(), :high, 0)

      assert {:hold, _} = decide(control, now: 100, granted: pacing(200, 500))
    end

    test "adaptive: false pins the pacing however bad it gets" do
      assert {:hold, _} =
               decide(pressured(0, 10_000), now: 10_000, config: config(adaptive: false))
    end
  end

  describe "backing off" do
    test "sizes the next request to what the handler was measured to drain" do
      # 4ms per row over a 200ms window is 50 rows.
      assert {:adapt, requested, _} =
               decide(pressured(0, 1_000),
                 now: 1_000,
                 meter: meter(4),
                 granted: pacing(200, 500)
               )

      assert requested.max_batch_size == 50
      assert requested.batch_window == 200
    end

    test "sizes against what the producer granted, not what was asked for" do
      # Asked for a 200ms window, granted 100ms — so the target is half.
      # Adapting against the request would be adapting against a number that
      # never took effect.
      assert {:adapt, requested, _} =
               decide(pressured(0, 1_000),
                 now: 1_000,
                 meter: meter(4),
                 granted: pacing(100, 500),
                 requested: pacing(200, 500)
               )

      assert requested.max_batch_size == 25
    end

    test "never asks for more than the configured maximum" do
      # A handler at 0.01ms/row could drain 20 000 rows per window. Adaptation
      # only ever moves max_batch_size *below* the configured value, so there is
      # nothing here to ask for and pressure must be coming from somewhere the
      # controller cannot fix.
      assert {:hold, _} =
               decide(pressured(0, 1_000),
                 now: 1_000,
                 meter: meter(0.01),
                 granted: pacing(200, 500),
                 requested: pacing(200, 500)
               )
    end

    test "backs off to exactly one window's worth of drain, not to a fraction" do
      # 1ms per row over a 200ms window is 200 rows — below the configured 500,
      # so this is a real back-off rather than a cap.
      assert {:adapt, requested, _} =
               decide(pressured(0, 1_000),
                 now: 1_000,
                 meter: meter(1),
                 granted: pacing(200, 500),
                 requested: pacing(200, 500)
               )

      assert requested.max_batch_size == 200
    end

    test "holds while the estimate is not yet warm" do
      # Re-parameterizing off two or three samples is how a control loop ends up
      # chasing its own warm-up.
      cold = Meter.sample(Meter.new(config()), 100, 0)

      assert {:hold, _} = decide(pressured(0, 1_000), now: 1_000, meter: cold)
    end

    test "widens the window once frame size has nowhere left to go" do
      assert {:adapt, requested, _} =
               decide(pressured(0, 1_000),
                 now: 1_000,
                 meter: meter(1_000),
                 granted: pacing(200, 1),
                 requested: pacing(200, 1)
               )

      # Lower the frame *rate* once frame *size* is at the floor.
      assert requested.max_batch_size == 1
      assert requested.batch_window == 400
    end

    test "stops widening at its own ceiling" do
      assert {:hold, _} =
               decide(pressured(0, 1_000),
                 now: 1_000,
                 meter: meter(1_000),
                 granted: pacing(5_000, 1),
                 requested: pacing(5_000, 1),
                 config: config(batch_window_ceiling: 5_000)
               )
    end
  end

  describe "recovering" do
    test "steps max_batch_size back toward the configured value" do
      assert {:adapt, requested, _} =
               decide(quiet(0, 1_000),
                 now: 1_000,
                 requested: pacing(200, 50),
                 config: config(max_batch_size: 500, recovery_percent: 25)
               )

      # A quarter of the configured maximum per step.
      assert requested.max_batch_size == 175
    end

    test "does not overshoot the configured value" do
      assert {:adapt, requested, _} =
               decide(quiet(0, 1_000), now: 1_000, requested: pacing(200, 490))

      assert requested.max_batch_size == 500
    end

    # An oversized max_batch_size costs nothing on a quiet stream — it is a
    # LIMIT on the producer's read, not a demand — while a widened window is a
    # floor on frame rate, and so sits in the latency of every message.
    test "unwinds the window before the batch size" do
      assert {:adapt, requested, _} =
               decide(quiet(0, 1_000),
                 now: 1_000,
                 requested: pacing(600, 50),
                 config: config(batch_window: 200, max_batch_size: 500)
               )

      assert requested.batch_window == 400
      assert requested.max_batch_size == 50
    end

    test "then the batch size, once the window is home" do
      assert {:adapt, requested, _} =
               decide(quiet(0, 1_000), now: 1_000, requested: pacing(200, 50))

      assert requested.batch_window == 200
      assert requested.max_batch_size > 50
    end

    test "holds once both are back where they started" do
      assert {:hold, _} = decide(quiet(0, 1_000), now: 1_000, requested: pacing(200, 500))
    end

    test "waits out several quiet windows before starting" do
      # Back off fast, recover slowly.
      assert {:hold, _} =
               decide(quiet(0, 300),
                 now: 300,
                 requested: pacing(200, 50),
                 config: config(recovery_windows: 3, batch_window: 200)
               )
    end
  end

  describe "hysteresis" do
    # Depth hovering at one threshold must not oscillate the decision, which is
    # why clearing pressure needs depth well below the mark that raised it.
    test "the hysteresis band neither sustains pressure nor counts as quiet" do
      control =
        Control.new()
        |> Control.observe(:high, 0)
        |> Control.observe(:normal, 10)

      assert {:hold, _} = decide(control, now: 10_000, requested: pacing(200, 50))
    end

    test "falling all the way to the low mark is what clears it" do
      control =
        Control.new()
        |> Control.observe(:high, 0)
        |> Control.observe(:low, 10)
        |> Control.observe(:low, 2_000)

      assert {:adapt, _requested, _} = decide(control, now: 2_000, requested: pacing(200, 50))
    end
  end

  describe "the minimum interval between changes" do
    test "refuses a second change too soon after the first" do
      {:adapt, _requested, control} =
        decide(pressured(0, 1_000), now: 1_000, meter: meter(4), granted: pacing(200, 500))

      # Still pressured, but a re-parameterization costs a close, a local drain
      # and a reconnect — the interval has to exceed one whole cycle.
      control = Control.observe(control, :high, 1_100)

      assert {:hold, _} =
               decide(control, now: 2_000, meter: meter(4), granted: pacing(200, 500))
    end

    test "allows one once the interval has elapsed" do
      {:adapt, _requested, control} =
        decide(pressured(0, 1_000), now: 1_000, meter: meter(4), granted: pacing(200, 500))

      control =
        control
        |> Control.observe(:high, 40_000)
        |> Control.observe(:high, 41_000)

      assert {:adapt, _requested, _} =
               decide(control,
                 now: 41_000,
                 meter: meter(8),
                 granted: pacing(200, 50),
                 requested: pacing(200, 50)
               )
    end

    test "a change resets both the pressure and quiet clocks" do
      # Otherwise the very observation that triggered a change would immediately
      # re-trigger the next one.
      {:adapt, _requested, control} =
        decide(pressured(0, 1_000), now: 1_000, meter: meter(4), granted: pacing(200, 500))

      assert control.pressure_since == nil
      assert control.quiet_since == nil
      assert control.changed_at == 1_000
    end
  end
end
