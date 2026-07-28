# Publish-to-handler latency, and the one number that reveals a broken wakeup path.
#
# End to end this composes: NOTIFY delivery, the producer's `batch_window`, one
# read, frame encode, the wire, frame parse, cast, and the consumer's per-message
# transaction commit.
#
# **Read the quiet case first.** On a stream that has been idle, the batch
# window owes the arriving message nothing — the producer's loop is parked in
# `await_wakeup/1` and the NOTIFY wakes it immediately — so latency should be
# single-digit milliseconds. Two failures show up here and nowhere else:
#
#   * near the *poll interval* (1 s by default) means the notification path is
#     not working and the poller is silently covering for it, which this design
#     could otherwise ship with unnoticed because everything still arrives;
#   * near a whole `batch_window` means the throttle is being measured from the
#     wrong instant, charging an idle stream a full window for the very wakeup
#     that ended its idleness. That was a real defect this benchmark caught.
#
# The busy case should then average about `batch_window / 2` and peak at one
# whole window, because a message waits for the next frame boundary and those
# are uniformly distributed relative to publishes.

alias DMEx.BenchHelper, as: Helper
alias DMEx.Consumer
alias DMEx.Test.IntegrationProducer, as: Producer
alias DMEx.Test.Messages.OrderPlaced

Helper.section("end_to_end_latency — publish to handler, percentiles")

defmodule Bench.LatencyHandler do
  @moduledoc false
  use DMEx.Consumer.Handler,
    producer: :bench,
    id: :latency,
    messages: [DMEx.Test.Messages.OrderPlaced]

  alias DMEx.Test.Messages.OrderPlaced

  @impl true
  def handle_message(%OrderPlaced{order_id: sent_at}, _metadata) do
    # Stamped *here*, in the handler, at the moment of delivery. Stamping it in
    # the receiving process instead would measure "publish to whenever the
    # benchmark got round to reading its mailbox", which on a run that publishes
    # before it collects is the publishing loop's own duration.
    send(
      :persistent_term.get(:bench_latency_owner),
      {:arrived, sent_at, System.monotonic_time(:microsecond)}
    )

    :ok
  end
end

Helper.start_repo!()
Helper.reset_log!()

Helper.start_producer!()

:persistent_term.put(:bench_latency_owner, self())

url = Helper.start_server!() <> "/stream"

Helper.start_consumer!(Bench.Consumer, url)

{:ok, _} = Consumer.subscribe(Bench.LatencyHandler, consumer: Bench.Consumer)

# The publish timestamp travels inside the message, so both ends are read from
# the same monotonic clock on the same node — no clock skew to reason about.
percentiles = fn label, latencies ->
  sorted = Enum.sort(latencies)
  at = fn q -> Enum.at(sorted, min(round(length(sorted) * q), length(sorted) - 1)) / 1000 end

  IO.puts(
    "  #{label}  p50 #{Float.round(at.(0.5), 1)}ms" <>
      "   p95 #{Float.round(at.(0.95), 1)}ms" <>
      "   p99 #{Float.round(at.(0.99), 1)}ms" <>
      "   max #{Float.round(at.(1.0), 1)}ms"
  )
end

await = fn sent_at ->
  receive do
    {:arrived, ^sent_at, arrived_at} -> arrived_at - sent_at
  after
    60_000 -> raise "nothing arrived within 60s — is the wakeup path working?"
  end
end

# One in flight at a time, with a gap: the stream is idle when each publish
# lands, so this isolates wakeup latency from framing.
quiet = fn count, interval_ms ->
  for _ <- 1..count do
    sent_at = System.monotonic_time(:microsecond)
    {:ok, _} = Producer.publish(%OrderPlaced{order_id: sent_at})
    latency = await.(sent_at)
    Process.sleep(interval_ms)
    latency
  end
end

# Publishes first, arrivals after — deliberately *not* one at a time.
#
# Measuring a busy stream with a synchronous publish/await loop measures the
# wrong thing: every publish would land immediately after the frame that
# delivered its predecessor, so every message would wait a full `batch_window`
# and the answer would always be `batch_window`, whatever the code did. Firing
# them independently spreads publishes across the window the way real traffic
# does, which is what makes the expected average `batch_window / 2`.
#
# The rate is also deliberately kept *below* what the consumer can drain. Push
# past that and this stops measuring framing and starts measuring backlog: at
# 500 back-to-back publishes against a consumer doing ~155 rows/s, p50 comes out
# near two seconds, which is a queueing figure rather than a latency one.
busy = fn count, interval_ms ->
  sent =
    for _ <- 1..count do
      sent_at = System.monotonic_time(:microsecond)
      {:ok, _} = Producer.publish(%OrderPlaced{order_id: sent_at})
      Process.sleep(interval_ms)
      sent_at
    end

  Enum.map(sent, await)
end

# Let the subscription connect before timing anything.
Process.sleep(500)

IO.puts("\nlatency from publish to handler:\n")
percentiles.("quiet (one publish per 200ms, stream idle between) ", quiet.(50, 200))
percentiles.("busy  (50/s, several messages per frame)           ", busy.(150, 20))

IO.puts("""

  Quiet is wakeup latency: NOTIFY, one read, encode, the wire, and the
  consumer's commit — the batch window owes an idle stream nothing.
  Busy should average about batch_window/2, since a message waits for the next
  frame boundary and those are uniformly distributed relative to publishes. If
  it comes out far higher, the publish rate has exceeded what the consumer can
  drain and the number is queueing delay rather than framing delay — check it
  against bench/e2e/catchup_drain.exs.
""")
