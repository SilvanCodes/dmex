# Append-to-handler latency, and the one number that reveals a broken wakeup path.
#
# End to end this composes: NOTIFY delivery, the log's batch interval, one
# read, frame encode, the wire, frame parse, cast, and the consumer's per-message
# transaction commit.
#
# **Read the quiet case first.** On a stream that has been idle, the batch
# window owes the arriving message nothing — the log's loop is parked in
# `await_wakeup/1` and the NOTIFY wakes it immediately — so latency should be
# single-digit milliseconds. Two failures show up here and nowhere else:
#
#   * near the *poll interval* (1 s by default) means the notification path is
#     not working and the poller is silently covering for it, which this design
#     could otherwise ship with unnoticed because everything still arrives;
#   * near a whole batch interval means the throttle is being measured from the
#     wrong instant, charging an idle stream a full window for the very wakeup
#     that ended its idleness. That was a real defect this benchmark caught.
#
# The busy case should then average about half a batch interval and peak at one
# whole window, because a message waits for the next frame boundary and those
# are uniformly distributed relative to appends.

alias DMEx.BenchHelper, as: Helper
alias DMEx.Test.IntegrationLog, as: Log
alias DMEx.Test.Messages.OrderPlaced

Helper.section("end_to_end_latency — append to handler, percentiles")

defmodule Bench.LatencyHandler do
  @moduledoc false
  use DMEx.Handler, logs: [:bench], id: :latency

  @impl true
  def handle_message(%OrderPlaced{order_id: sent_at}, _metadata) do
    # Stamped *here*, in the handler, at the moment of delivery. Stamping it in
    # the receiving process instead would measure "append to whenever the
    # benchmark got round to reading its mailbox", which on a run that appends
    # before it collects is the appending loop's own duration.
    send(
      :persistent_term.get(:bench_latency_owner),
      {:arrived, sent_at, System.monotonic_time(:microsecond)}
    )

    :ok
  end
end

Helper.start_repo!()
Helper.reset_log!()

Helper.start_log!()

:persistent_term.put(:bench_latency_owner, self())

url = Helper.start_server!() <> "/stream"

Helper.start_supervisor!(Bench.Supervisor, url)

{:ok, _} = DMEx.Supervisor.subscribe(Bench.LatencyHandler, supervisor: Bench.Supervisor)

# The append timestamp travels inside the message, so both ends are read from
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

# One in flight at a time, with a gap: the stream is idle when each append
# lands, so this isolates wakeup latency from framing.
quiet = fn count, interval_ms ->
  for _ <- 1..count do
    sent_at = System.monotonic_time(:microsecond)
    {:ok, _} = Log.append(%OrderPlaced{order_id: sent_at})
    latency = await.(sent_at)
    Process.sleep(interval_ms)
    latency
  end
end

# Publishes first, arrivals after — deliberately *not* one at a time.
#
# Measuring a busy stream with a synchronous append/await loop measures the
# wrong thing: every append would land immediately after the frame that
# delivered its predecessor, so every message would wait a full interval and the
# answer would always be that interval, whatever the code did. Firing
# them independently spreads appends across the window the way real traffic
# does, which is what makes the expected average half an interval.
#
# The rate is deliberately kept *below* what the consumer can drain: past that
# this stops measuring latency and starts measuring backlog.
busy = fn count, interval_ms ->
  sent =
    for _ <- 1..count do
      sent_at = System.monotonic_time(:microsecond)
      {:ok, _} = Log.append(%OrderPlaced{order_id: sent_at})
      Process.sleep(interval_ms)
      sent_at
    end

  Enum.map(sent, await)
end

# Let the subscription connect before timing anything.
Process.sleep(500)

IO.puts("\nlatency from append to handler:\n")
percentiles.("quiet (one append per 200ms, stream idle between) ", quiet.(50, 200))
percentiles.("busy  (50/s, several messages per frame)           ", busy.(150, 20))

IO.puts("""

  Quiet is wakeup latency: NOTIFY, one read, encode, the wire, and the
  consumer's commit — the batch window owes an idle stream nothing.
  Busy should average about half a batch interval, since a message waits for the next
  frame boundary and those are uniformly distributed relative to appends. If
  it comes out far higher, the append rate has exceeded what the consumer can
  drain and the number is queueing delay rather than framing delay — check it
  against bench/e2e/catchup_drain.exs.
""")
