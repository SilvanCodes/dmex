# How fast can a log actually be replayed, and what is the bottleneck?
#
# "Arbitrarily replayable streams" is one of the three things DMEx claims to
# buy, and it is only meaningful with a number attached. This measures three
# stages against the same seeded log, so the answer separates rather than
# conflates them:
#
#   (a) **log-side**: a client that reads and discards. Read plus encode
#       plus the wire, with no consumer cost at all.
#   (b) **end to end, no-op handler**: adds decode, cast, and the consumer's
#       per-row transaction.
#   (c) **end to end, every row skipped**: a whole-log handler, so rows arrive,
#       no handler runs, and their advances coalesce into one cursor write per
#       `checkpoint_interval`.
#
# (c) should sit near (a): a skipped row costs a map lookup and a telemetry
# event, no database work.
#
# Last measured — developer machine, stable across four runs:
#
#   * (a) ~36 000 rows/s, (b) ~265 rows/s, (c) ~39 000 rows/s;
#   * 200 000 skipped rows cost 5 cursor writes across a 5.15 s drain — one per
#     `checkpoint_interval`, or 40 000 rows per write.
#
# The two orders of magnitude between (b) and the other two is the per-row
# commit, which is the handler/cursor guarantee rather than overhead. At ~265
# rows/s a million-row log of entirely handled messages replays in about an
# hour; a mostly-skipped one replays at wire speed. (a) and (c) are paced by the
# log's `max_batch_frequency`, not by anything the design imposes.

alias DMEx.BenchHelper, as: Helper
alias DMEx.Test.Messages.OrderPlaced
alias Ecto.Adapters.SQL

Helper.section("catchup_drain — replay rate, and which stage is the bottleneck")

# 1_000_000 in the design's own framing. 20_000 keeps a run to a few minutes and
# still shows the shape; raise it when the question is "how long would replaying
# our actual log take".
backlog = 20_000

# Stage (c) gets its own, larger backlog, because the two stages want opposite
# things. (b) is commit-bound at a few hundred rows/s, so 20 000 already costs
# well over a minute. (c) runs at wire speed and would drain 20 000 in half a
# second — *inside* one `checkpoint_interval`, so no cursor write would fall due
# and the coalescing ratio the stage exists to measure would come out as zero.
# This is sized to span several checkpoints instead.
skip_backlog = 200_000

defmodule Bench.CountingHandler do
  @moduledoc false
  use DMEx.Handler, logs: [:bench], name: :counting

  @impl true
  def handle_message(%OrderPlaced{}, _metadata) do
    :counters.add(:persistent_term.get(:bench_drain_counter), 1, 1)

    :ok
  end
end

Helper.start_repo!()
Helper.reset_log!()

Helper.start_log!()

# Seeded in one statement: appends are deliberately serialized, and this
# benchmark is about the read path.
seed = fn count ->
  IO.puts("seeding #{count} rows...")

  SQL.query!(
    DMEx.Test.IntegrationRepo,
    """
    INSERT INTO dmex_messages (type, data, metadata)
    SELECT $1, json_build_object('order_id', n, 'note', repeat('x', 200))::text::jsonb, '{}'::jsonb
      FROM generate_series(1, $2) AS n
    """,
    [OrderPlaced.type(), count]
  )
end

seed.(backlog)

base = Helper.start_server!()
port = base |> URI.parse() |> Map.fetch!(:port)
url = base <> "/stream"

report = fn label, rows, micros ->
  seconds = micros / 1_000_000

  IO.puts(
    "  #{String.pad_trailing(label, 34)} " <>
      "#{round(rows / seconds)} rows/s  (#{Float.round(seconds, 2)}s)"
  )
end

# Bounded, and it returns what it actually saw rather than asserting it got
# there. An unbounded wait turns a miscount, a subscription that never connects
# or a `404` into a hang with no output — which is exactly how stage (a)'s
# broken row counter and stage (c)'s selective fingerprint stayed hidden. A
# short run that says "reached N of M" is a bug report; a run that never returns
# is not.
await_timeout = 300_000

await = fn counter, target, label ->
  deadline = System.monotonic_time(:millisecond) + await_timeout

  wait = fn wait ->
    seen = :counters.get(counter, 1)

    cond do
      seen >= target ->
        seen

      System.monotonic_time(:millisecond) >= deadline ->
        IO.puts("  !! #{label} reached only #{seen} of #{target} in #{div(await_timeout, 1_000)}s")

        seen

      true ->
        Process.sleep(50)
        wait.(wait)
    end
  end

  wait.(wait)
end

## (a) Log side only: read, encode, wire.
started = System.monotonic_time(:microsecond)
{:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, mode: :active)
{:ok, conn, ref} = Mint.HTTP.request(conn, "GET", "/stream", [], nil)

# One per row frame, and nothing else on the wire carries it: a heartbeat is a
# comment and the `retry:` frame has no `data:` line. The row's id travels in
# the SSE `id:` field as `previous/current`, *not* as a JSON key — counting
# `"id":` finds only whatever the payload happens to contain, which for this
# benchmark's `order_id` rows is nothing at all.
marker = ~s(data: {"data":)

# A chunk boundary can fall inside the marker, so the tail of each chunk is
# carried into the next. Without this the count silently runs low.
carry = byte_size(marker) - 1

count = fn chunk, held ->
  scanned = held <> chunk

  {
    scanned |> :binary.matches(marker) |> length(),
    binary_part(scanned, max(byte_size(scanned) - carry, 0), min(carry, byte_size(scanned)))
  }
end

discard = fn discard, conn, seen, held ->
  if seen >= backlog do
    seen
  else
    receive do
      message ->
        case Mint.HTTP.stream(conn, message) do
          {:ok, conn, responses} ->
            {seen, held} =
              Enum.reduce(responses, {seen, held}, fn
                {:data, ^ref, chunk}, {acc, held} ->
                  {found, held} = count.(chunk, held)
                  {acc + found, held}

                _other, acc ->
                  acc
              end)

            discard.(discard, conn, seen, held)

          # A message this connection did not own — keep waiting rather than
          # treating it as end of stream.
          :unknown ->
            discard.(discard, conn, seen, held)

          {:error, _conn, _reason, _responses} ->
            seen
        end
    after
      # Shorter than `heartbeat_interval`, so a stream that has gone quiet ends this
      # stage. A timeout longer than the heartbeat interval never fires: the
      # heartbeats themselves keep resetting it, and a miscount would hang here
      # forever rather than reporting.
      10_000 -> seen
    end
  end
end

seen = discard.(discard, conn, 0, "")
report.("(a) log -> discard", seen, System.monotonic_time(:microsecond) - started)

## (b) End to end with a no-op handler.
:persistent_term.put(:bench_drain_counter, :counters.new(1, [:write_concurrency]))
counter = :persistent_term.get(:bench_drain_counter)

Helper.start_supervisor!(Bench.Supervisor, url)

started = System.monotonic_time(:microsecond)

{:ok, _} = DMEx.Supervisor.subscribe(Bench.CountingHandler, supervisor: Bench.Supervisor)

drained = await.(counter, backlog, "(b)")
report.("(b) end to end, no-op handler", drained, System.monotonic_time(:microsecond) - started)

## (c) End to end where every row is skipped.

# Topped up to `skip_backlog` so this stage spans several `checkpoint_interval`s
# and the coalescing ratio below is a measurement rather than a zero. Stage (c)
# starts from the beginning of the log, so it drains the whole thing.
seed.(skip_backlog - backlog)

defmodule Bench.UnusedMessage do
  @moduledoc false
  use DMEx.Message, type: "bench.unused_message", fields: [:id]
end

defmodule Bench.SkippingHandler do
  @moduledoc false
  # Matches a type the log does not contain, so every row is a skip: a lookup,
  # a cursor write and a commit, with no handler.
  #
  # The catch-all clause — which names no type, so no fingerprint is built — is
  # load-bearing twice over. It is what makes "every row skipped" true at all: a
  # selective stream would carry none of these rows, so there would be nothing
  # to skip. And a selective fingerprint here could not match anything, since
  # `Bench.UnusedMessage` is defined at runtime in this script while the producer
  # expands filters over *compiled* application modules (`DMEx.Message.types/0`)
  # — the filter would match zero types, the connect would 404, and this stage
  # would retry forever.
  use DMEx.Handler, logs: [:bench], name: :skipping

  @impl true
  def handle_message(%Bench.UnusedMessage{}, _metadata), do: :ok
  def handle_message(_message, _metadata), do: :ok
end

skips = :counters.new(1, [:write_concurrency])
checkpoints = :counters.new(1, [:write_concurrency])

:telemetry.attach(
  "bench-skips",
  [:dmex, :handler, :skip],
  fn _e, _m, _meta, _c -> :counters.add(skips, 1, 1) end,
  nil
)

# The mechanism, counted rather than assumed: skips per checkpoint is how many
# cursor writes coalescing actually removed.
:telemetry.attach(
  "bench-checkpoints",
  [:dmex, :handler, :checkpoint],
  fn _e, _m, _meta, _c -> :counters.add(checkpoints, 1, 1) end,
  nil
)

started = System.monotonic_time(:microsecond)

{:ok, _} = DMEx.Supervisor.subscribe(Bench.SkippingHandler, supervisor: Bench.Supervisor)

skipped = await.(skips, skip_backlog, "(c)")

report.(
  "(c) end to end, every row skipped",
  skipped,
  System.monotonic_time(:microsecond) - started
)

written = :counters.get(checkpoints, 1)

# `max(written, 1)` would print a ratio for a run where nothing was written,
# which reads as the best possible coalescing when in fact the stage drained
# before the first checkpoint fell due. `skip_backlog` is sized so this is the
# measured branch; the other one says why it is not rather than inventing it.
detail =
  if written > 0 do
    # `Float.round/2` renders 40000.0 as "4.0e4", which is not a number anyone
    # reads off a benchmark.
    "#{written}  (#{:erlang.float_to_binary(skipped / written, decimals: 1)} rows per write)"
  else
    "0  (drained inside one checkpoint_interval, so none fell due; " <>
      "raise `skip_backlog` to measure the ratio)"
  end

IO.puts("  #{String.pad_trailing("    cursor writes for those skips", 34)} #{detail}")

:telemetry.detach("bench-skips")
:telemetry.detach("bench-checkpoints")

IO.puts("""

  (a) vs (b) is what the consumer pipeline costs on top of the log.
  (b) vs (c) was the per-row-commit question, and it is now answered rather than
  open: skips are coalesced into one cursor write per `checkpoint_interval`, so
  (c) should sit near (a) — a skipped row costs a map lookup and a telemetry
  event, and no database work at all. (b) is unchanged by design: a handled
  message still commits its own advance, because that transaction is the
  handler/cursor guarantee.
""")
