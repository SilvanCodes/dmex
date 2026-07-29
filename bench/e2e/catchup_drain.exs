# How fast can a log actually be replayed, and what is the bottleneck?
#
# "Arbitrarily replayable streams" is one of the three things DMEx claims to
# buy, and it is only meaningful with a number attached. This measures three
# stages against the same seeded log, so the answer separates rather than
# conflates them:
#
#   (a) **producer-side**: a client that reads and discards. Read plus encode
#       plus the wire, with no consumer cost at all.
#   (b) **end to end, no-op handler**: adds decode, cast, and the consumer's
#       per-row transaction.
#   (c) **end to end, every row skipped**: the consumer has no module for the
#       type, so no handler runs — but the row still costs a lookup, a
#       `last_seen` write and a commit.
#
# **(c) versus (b) is the measurement behind an open question in
# `ARCHITECTURE.md`**: on a stream where a consumer has modules for a small
# fraction of the types, the drain is dominated by commits for rows nothing was
# done with. If (c) is close to (b), skips are commit-bound, and coalescing
# consecutive skips into one cursor advance is worth its complexity. If (c) is
# far faster, it is not.

alias DMEx.BenchHelper, as: Helper
alias DMEx.Consumer
alias DMEx.Test.IntegrationProducer, as: Producer
alias DMEx.Test.Messages.OrderPlaced
alias Ecto.Adapters.SQL

Helper.section("catchup_drain — replay rate, and which stage is the bottleneck")

# 1_000_000 in the design's own framing. 20_000 keeps a run to a few minutes and
# still shows the shape; raise it when the question is "how long would replaying
# our actual log take".
backlog = 20_000

defmodule Bench.CountingHandler do
  @moduledoc false
  use DMEx.Consumer.Handler, producer: :bench, id: :counting

  @impl true
  def handle_message(%DMEx.Test.Messages.OrderPlaced{}, _metadata) do
    :counters.add(:persistent_term.get(:bench_drain_counter), 1, 1)

    :ok
  end
end

Helper.start_repo!()
Helper.reset_log!()

Helper.start_producer!()

# Seeded in one statement: appends are deliberately serialized, and this
# benchmark is about the read path.
IO.puts("seeding #{backlog} rows...")

SQL.query!(
  DMEx.Test.IntegrationRepo,
  """
  INSERT INTO dmex_messages (message_type, data, metadata)
  SELECT $1, json_build_object('order_id', n, 'note', repeat('x', 200))::text::jsonb, '{}'::jsonb
    FROM generate_series(1, $2) AS n
  """,
  [OrderPlaced.type(), backlog]
)

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

## (a) Producer side only: read, encode, wire.

started = System.monotonic_time(:microsecond)
{:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, mode: :active)
{:ok, conn, ref} = Mint.HTTP.request(conn, "GET", "/stream", [], nil)

discard = fn discard, conn, seen ->
  if seen >= backlog do
    seen
  else
    receive do
      message ->
        case Mint.HTTP.stream(conn, message) do
          {:ok, conn, responses} ->
            seen =
              Enum.reduce(responses, seen, fn
                {:data, ^ref, chunk}, acc ->
                  acc + (chunk |> :binary.matches(~s("id":)) |> length())

                _other, acc ->
                  acc
              end)

            discard.(discard, conn, seen)

          # A message this connection did not own — keep waiting rather than
          # treating it as end of stream.
          :unknown ->
            discard.(discard, conn, seen)

          {:error, _conn, _reason, _responses} ->
            seen
        end
    after
      30_000 -> seen
    end
  end
end

seen = discard.(discard, conn, 0)
report.("(a) producer -> discard", seen, System.monotonic_time(:microsecond) - started)

## (b) End to end with a no-op handler.

:persistent_term.put(:bench_drain_counter, :counters.new(1, [:write_concurrency]))
counter = :persistent_term.get(:bench_drain_counter)

Helper.start_consumer!(Bench.Consumer, url)

started = System.monotonic_time(:microsecond)

{:ok, _} = Consumer.subscribe(Bench.CountingHandler, consumer: Bench.Consumer)

await = fn await, target ->
  if :counters.get(counter, 1) >= target do
    :ok
  else
    Process.sleep(50)
    await.(await, target)
  end
end

await.(await, backlog)
report.("(b) end to end, no-op handler", backlog, System.monotonic_time(:microsecond) - started)

## (c) End to end where every row is skipped.

defmodule Bench.UnusedMessage do
  @moduledoc false
  @derive JSON.Encoder
  use DMEx.Message
  defstruct [:id]
end

defmodule Bench.SkippingHandler do
  @moduledoc false
  # Matches a type the log does not contain, so every row is a skip: a lookup,
  # a cursor write and a commit, with no handler.
  use DMEx.Consumer.Handler, producer: :bench, id: :skipping

  @impl true
  def handle_message(%Bench.UnusedMessage{}, _metadata), do: :ok
end

skips = :counters.new(1, [:write_concurrency])
checkpoints = :counters.new(1, [:write_concurrency])

:telemetry.attach(
  "bench-skips",
  [:dmex, :consumer, :skip],
  fn _e, _m, _meta, _c -> :counters.add(skips, 1, 1) end,
  nil
)

# The mechanism, counted rather than assumed: skips per checkpoint is how many
# cursor writes coalescing actually removed.
:telemetry.attach(
  "bench-checkpoints",
  [:dmex, :consumer, :checkpoint],
  fn _e, _m, _meta, _c -> :counters.add(checkpoints, 1, 1) end,
  nil
)

started = System.monotonic_time(:microsecond)

{:ok, _} = Consumer.subscribe(Bench.SkippingHandler, consumer: Bench.Consumer)

await_skips = fn await_skips, target ->
  if :counters.get(skips, 1) >= target do
    :ok
  else
    Process.sleep(50)
    await_skips.(await_skips, target)
  end
end

await_skips.(await_skips, backlog)

report.(
  "(c) end to end, every row skipped",
  backlog,
  System.monotonic_time(:microsecond) - started
)

written = :counters.get(checkpoints, 1)

IO.puts(
  "  #{String.pad_trailing("    cursor writes for those skips", 34)} " <>
    "#{written}  (#{Float.round(backlog / max(written, 1), 1)} rows per write)"
)

:telemetry.detach("bench-skips")
:telemetry.detach("bench-checkpoints")

IO.puts("""

  (a) vs (b) is what the consumer pipeline costs on top of the producer.
  (b) vs (c) was the per-row-commit question, and it is now answered rather than
  open: skips are coalesced into one cursor write per `checkpoint_interval`, so
  (c) should sit near (a) — a skipped row costs a map lookup and a telemetry
  event, and no database work at all. (b) is unchanged by design: a handled
  message still commits its own advance, because that transaction is the
  handler/cursor guarantee.
""")
