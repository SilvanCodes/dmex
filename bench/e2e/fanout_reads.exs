# The `C x P` read amplification, and where it actually binds.
#
# Every connection issues its own reads, so C connections at the tip with
# append rate P cost up to `C x P` reads per second. Where does that actually
# bind — query load, or pool checkout?
#
# Three things to read off it:
#
#   * **`max_batch_frequency` really caps each connection's query rate**,
#     however fast the log is written. Reads/s should track `C x frequency`,
#     not `C x P`, once P exceeds the window rate.
#   * **An idle log costs one query per poll interval per *app***, not per
#     connection. The dispatcher only broadcasts when `max(id)` moved, so the
#     idle column should be flat in C.
#   * **Where pool checkout queueing appears.** That is the connection ceiling,
#     and the number that would justify partitioning the pool so stream reads
#     cannot starve append writes.
#
# Last measured — developer machine, three runs, `pool_size: 10`:
#
#   * each connection wrote 53-66 times/s against the 100/s default, at C=10 and
#     C=50, however fast the log was appended to;
#   * an idle log cost 1 query/s flat at C=10, 50 and 200;
#   * pool checkout queueing appears between C=50 and C=200: at C=200 with a
#     100/s append rate ~50 000 of ~61 000 queries waited over 5 ms and
#     per-connection writes fell to ~40-48/s, at C=50 only ~120 of ~30 000, and
#     at C=10 none.
#
# So the frequency cap stops being the limit and the pool starts being it — and
# it scales with pool size rather than with anything DMEx chooses, which is why
# `max_batch_frequency` is the first setting to lower when many streams share
# one pool.

alias DMEx.BenchHelper, as: Helper
alias DMEx.IntegrationCase
alias DMEx.Test.IntegrationLog, as: Log
alias DMEx.Test.Messages.OrderPlaced
alias DMEx.Test.Server
alias DMEx.Test.SSEClient

Helper.section("fanout_reads — read amplification and pool contention")

Helper.start_repo!()
IntegrationCase.truncate_all!()

Helper.start_log!()

base = Server.start!(on_exit: false)
port = base |> URI.parse() |> Map.fetch!(:port)

# Counted from the log's own telemetry rather than inferred, so this is
# what the loop really did.
#
#   1 writes    2 queries    3 rows    4 slow pool checkouts
counter = :counters.new(4, [:write_concurrency])

# One write carries a whole read's rows, so writes are what `max_batch_frequency`
# caps and rows are what the log actually moved. Counting both keeps the two
# from being confused for each other.
:telemetry.attach(
  "bench-writes",
  [:dmex, :stream, :write],
  fn _event, measurements, _meta, _config ->
    :counters.add(counter, 1, 1)
    :counters.add(counter, 3, Map.get(measurements, :rows, 0))
  end,
  nil
)

# 5 ms in native units, which is what Ecto reports `queue_time` in.
slow_checkout = System.convert_time_unit(5, :millisecond, :native)

:telemetry.attach(
  "bench-queries",
  [:dm_ex, :test, :integration_repo, :query],
  fn _event, measurements, _meta, _config ->
    :counters.add(counter, 2, 1)
    # Checkout queue time is the pool-contention signal, and it needs a slot of
    # its own — folding it into the query counter would just inflate that.
    if Map.get(measurements, :queue_time, 0) > slow_checkout do
      :counters.add(counter, 4, 1)
    end
  end,
  nil
)

# Read from the log rather than restated, so the line cannot drift from the
# configuration it is describing.
cap = Log.__dmex__().config.max_batch_frequency

# A deliberately dumb SSE client: it holds a connection open for the whole
# round and never acts on what arrives. The point is the per-connection work
# the log does; each client is killed at the end of its round, so the window
# only has to outlast one.
connect = fn ->
  spawn(fn ->
    SSEClient.request("127.0.0.1", port, "/stream", [], until: :deadline, timeout: 60_000)
  end)
end

for connections <- [10, 50, 200], append_rate <- [0, 100] do
  Helper.section("C=#{connections} connections, P=#{append_rate}/s")

  clients = for _ <- 1..connections, do: connect.()
  # Let every connection finish its catch-up read and settle at the tip.
  Process.sleep(2_000)

  for slot <- 1..4, do: :counters.put(counter, slot, 0)

  appender =
    if append_rate > 0 do
      spawn(fn ->
        interval = max(div(1_000, append_rate), 1)

        fn ->
          {:ok, _} = Log.append(%OrderPlaced{order_id: 1})
          Process.sleep(interval)
        end
        |> Stream.repeatedly()
        |> Enum.take(append_rate * 5)
      end)
    end

  Process.sleep(5_000)

  writes = :counters.get(counter, 1)
  queries = :counters.get(counter, 2)
  rows = :counters.get(counter, 3)
  slow = :counters.get(counter, 4)

  IO.puts("  writes sent:      #{writes} (#{div(writes, 5)}/s)")
  IO.puts("  rows in them:     #{rows} (#{div(rows, 5)}/s)")
  IO.puts("  log queries:      #{queries} (#{div(queries, 5)}/s)")
  IO.puts("  slow checkouts:   #{slow} (queue_time over 5 ms)")

  IO.puts(
    "  per connection:   #{Float.round(writes / connections / 5, 2)} writes/s " <>
      "(max_batch_frequency caps this at #{Float.round(cap / 1, 2)}/s)"
  )

  if appender, do: Process.exit(appender, :kill)
  Enum.each(clients, &Process.exit(&1, :kill))
  Process.sleep(500)
end

:telemetry.detach("bench-writes")
:telemetry.detach("bench-queries")
