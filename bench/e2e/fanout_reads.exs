# The `C x P` read amplification, and where it actually binds.
#
# Every connection issues its own reads, so C connections at the tip with
# publish rate P cost up to `C x P` reads per second. `ARCHITECTURE.md` says the
# binding constraint in practice is **database connection-pool contention**
# rather than raw query load, and asks for the realistic connection ceiling per
# producer. This is that measurement.
#
# Three things to read off it:
#
#   * **`batch_window` really caps each connection at one read per window**,
#     however fast the log is written. Reads/s should track `C / batch_window`,
#     not `C x P`, once P exceeds the window rate.
#   * **An idle log costs one query per poll interval per *app***, not per
#     connection. The dispatcher only broadcasts when `max(id)` moved, so the
#     idle column should be flat in C.
#   * **Where pool checkout queueing appears.** That is the connection ceiling,
#     and the number that would justify partitioning the pool so stream reads
#     cannot starve publish writes.

alias DMEx.BenchHelper, as: Helper
alias DMEx.Test.IntegrationProducer, as: Producer
alias DMEx.Test.Messages.OrderPlaced

Helper.section("fanout_reads — read amplification and pool contention")

Helper.start_repo!()
Helper.reset_log!()

Helper.start_producer!()

port = Helper.start_server!() |> URI.parse() |> Map.fetch!(:port)

# Counted from the producer's own telemetry rather than inferred, so this is
# what the loop really did.
counter = :counters.new(2, [:write_concurrency])

:telemetry.attach(
  "bench-frames",
  [:dmex, :stream, :frame],
  fn _event, _measure, _meta, _config -> :counters.add(counter, 1, 1) end,
  nil
)

:telemetry.attach(
  "bench-queries",
  [:dm_ex, :test, :integration_repo, :query],
  fn _event, measurements, _meta, _config ->
    :counters.add(counter, 2, 1)
    # Checkout queue time is the pool-contention signal.
    if Map.get(measurements, :queue_time, 0) > 5_000_000, do: :counters.add(counter, 2, 0)
  end,
  nil
)

# A deliberately dumb SSE client: it reads and discards. The point is to hold a
# connection open and let the producer do its per-connection work.
connect = fn ->
  spawn(fn ->
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, mode: :active)
    {:ok, conn, _ref} = Mint.HTTP.request(conn, "GET", "/stream", [], nil)

    drain = fn drain, conn ->
      receive do
        message ->
          case Mint.HTTP.stream(conn, message) do
            {:ok, conn, _responses} -> drain.(drain, conn)
            _other -> :ok
          end
      end
    end

    drain.(drain, conn)
  end)
end

for connections <- [10, 50, 200], publish_rate <- [0, 100] do
  Helper.section("C=#{connections} connections, P=#{publish_rate}/s")

  clients = for _ <- 1..connections, do: connect.()
  # Let every connection finish its catch-up read and settle at the tip.
  Process.sleep(2_000)

  :counters.put(counter, 1, 0)
  :counters.put(counter, 2, 0)

  publisher =
    if publish_rate > 0 do
      spawn(fn ->
        interval = max(div(1_000, publish_rate), 1)

        Stream.repeatedly(fn ->
          {:ok, _} = Producer.publish(%OrderPlaced{order_id: 1})
          Process.sleep(interval)
        end)
        |> Enum.take(publish_rate * 5)
      end)
    end

  Process.sleep(5_000)

  frames = :counters.get(counter, 1)
  queries = :counters.get(counter, 2)

  IO.puts("  frames sent:      #{frames} (#{div(frames, 5)}/s)")
  IO.puts("  producer queries: #{queries} (#{div(queries, 5)}/s)")

  IO.puts(
    "  per connection:   #{Float.round(frames / connections / 5, 2)} frames/s " <>
      "(batch_window caps this at #{Float.round(1000 / 200, 1)}/s)"
  )

  if publisher, do: Process.exit(publisher, :kill)
  Enum.each(clients, &Process.exit(&1, :kill))
  Process.sleep(500)
end

:telemetry.detach("bench-frames")
:telemetry.detach("bench-queries")
