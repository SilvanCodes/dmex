# End-to-end benchmarks: the questions ARCHITECTURE.md leaves open with no
# numbers attached.
#
#     MIX_ENV=test mix bench.e2e
#
# These need a reachable Postgres (the integration database) and they write to
# it. Each file states which open question it answers. None run in CI.

Code.require_file("support/bench_helper.exs", __DIR__)

for file <- ~w(publish_throughput end_to_end_latency fanout_reads catchup_drain) do
  Code.require_file("e2e/#{file}.exs", __DIR__)
end
