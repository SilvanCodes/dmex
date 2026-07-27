# Micro-benchmarks: the per-row and per-frame costs on DMEx's hot paths.
#
#     MIX_ENV=test mix bench.micro
#
# Each file states the design claim it is testing. None of these run in CI —
# benchmarks are for decisions, not regressions.

Code.require_file("support/bench_helper.exs", __DIR__)

for file <- ~w(frame_encode frame_parse row_splice message_cast message_registry sse_decode) do
  Code.require_file("micro/#{file}.exs", __DIR__)
end
