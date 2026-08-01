# Micro-benchmarks: the per-row and per-frame costs on DMEx's hot paths.
#
#     MIX_ENV=test mix bench.micro
#
# Each file states the question it answers.

Code.require_file("support/bench_helper.exs", __DIR__)

for file <- ~w(frame_encode frame_parse row_splice message_cast message_registry sse_decode) do
  Code.require_file("micro/#{file}.exs", __DIR__)
end
