# What does the append lock actually cost?
#
# Per-app advisory-locked appends make the reader cursor sound but serialize
# every publish in the application. "What publish throughput does that actually
# cap out at" was the open question, and this is the measurement behind the
# figures `ARCHITECTURE.md` now quotes.
#
# Read it two ways:
#
#   * Locked throughput should be roughly flat across concurrency. If it rises,
#     the lock is not doing what it claims; if it collapses, contention is
#     costing more than serialization alone.
#   * The locked/unlocked ratio at high concurrency is the price being paid for
#     a gap-free cursor. That is the number to weigh against the alternatives
#     the design rejected — snapshot watermarks, read-behind lag.
#
# Secondary signal: if locked and unlocked are close even at concurrency 32, the
# bottleneck is fsync rather than the lock, and the advice to users is "batch
# your publishes", not "the lock is the problem".

alias DMEx.BenchHelper, as: Helper
alias DMEx.Test.IntegrationProducer, as: Producer
alias DMEx.Test.Messages.OrderPlaced
alias Ecto.Adapters.SQL

Helper.section("publish_throughput — the cost of serialized appends")

Helper.start_repo!()
Helper.start_producer!()

message = %OrderPlaced{order_id: 1, customer_id: 2, total: 1999}
data = JSON.encode!(message)

# The same insert without the advisory lock, as the control. Deliberately not
# routed through `publish/2`, because the point is to isolate the lock.
unlocked = fn ->
  SQL.query!(
    DMEx.Test.IntegrationRepo,
    """
    INSERT INTO dmex_messages (message_type, data, metadata)
         VALUES ($1, $2::text::jsonb, '{}'::jsonb)
    """,
    [OrderPlaced.type(), data]
  )
end

for concurrency <- [1, 2, 8, 32] do
  Helper.section("concurrency #{concurrency}")
  Helper.reset_log!()

  Benchee.run(
    %{
      "publish (advisory-locked)" => fn -> {:ok, _} = Producer.publish(message) end,
      "raw insert (no lock)" => fn -> unlocked.() end
    },
    Helper.opts(parallel: concurrency, memory_time: 0)
  )
end
