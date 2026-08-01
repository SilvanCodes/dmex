# Confirms an assumption the pipeline rests on.
#
# Every row a consumer receives gets its `message_type` resolved against the
# subscription's module registry, including rows that will be skipped. That is
# only defensible if the lookup is negligible. This measures it, plus the
# one-off cost of building the registry per subscription.

alias DMEx.BenchHelper, as: Helper
alias DMEx.Message

Helper.section("message_registry — per-row type resolution")

for n <- 1..1_000 do
  Code.eval_string("""
  defmodule Bench.Messages.M#{n} do
    use DMEx.Message, [:id]
  end
  """)
end

modules = fn count -> MapSet.new(1..count, &Module.concat(Bench.Messages, :"M#{&1}")) end

inputs =
  for count <- [10, 100, 1_000], into: %{} do
    set = modules.(count)
    registry = Message.registry(set)
    # Look up the last one, so a linear implementation would show.
    type = set |> Enum.max() |> then(& &1.type())

    {"#{count} modules", {set, registry, type}}
  end

Benchee.run(
  %{
    "lookup (per row)" => fn {_set, registry, type} -> Map.fetch!(registry, type) end,
    "registry build (per subscription)" => fn {set, _registry, _type} ->
      Message.registry(set)
    end
  },
  Helper.opts(inputs: inputs)
)
