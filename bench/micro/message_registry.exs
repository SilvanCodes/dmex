# Confirms an assumption the pipeline rests on.
#
# Every row a consumer receives gets its `message_type` resolved against the
# subscription's module registry — including rows that will be skipped, because
# a filter is heuristic in one direction and `selective: false` asks for the
# whole log outright, so skipping is a pipeline step rather than a filter at the
# door.
#
# That is only defensible if the lookup is negligible. This measures it, plus
# the one-off cost of building the registry per subscription.

alias DMEx.BenchHelper, as: Helper
alias DMEx.Message

Helper.section("message_registry — per-row type resolution")

for n <- 1..1_000 do
  Code.eval_string("""
  defmodule Bench.Messages.M#{n} do
    use DMEx.Message
    defstruct [:id]
  end
  """)
end

modules = fn count -> Enum.map(1..count, &Module.concat(Bench.Messages, :"M#{&1}")) end

inputs =
  for count <- [10, 100, 1_000], into: %{} do
    list = modules.(count)
    registry = Message.registry(list)
    # Look up the last one, so a linear implementation would show.
    type = List.last(list).type()

    {"#{count} modules", {list, registry, type}}
  end

Benchee.run(
  %{
    "lookup (per row)" => fn {_list, registry, type} -> Map.fetch!(registry, type) end,
    "registry build (per subscription)" => fn {list, _registry, _type} ->
      Message.registry(list)
    end
  },
  Helper.opts(inputs: inputs)
)
