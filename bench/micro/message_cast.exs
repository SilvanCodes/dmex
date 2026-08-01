# Should `use DMEx.Message` inject precomputed field-name strings?
#
# `DMEx.Message.cast_default/2` walks the struct's fields and calls
# `Atom.to_string/1` on each one to look it up in the incoming map. That is one
# conversion per field per row, on the consumer's per-row path — multiplied by
# every message the application will ever consume.
#
# The question is whether `use DMEx.Message` should inject a precomputed
# `[{field, "field"}]` list instead. This measures the current implementation
# against that variant and against a hand-written `cast/1`, which is the floor.

alias DMEx.BenchHelper, as: Helper
alias DMEx.Message

Helper.section("message_cast — per-row casting cost")

defmodule Bench.Cast do
  @moduledoc false

  # The precomputed-strings variant `use DMEx.Message` might inject.
  def cast_precomputed(module, fields, data) do
    Enum.reduce(fields, module.__struct__(), fn {field, name}, acc ->
      case Map.fetch(data, name) do
        {:ok, value} -> Map.put(acc, field, value)
        :error -> acc
      end
    end)
  end
end

for size <- [3, 10, 30] do
  fields = Enum.map(1..size, &:"field_#{&1}")

  Code.eval_string("""
  defmodule Bench.Struct#{size} do
    use DMEx.Message, #{inspect(fields)}
  end
  """)
end

inputs =
  for size <- [3, 10, 30], into: %{} do
    module = Module.concat(Bench, :"Struct#{size}")
    fields = Enum.map(1..size, &:"field_#{&1}")
    data = Map.new(fields, &{Atom.to_string(&1), 42})
    pairs = Enum.map(fields, &{&1, Atom.to_string(&1)})

    {"#{size} fields", {module, pairs, data}}
  end

Benchee.run(
  %{
    "cast_default (as shipped)" => fn {module, _pairs, data} ->
      Message.cast_default(module, data)
    end,
    "precomputed field strings" => fn {module, pairs, data} ->
      Bench.Cast.cast_precomputed(module, pairs, data)
    end
  },
  Helper.opts(inputs: inputs)
)
