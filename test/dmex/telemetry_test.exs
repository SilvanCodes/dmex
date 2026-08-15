defmodule DMEx.TelemetryTest do
  use DMEx.Case, async: true

  alias DMEx.Telemetry

  @span_suffixes [:start, :stop, :exception]

  describe "documentation" do
    # DMEx.Telemetry has no code, so the catalogue is the only statement of the
    # event vocabulary and an event renamed in lib breaks every dashboard built
    # on it without failing anything.
    #
    # Both sides are normalised to the concrete names a handler attaches to: a
    # span is expanded into its `:start` / `:stop` / `:exception` triad, from
    # `:telemetry.span/3`'s prefix on the lib side and from the `[…, :start]`
    # of the catalogue row that writes the other two as bare `` `:stop` ``.
    test "the catalogue names exactly the events lib emits" do
      documented = documented_events()
      emitted = emitted_events()

      assert documented == emitted, """
      DMEx.Telemetry has drifted from the events lib emits.

      documented but never emitted: #{inspect(Enum.sort(MapSet.difference(documented, emitted)))}
      emitted but undocumented: #{inspect(Enum.sort(MapSet.difference(emitted, documented)))}
      """
    end
  end

  # Every `[:dmex, …]` in the @moduledoc's Markdown tables, read out of the docs
  # chunk so that what a reader is shown is what is checked.
  defp documented_events do
    {:docs_v1, _, _, _, %{"en" => moduledoc}, _, _} = Code.fetch_docs(Telemetry)

    moduledoc
    |> String.split("\n")
    |> Enum.filter(&String.starts_with?(&1, "|"))
    |> Enum.flat_map(&Regex.scan(~r/`(\[:dmex[^\]]*\])`/, &1, capture: :all_but_first))
    |> Enum.flat_map(fn [event] -> documented_event(eval!(event)) end)
    |> MapSet.new()
  end

  defp documented_event(event) do
    case List.last(event) do
      :start -> span_events(Enum.drop(event, -1))
      _leaf -> [event]
    end
  end

  # Read from the source tree rather than the BEAM: the event list is an
  # argument, not something the compiled module remembers. Every call site
  # passes it as a literal, and one that computed it would escape this scan.
  defp emitted_events do
    root = File.cwd!()
    catalogue = Path.join(root, "lib/dmex/telemetry.ex")

    root
    |> Path.join("lib/**/*.ex")
    |> Path.wildcard()
    # The catalogue quotes `:telemetry` at length and may not satisfy itself.
    |> Enum.reject(&(&1 == catalogue))
    |> Enum.flat_map(fn path ->
      ~r/:telemetry\.(execute|span)\(\s*(\[[^\]]*\])/
      |> Regex.scan(File.read!(path), capture: :all_but_first)
      |> Enum.flat_map(fn
        ["span", prefix] -> span_events(eval!(prefix))
        ["execute", event] -> [eval!(event)]
      end)
    end)
    |> MapSet.new()
  end

  defp span_events(prefix), do: Enum.map(@span_suffixes, &(prefix ++ [&1]))

  defp eval!(source) do
    {term, _binding} = Code.eval_string(source)
    term
  end
end
