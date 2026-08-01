defmodule DMEx.MessageTest do
  use DMEx.Case, async: true

  alias DMEx.Test.Handlers
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Messages.OrderShipped
  alias DMEx.Test.Messages.WithCustomCast

  describe "type/0" do
    test "derives the full, fully-qualified module name" do
      assert OrderPlaced.type() == "DMEx.Test.Messages.OrderPlaced"
      assert OrderShipped.type() == "DMEx.Test.Messages.OrderShipped"
    end

    test "is a compile-time constant, not an inspect/1 call per invocation" do
      # A literal-returning function compiles to a single `return` of a constant;
      # if `type/0` still called inspect/1 it would allocate a new binary each
      # time and the two results would not be the same term.
      assert :erts_debug.same(OrderPlaced.type(), OrderPlaced.type())
    end

    test "distinguishes same-named modules in different namespaces" do
      refute OrderPlaced.type() == "OrderPlaced"
      assert String.contains?(OrderPlaced.type(), "DMEx.Test.Messages.")
    end
  end

  describe "cast/1 default" do
    test "maps string keys onto declared fields" do
      assert OrderPlaced.cast(%{"order_id" => 7, "customer_id" => 3, "total" => 1999}) ==
               %OrderPlaced{order_id: 7, customer_id: 3, total: 1999}
    end

    test "leaves the struct's own default in place for absent keys" do
      assert OrderShipped.cast(%{"order_id" => 7}) ==
               %OrderShipped{order_id: 7, carrier: "unknown"}
    end

    test "ignores keys the struct does not declare" do
      assert OrderPlaced.cast(%{"order_id" => 7, "not_a_field" => "x", "__struct__" => "evil"}) ==
               %OrderPlaced{order_id: 7}
    end

    test "an empty map yields the struct defaults" do
      assert OrderPlaced.cast(%{}) == %OrderPlaced{}
      assert OrderShipped.cast(%{}) == %OrderShipped{carrier: "unknown"}
    end

    test "passes values through untouched, whatever their JSON shape" do
      nested = %{"a" => [1, %{"b" => nil}]}

      assert %OrderPlaced{total: ^nested} = OrderPlaced.cast(%{"total" => nested})
    end

    test "an explicit null overrides a field default" do
      assert OrderShipped.cast(%{"order_id" => 7, "carrier" => nil}) ==
               %OrderShipped{order_id: 7, carrier: nil}
    end

    test "never creates atoms, whatever keys arrive" do
      # Keys that are plausible field names but are not, plus keys that could
      # only ever exist as atoms if something called String.to_atom/1 on them.
      keys =
        Enum.map(1..500, fn i ->
          "dmex_definitely_not_an_existing_atom_#{i}_#{System.unique_integer([:positive])}"
        end)

      assert OrderPlaced.cast(Map.new(keys, &{&1, 1})) == %OrderPlaced{}

      # Asserted per key rather than against :erlang.system_info(:atom_count):
      # that counter is global, so any other async test defining a module moves
      # it and this becomes flaky while testing nothing more.
      for key <- keys do
        assert_raise ArgumentError, fn -> String.to_existing_atom(key) end
      end
    end

    test "rejects non-map input rather than silently producing an empty struct" do
      # apply/3 here is deliberate, not laziness: a direct call would be flagged
      # by the compiler's type checker for the very argument this test exists to
      # pass, and the guard is what we are asserting on.
      # credo:disable-for-next-line Credo.Check.Refactor.Apply
      assert_raise FunctionClauseError, fn -> apply(OrderPlaced, :cast, ["not a map"]) end
      # credo:disable-for-next-line Credo.Check.Refactor.Apply
      assert_raise FunctionClauseError, fn -> apply(OrderPlaced, :cast, [nil]) end
    end
  end

  describe "cast/1 override" do
    test "a user-defined cast/1 replaces the generated default" do
      assert %WithCustomCast{order_id: 7, placed_at: %DateTime{} = at} =
               WithCustomCast.cast(%{"order_id" => 7, "placed_at" => "2026-07-27T10:31:04Z"})

      assert DateTime.to_iso8601(at) == "2026-07-27T10:31:04Z"
    end

    test "the override still satisfies the behaviour" do
      assert function_exported?(WithCustomCast, :cast, 1)
      assert WithCustomCast.type() == "DMEx.Test.Messages.WithCustomCast"
    end
  end

  describe "round-trip with JSON" do
    test "encode then decode then cast returns the original struct" do
      original = %OrderPlaced{order_id: 7, customer_id: 3, total: 1999}

      assert original ==
               original
               |> JSON.encode!()
               |> JSON.decode!()
               |> OrderPlaced.cast()
    end
  end

  describe "registry/1" do
    test "maps each module's type to the module" do
      assert DMEx.Message.registry(MapSet.new([OrderPlaced, OrderShipped])) == %{
               "DMEx.Test.Messages.OrderPlaced" => OrderPlaced,
               "DMEx.Test.Messages.OrderShipped" => OrderShipped
             }
    end

    test "an empty set yields an empty registry" do
      assert DMEx.Message.registry(MapSet.new()) == %{}
    end

    test "takes the set a handler's declaration already carries" do
      # Not a list: the same module twice is unrepresentable in the input rather
      # than tolerated by the reduce, which is why the collision check below can
      # be unconditional.
      assert DMEx.Message.registry(Handlers.EchoAll.__dmex__().messages) == %{
               "DMEx.Test.Messages.OrderPlaced" => OrderPlaced,
               "DMEx.Test.Messages.OrderShipped" => OrderShipped
             }
    end

    test "raises when two modules claim the same type" do
      # `use DMEx.Message` cannot produce this collision — `type/0` is derived
      # from the module name and is deliberately not overridable. It takes a
      # module implementing the behaviour by hand, which is exactly the case the
      # check exists to catch.
      defmodule Impostor do
        @moduledoc false
        @behaviour DMEx.Message

        defstruct []

        @impl true
        def type, do: "DMEx.Test.Messages.OrderPlaced"

        @impl true
        def cast(data), do: DMEx.Message.cast_default(__MODULE__, data)
      end

      assert_raise ArgumentError, ~r/claimed by both/, fn ->
        DMEx.Message.registry(MapSet.new([OrderPlaced, Impostor]))
      end
    end
  end

  describe "use DMEx.Message fields" do
    test "generates the struct from a list of atoms" do
      assert Map.from_struct(%OrderPlaced{}) == %{order_id: nil, customer_id: nil, total: nil}
    end

    test "generates the struct from a mixed list, keeping the declared defaults" do
      assert Map.from_struct(%OrderShipped{}) == %{order_id: nil, carrier: "unknown"}
    end

    test "generates the JSON.Encoder derivation, so the struct is appendable as-is" do
      encoded = JSON.encode!(%OrderPlaced{order_id: 7, customer_id: 3, total: 1999})

      assert JSON.decode!(encoded) == %{"order_id" => 7, "customer_id" => 3, "total" => 1999}
    end

    test "generates a t/0 type over the declared fields" do
      spec = type_spec(OrderShipped)

      assert spec =~ "OrderShipped"
      assert spec =~ "order_id: term()"
      assert spec =~ "carrier: term()"
    end

    test "no fields yields a struct with no fields" do
      defmodule Marker do
        @moduledoc false
        use DMEx.Message
      end

      # `struct/1` rather than `%Marker{}`: the module is defined in this very
      # compilation context, so the literal cannot be expanded here.
      marker = struct(Marker)

      assert Map.from_struct(marker) == %{}
      assert Marker.cast(%{"anything" => 1}) == marker

      # No JSON assertion: protocols are consolidated by the time a test defines
      # a module, so the generated `@derive` cannot install an implementation
      # here — Elixir says as much in a warning. The derivation is covered by
      # the compiled message modules above.
    end

    test "rejects anything defstruct would not take" do
      assert_raise ArgumentError, ~r/in the shape defstruct takes them/, fn ->
        defmodule BadFields do
          @moduledoc false
          use DMEx.Message, "order_id"
        end
      end

      assert_raise ArgumentError, ~r/in the shape defstruct takes them/, fn ->
        defmodule BadField do
          @moduledoc false
          use DMEx.Message, [:order_id, "carrier"]
        end
      end
    end
  end

  defp type_spec(module) do
    {:ok, types} = Code.Typespec.fetch_types(module)
    {:type, type} = Enum.find(types, &match?({:type, {:t, _, []}}, &1))

    type |> Code.Typespec.type_to_quoted() |> Macro.to_string()
  end
end
