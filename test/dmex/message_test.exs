defmodule DMEx.MessageTest do
  use DMEx.Case, async: true

  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Messages.OrderShipped
  alias DMEx.Test.Messages.WithCustomCast
  alias DMEx.Test.Messages.WithCustomEncoder

  describe "type/0" do
    test "returns the declared type" do
      assert OrderPlaced.type() == "order_placed"
      assert OrderShipped.type() == "order_shipped"
    end

    test "is a compile-time constant, not a call per invocation" do
      # A literal-returning function compiles to a single `return` of a constant;
      # if `type/0` computed the string it would allocate a new binary each time
      # and the two results would not be the same term.
      assert :erts_debug.same(OrderPlaced.type(), OrderPlaced.type())
    end

    test "is independent of the module name" do
      # The whole point: these fixtures declare types that share nothing with
      # what they are called, so a regression that derived the type from the
      # module — the old behaviour — fails here rather than passing quietly.
      refute OrderPlaced.type() == inspect(OrderPlaced)
      refute String.contains?(OrderPlaced.type(), "DMEx")
    end

    test "renders an atom and a string identically" do
      # Otherwise a style change at the declaration site mints a second wire
      # type and orphans everything already appended under the first.
      assert compile_type(~s|type: :order_placed|) == compile_type(~s|type: "order_placed"|)
    end

    test "accepts dots, for namespacing" do
      assert compile_type(~s|type: "acme.orders.placed.v1"|) == "acme.orders.placed.v1"
    end

    test "rejects a dot that separates nothing" do
      # A dot is a namespace separator, not a legal character: `"."` and
      # `"foo."` name a segment that is not there.
      for bad <- [~s|"."|, ~s|"..."|, ~s|".foo"|, ~s|"foo."|, ~s|"a..b"|] do
        assert_raise ArgumentError, ~r/must be an atom or a string matching/, fn ->
          compile_message("type: #{bad}")
        end
      end
    end
  end

  describe "use DMEx.Message type" do
    test "requires :type" do
      assert_raise ArgumentError, ~r/requires :type/, fn ->
        compile_message("fields: [:order_id]")
      end
    end

    test "rejects a type outside the SSE-safe charset" do
      for bad <- [~s|"order-placed"|, ~s|"acme/orders:placed"|, ~s|"order placed"|, ~s|""|] do
        assert_raise ArgumentError, ~r/must be an atom or a string matching/, fn ->
          compile_message("type: #{bad}")
        end
      end
    end

    test "rejects a type that is neither an atom nor a string" do
      assert_raise ArgumentError, ~r/must be an atom or a string matching/, fn ->
        compile_message("type: 7")
      end
    end

    test "rejects unknown options" do
      assert_raise ArgumentError, ~r/unknown option \[:selective\]/, fn ->
        compile_message(~s|type: "ok", selective: true|)
      end
    end

    test "rejects a non-keyword argument" do
      assert_raise ArgumentError, ~r/takes a keyword list/, fn ->
        compile_message("[:order_id]")
      end
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
      assert WithCustomCast.type() == "with_custom_cast"
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

    test "a hand-written cast/1 inverts the encoding too" do
      # The default cast/1 and the derivation are generated from one field list
      # and cannot disagree. A hand-written one can, which is the only place a
      # round-trip test says anything.
      original = %WithCustomCast{order_id: 7, placed_at: ~U[2026-07-27 10:31:04Z]}

      assert original ==
               original
               |> JSON.encode!()
               |> JSON.decode!()
               |> WithCustomCast.cast()
    end
  end

  describe "encoding" do
    test "a field must hold something JSON.Encoder already encodes" do
      # The invariant the moduledoc states. `URI` stands in for any struct that
      # derives nothing: the derivation encodes fields with `JSON.Encoder`, and
      # there is no implementation to reach for.
      refute JSON.Encoder.impl_for(%URI{})

      assert_raise Protocol.UndefinedError, fn ->
        JSON.encode!(%OrderPlaced{order_id: 7, total: %URI{path: "/x"}})
      end
    end

    test "a struct JSON.Encoder does implement encodes as that implementation renders it" do
      assert JSON.encode!(%OrderPlaced{order_id: 7, total: ~U[2026-07-27 10:31:04Z]}) =~
               ~s("total":"2026-07-27T10:31:04Z")
    end

    test "an explicit defimpl replaces the generated derivation" do
      # The write-side escape hatch: a second `@derive` cannot work — it would
      # land after the generated `defstruct` — but an implementation does.
      assert JSON.decode!(JSON.encode!(%WithCustomEncoder{order_id: 7, secret: "shh"})) ==
               %{"order_id" => 7}
    end
  end

  describe "index!/1" do
    test "keys the modules by their declared type" do
      assert DMEx.Message.index!([OrderPlaced, OrderShipped]) ==
               %{"order_placed" => OrderPlaced, "order_shipped" => OrderShipped}
    end

    test "raises on a collision rather than letting the second module win" do
      # The failure this exists to prevent: a plain `Map.new/2` would return a
      # map of size one here, leaving the losing module permanently unreachable
      # and every row of its type silently skipped.
      twin = compile_message(~s|type: "order_placed"|)

      assert_raise ArgumentError, ~r/declared by both.*give one of them a different/s, fn ->
        DMEx.Message.index!([OrderPlaced, twin])
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

    test "generates no t/0, so a module can declare the informative one itself" do
      refute type_spec(OrderShipped)

      # A generated `t/0` could only say `term()` per field, and would make this
      # a redefinition — a compile error rather than a weaker type.
      module =
        compile_message(~s|type: "with_own_t", fields: [:order_id]|, """
        @type t :: %__MODULE__{order_id: pos_integer()}
        """)

      assert module.type() == "with_own_t"
    end

    test "omitting fields: yields a struct with no fields" do
      marker = compile_message(~s|type: "marker"|)

      # `struct/1` rather than `%Marker{}`: the module is defined at runtime, so
      # the literal cannot be expanded here.
      empty = struct(marker)

      assert Map.from_struct(empty) == %{}
      assert marker.cast(%{"anything" => 1}) == empty

      # No JSON assertion: protocols are consolidated by the time a test defines
      # a module, so the generated `@derive` cannot install an implementation
      # here — Elixir says as much in a warning. The derivation is covered by
      # the compiled message modules above.
    end

    test "rejects anything defstruct would not take" do
      assert_raise ArgumentError, ~r/in the shape defstruct takes them/, fn ->
        compile_message(~s|type: "bad", fields: "order_id"|)
      end

      assert_raise ArgumentError, ~r/in the shape defstruct takes them/, fn ->
        compile_message(~s|type: "bad", fields: [:order_id, "carrier"]|)
      end
    end
  end

  defp compile_type(declaration), do: declaration |> compile_message() |> then(& &1.type())

  # Compiled into a uniquely-named module each time, so nothing here depends on
  # a module name — which is the point of the design under test.
  defp compile_message(declaration, body \\ "") do
    name = "DMEx.MessageTest.Generated#{System.unique_integer([:positive])}"

    {{:module, module, _, _}, _} =
      Code.eval_string(
        """
        defmodule #{name} do
          use DMEx.Message, #{declaration}
          #{body}
        end
        """,
        [],
        file: __ENV__.file
      )

    module
  end

  defp type_spec(module) do
    {:ok, types} = Code.Typespec.fetch_types(module)

    case Enum.find(types, &match?({:type, {:t, _, []}}, &1)) do
      {:type, type} -> type |> Code.Typespec.type_to_quoted() |> Macro.to_string()
      nil -> nil
    end
  end
end
