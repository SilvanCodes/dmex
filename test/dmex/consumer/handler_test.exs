defmodule DMEx.Consumer.HandlerTest do
  use DMEx.Case, async: true

  alias DMEx.Consumer.Handler
  alias DMEx.Test.Handlers
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Messages.OrderShipped

  describe "the declaration" do
    test "attaches the behaviour" do
      assert Handler in Handlers.Echo.module_info(:attributes)[:behaviour]
    end

    test "is recognisable as a handler" do
      assert Handler.handler?(Handlers.Echo)
    end

    test "a module without `use` is not a handler, even with the right function" do
      # NotAHandler exports handle_message/2 and is still rejected: the point is
      # to fail at subscribe/2 with a clear message rather than at the first row.
      assert Code.ensure_loaded?(Handlers.NotAHandler)
      assert function_exported?(Handlers.NotAHandler, :handle_message, 2)
      refute Handler.handler?(Handlers.NotAHandler)
    end

    test "a module that does not exist is not a handler" do
      refute Handler.handler?(NoSuchModuleAnywhere)
    end

    test "carries the producer, the id, the module and the message list" do
      assert Handlers.EchoAll.__dmex__() == %{
               handler: Handlers.EchoAll,
               producer: "test",
               id: "test/echo_all",
               messages: [OrderPlaced, OrderShipped]
             }
    end

    test "no default handle_message/2 is injected" do
      # A silently-succeeding default would let a handler that forgot to
      # implement anything advance its cursor over every message it was given.
      {result, _} =
        Code.eval_string("""
        defmodule DMEx.Consumer.HandlerTest.Empty do
          use DMEx.Consumer.Handler,
            producer: :orders,
            id: :empty,
            messages: [DMEx.Test.Messages.OrderPlaced]
        end
        """)

      {:module, module, _, _} = result

      refute function_exported?(module, :handle_message, 2)
    end
  end

  describe "the subscription id" do
    # **Frozen.** This string is a text primary key in every deployment's
    # database. Changing the separator, the charset or the normalization resets
    # every cursor everywhere, so the literal is asserted rather than derived.
    test "is exactly \"producer/id\"" do
      assert Handler.subscription_id(Handlers.Echo) == "test/echo"
    end

    test "renders atoms and strings identically" do
      # Otherwise a config style change would silently mint a second identity.
      assert declare(producer: :orders, id: :order_projection) ==
               declare(producer: "orders", id: "order_projection")
    end

    test "does not depend on the module's name" do
      # The substance of the whole design: renaming a handler is free.
      first = declare(producer: :orders, id: :order_projection)
      second = declare(producer: :orders, id: :order_projection)

      assert first == second
      assert first == "orders/order_projection"
    end

    test "rejects a module that is not a handler" do
      assert_raise ArgumentError, ~r/is not a DMEx handler/, fn ->
        Handler.subscription_id(Handlers.NotAHandler)
      end
    end
  end

  describe "validation" do
    test "requires :producer" do
      assert_raise ArgumentError, ~r/requires :producer/, fn ->
        compile(id: :some_id, messages: [OrderPlaced])
      end
    end

    test "requires :id" do
      assert_raise ArgumentError, ~r/requires :id/, fn ->
        compile(producer: :orders, messages: [OrderPlaced])
      end
    end

    test "requires :messages" do
      assert_raise ArgumentError, ~r/requires :messages/, fn ->
        compile(producer: :orders, id: :some_id)
      end
    end

    test "rejects an unknown option" do
      assert_raise ArgumentError, ~r/unknown option \[:url\]/, fn ->
        compile(
          producer: :orders,
          id: :some_id,
          messages: [OrderPlaced],
          url: "https://example.test/stream"
        )
      end
    end

    # The charset is what makes "/" an unambiguous separator with no escaping,
    # so two distinct pairs can never render to one string.
    for {label, value} <- [
          {"a dash", ":\"order-projection\""},
          {"a dot", ":\"orders.projection\""},
          {"a capital", ":OrderProjection"},
          {"a leading digit", ":\"1st\""},
          {"a slash", ":\"orders/projection\""}
        ] do
      test "rejects #{label} in :id" do
        assert_raise ArgumentError, ~r/must be an atom or a string matching/, fn ->
          compile_raw(
            "producer: :orders, id: #{unquote(value)}, messages: [#{inspect(OrderPlaced)}]"
          )
        end
      end
    end

    test "rejects a non-identifier :producer" do
      assert_raise ArgumentError, ~r/:producer.*must be an atom or a string matching/s, fn ->
        compile(producer: "Orders", id: :some_id, messages: [OrderPlaced])
      end
    end

    test "rejects an empty :messages" do
      assert_raise ArgumentError, ~r/:messages.*the list is empty/s, fn ->
        compile(producer: :orders, id: :some_id, messages: [])
      end
    end

    test "rejects a :messages that is not a list" do
      assert_raise ArgumentError, ~r/:messages.*non-empty list/s, fn ->
        compile(producer: :orders, id: :some_id, messages: OrderPlaced)
      end
    end

    test "rejects a non-module entry in :messages" do
      assert_raise ArgumentError, ~r/:messages.*is not a module/s, fn ->
        compile(producer: :orders, id: :some_id, messages: ["OrderPlaced"])
      end
    end

    # Deliberately *not* checked here. Touching a message module from inside the
    # macro would make every handler compile-time dependent on every type it
    # lists, so editing one struct would recompile all of them. It is
    # DMEx.Consumer.Pipeline.build/1 that rejects this, at runtime.
    test "does not check that the declared modules are messages" do
      assert compile(producer: :orders, id: :some_id, messages: [NoSuchMessageAnywhere])
    end
  end

  ## Helpers

  defp declare(opts) do
    opts
    |> Keyword.put_new(:messages, [OrderPlaced])
    |> compile()
    |> Map.fetch!(:id)
  end

  defp compile(opts) do
    opts
    |> Enum.map_join(", ", fn {key, value} -> "#{key}: #{inspect(value)}" end)
    |> compile_raw()
  end

  # Compiled into a uniquely-named module each time, so nothing here depends on
  # a module name — which is the point of the design under test.
  defp compile_raw(declaration) do
    name = "DMEx.Consumer.HandlerTest.Generated#{System.unique_integer([:positive])}"

    {{:module, module, _, _}, _} =
      Code.eval_string("""
      defmodule #{name} do
        use DMEx.Consumer.Handler, #{declaration}

        @impl true
        def handle_message(_message, _metadata), do: :ok
      end
      """)

    module.__dmex__()
  end
end
