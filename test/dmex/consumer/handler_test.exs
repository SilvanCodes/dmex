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

    test "carries the producer, the id, the module and the derived message list" do
      assert %{
               handler: Handlers.EchoAll,
               producer: "test",
               id: "test/echo_all",
               messages: [OrderPlaced, OrderShipped]
             } = Handlers.EchoAll.__dmex__()
    end

    # The message list's second job. Built here rather than at connect because
    # this is the first point `type/0` is callable — `Code.ensure_compiled!/1`
    # has just run on every matched module — and because a constant is what
    # makes "one URL per deployment" literal.
    test "carries the message list encoded as a selection fingerprint" do
      %{fingerprint: fingerprint, messages: messages} = Handlers.EchoAll.__dmex__()

      assert is_binary(fingerprint)
      assert {:ok, decoded} = DMEx.Fingerprint.decode(fingerprint)

      # No false negatives is the whole guarantee, so it is worth asserting on
      # the real handler rather than only in the property test.
      for message <- messages do
        assert DMEx.Fingerprint.member?(decoded, message.type())
      end
    end

    test "selection defaults to on" do
      assert %{fingerprint: fingerprint} = Handlers.Echo.__dmex__()
      assert is_binary(fingerprint)
    end

    test "`selective: false` is carried as a nil fingerprint" do
      # No separate boolean: a nil fingerprint *is* that answer, and two fields
      # saying the same thing could only ever disagree. `DMEx.Consumer` passes
      # whatever is here with no decision of its own.
      declaration = Handlers.EchoWholeLog.__dmex__()

      assert declaration.fingerprint == nil
    end

    test "`selective:` must be a boolean" do
      assert_raise ArgumentError, ~r/:selective in .* must be true .* or false/s, fn ->
        defmodule NotABoolean do
          use DMEx.Consumer.Handler, producer: :test, id: :not_a_boolean, selective: :sometimes

          def handle_message(%OrderPlaced{}, _metadata), do: :ok
        end
      end
    end
  end

  # The substance of deriving rather than declaring: the list cannot disagree
  # with the clauses, because it *is* the clauses.
  describe "the message list" do
    test "is the structs handle_message/2 matches on, in clause order" do
      assert messages_of("""
             def handle_message(%DMEx.Test.Messages.OrderShipped{}, _metadata), do: :ok
             def handle_message(%DMEx.Test.Messages.OrderPlaced{}, _metadata), do: :ok
             """) == [OrderShipped, OrderPlaced]
    end

    test "reads through an alias" do
      assert messages_of("""
             alias DMEx.Test.Messages.OrderPlaced

             def handle_message(%OrderPlaced{} = message, _metadata), do: message
             """) == [OrderPlaced]
    end

    test "reads a bound struct pattern, either way round" do
      assert messages_of("""
             def handle_message(%DMEx.Test.Messages.OrderPlaced{} = m, _metadata), do: m
             """) == [OrderPlaced]

      assert messages_of("""
             def handle_message(m = %DMEx.Test.Messages.OrderPlaced{}, _metadata), do: m
             """) == [OrderPlaced]
    end

    test "reads a pattern that also matches on fields" do
      assert messages_of("""
             def handle_message(%DMEx.Test.Messages.OrderPlaced{order_id: id}, _meta), do: id
             """) == [OrderPlaced]
    end

    test "collapses two clauses for one type" do
      assert messages_of("""
             def handle_message(%DMEx.Test.Messages.OrderPlaced{order_id: 1}, _meta), do: :one
             def handle_message(%DMEx.Test.Messages.OrderPlaced{}, _meta), do: :other
             """) == [OrderPlaced]
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

  describe "validating the clauses" do
    # A bare variable declares no type at all. Deriving an empty list from it
    # would give a handler that compiles, subscribes, advances its cursor over
    # everything and invokes nothing.
    test "rejects a clause that does not match on a struct" do
      assert_raise ArgumentError, ~r/does not match on a struct/, fn ->
        compile("def handle_message(_message, _metadata), do: :ok")
      end
    end

    # And it cannot be a fallback either: only declared types are ever
    # delivered, so a catch-all after the struct clauses is unreachable.
    test "rejects a catch-all sitting behind a struct clause" do
      assert_raise ArgumentError, ~r/cannot serve as a fallback/, fn ->
        compile("""
        def handle_message(%DMEx.Test.Messages.OrderPlaced{}, _metadata), do: :ok
        def handle_message(_message, _metadata), do: :ignored
        """)
      end
    end

    test "names the file and line of the offending clause" do
      assert_raise ArgumentError, ~r/handler_test\.exs:\d+: this handle_message/, fn ->
        compile("def handle_message(_message, _metadata), do: :ok")
      end
    end

    # A struct that is not a message. Caught here rather than as an
    # UndefinedFunctionError on `type/0` when a live row arrives.
    test "rejects a struct that is not a DMEx message" do
      assert_raise ArgumentError, ~r/DateTime, which is not a DMEx message module/, fn ->
        compile("def handle_message(%DateTime{} = message, _metadata), do: message")
      end
    end

    test "rejects a handler with no handle_message/2 at all" do
      assert_raise ArgumentError, ~r/defines no handle_message\/2/, fn ->
        compile_raw("producer: :orders, id: :some_id", "")
      end
    end
  end

  describe "validating the options" do
    test "requires :producer" do
      assert_raise ArgumentError, ~r/requires :producer/, fn ->
        compile_raw("id: :some_id", clause())
      end
    end

    test "requires :id" do
      assert_raise ArgumentError, ~r/requires :id/, fn ->
        compile_raw("producer: :orders", clause())
      end
    end

    test "rejects an unknown option" do
      assert_raise ArgumentError, ~r/unknown option \[:url\]/, fn ->
        compile_raw(~s(producer: :orders, id: :some_id, url: "https://x.test"), clause())
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
          compile_raw("producer: :orders, id: #{unquote(value)}", clause())
        end
      end
    end

    test "rejects a non-identifier :producer" do
      assert_raise ArgumentError, ~r/:producer.*must be an atom or a string matching/s, fn ->
        compile_raw(~s(producer: "Orders", id: :some_id), clause())
      end
    end
  end

  ## Helpers

  defp clause do
    "def handle_message(%DMEx.Test.Messages.OrderPlaced{}, _metadata), do: :ok"
  end

  defp declare(opts) do
    opts
    |> Enum.map_join(", ", fn {key, value} -> "#{key}: #{inspect(value)}" end)
    |> compile_raw(clause())
    |> Map.fetch!(:id)
  end

  defp compile(body), do: compile_raw("producer: :orders, id: :some_id", body)

  defp messages_of(body), do: body |> compile() |> Map.fetch!(:messages)

  # Compiled into a uniquely-named module each time, so nothing here depends on
  # a module name — which is the point of the design under test.
  defp compile_raw(declaration, body) do
    name = "DMEx.Consumer.HandlerTest.Generated#{System.unique_integer([:positive])}"

    {{:module, module, _, _}, _} =
      Code.eval_string(
        """
        defmodule #{name} do
          use DMEx.Consumer.Handler, #{declaration}

          #{body}
        end
        """,
        [],
        file: __ENV__.file
      )

    module.__dmex__()
  end
end
