defmodule DMEx.HandlerTest do
  use DMEx.Case, async: true

  alias DMEx.Handler
  alias DMEx.Handler.Fingerprint
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

    test "carries the logs, the id, the module and the derived message set" do
      assert %{
               handler: Handlers.EchoAll,
               logs: ["test"],
               id: "echo_all",
               messages: messages
             } = Handlers.EchoAll.__dmex__()

      assert messages == MapSet.new([OrderPlaced, OrderShipped])
    end

    # The message set's second job. Built here rather than at connect because
    # this is the first point `type/0` is callable — `Code.ensure_compiled!/1`
    # has just run on every matched module — and because a constant is what
    # makes "one URL per deployment" literal.
    test "carries the message set encoded as a selection fingerprint" do
      %{fingerprint: fingerprint, messages: messages} = Handlers.EchoAll.__dmex__()

      assert is_binary(fingerprint)
      assert {:ok, decoded} = Fingerprint.decode(fingerprint)

      # No false negatives is the whole guarantee, so it is worth asserting on
      # the real handler rather than only in the property test.
      for message <- messages do
        assert Fingerprint.member?(decoded, message.type())
      end
    end

    test "selection defaults to on" do
      assert %{fingerprint: fingerprint} = Handlers.Echo.__dmex__()
      assert is_binary(fingerprint)
    end

    test "`selective: false` is carried as a nil fingerprint" do
      # No separate boolean: a nil fingerprint *is* that answer, and two fields
      # saying the same thing could only ever disagree. `DMEx.Supervisor` passes
      # whatever is here with no decision of its own.
      declaration = Handlers.EchoWholeLog.__dmex__()

      assert declaration.fingerprint == nil
    end

    test "`selective:` must be a boolean" do
      assert_raise ArgumentError, ~r/:selective in .* must be true .* or false/s, fn ->
        defmodule NotABoolean do
          use DMEx.Handler, logs: [:test], id: :not_a_boolean, selective: :sometimes

          def handle_message(%OrderPlaced{}, _metadata), do: :ok
        end
      end
    end
  end

  # The substance of deriving rather than declaring: the set cannot disagree
  # with the clauses, because it *is* the clauses.
  describe "the message set" do
    test "is the structs handle_message/2 matches on" do
      assert messages_of("""
             def handle_message(%DMEx.Test.Messages.OrderShipped{}, _metadata), do: :ok
             def handle_message(%DMEx.Test.Messages.OrderPlaced{}, _metadata), do: :ok
             """) == MapSet.new([OrderPlaced, OrderShipped])
    end

    test "is a MapSet, so clause order carries no information" do
      # The fingerprint is already order-independent; this makes the declaration
      # itself say so, rather than leaving a list whose order means nothing.
      forward =
        messages_of("""
        def handle_message(%DMEx.Test.Messages.OrderPlaced{}, _metadata), do: :ok
        def handle_message(%DMEx.Test.Messages.OrderShipped{}, _metadata), do: :ok
        """)

      reversed =
        messages_of("""
        def handle_message(%DMEx.Test.Messages.OrderShipped{}, _metadata), do: :ok
        def handle_message(%DMEx.Test.Messages.OrderPlaced{}, _metadata), do: :ok
        """)

      assert %MapSet{} = forward
      assert forward == reversed
    end

    test "reads through an alias" do
      assert messages_of("""
             alias DMEx.Test.Messages.OrderPlaced

             def handle_message(%OrderPlaced{} = message, _metadata), do: message
             """) == MapSet.new([OrderPlaced])
    end

    test "reads a bound struct pattern, either way round" do
      assert messages_of("""
             def handle_message(%DMEx.Test.Messages.OrderPlaced{} = m, _metadata), do: m
             """) == MapSet.new([OrderPlaced])

      assert messages_of("""
             def handle_message(m = %DMEx.Test.Messages.OrderPlaced{}, _metadata), do: m
             """) == MapSet.new([OrderPlaced])
    end

    test "reads a pattern that also matches on fields" do
      assert messages_of("""
             def handle_message(%DMEx.Test.Messages.OrderPlaced{order_id: id}, _meta), do: id
             """) == MapSet.new([OrderPlaced])
    end

    test "collapses two clauses for one type" do
      assert messages_of("""
             def handle_message(%DMEx.Test.Messages.OrderPlaced{order_id: 1}, _meta), do: :one
             def handle_message(%DMEx.Test.Messages.OrderPlaced{}, _meta), do: :other
             """) == MapSet.new([OrderPlaced])
    end
  end

  # Every operator-facing call addresses exactly one cursor, so a handler reading
  # several logs has to be told which — and one reading a single log never does.
  describe "log!/2" do
    test "answers the only log when the handler reads one" do
      assert Handler.log!(Handlers.Echo, []) == "test"
    end

    test "refuses to guess for a handler reading several" do
      assert_raise ArgumentError, ~r/reads 2 logs \(test, other\).*`log: :test`/s, fn ->
        Handler.log!(Handlers.Fanned, [])
      end
    end

    test "takes the one the call names, as an atom or a string" do
      assert Handler.log!(Handlers.Fanned, log: :other) == "other"
      assert Handler.log!(Handlers.Fanned, log: "test") == "test"
    end

    test "rejects a log the handler does not read, even when it reads one" do
      assert_raise ArgumentError, ~r/does not read "elsewhere"/, fn ->
        Handler.log!(Handlers.Echo, log: :elsewhere)
      end
    end
  end

  describe "the subscription id" do
    # **Frozen.** This string is a text primary key in every deployment's
    # database. Changing the separator, the charset or the normalization resets
    # every cursor everywhere, so the literal is asserted rather than derived.
    test "is exactly \"log/id\"" do
      assert Handler.subscription_id(Handlers.Echo, :test) == "test/echo"
    end

    test "is one per log the handler reads" do
      assert Handler.subscription_ids(Handlers.Echo) == ["test/echo"]
      assert Handler.subscription_ids(Handlers.Fanned) == ["test/fanned", "other/fanned"]
    end

    test "rejects a log the handler does not read" do
      assert_raise ArgumentError, ~r/does not read "elsewhere"/, fn ->
        Handler.subscription_id(Handlers.Echo, :elsewhere)
      end
    end

    test "renders atoms and strings identically" do
      # Otherwise a config style change would silently mint a second identity.
      assert declare(logs: [:orders], id: :order_projection) ==
               declare(logs: ["orders"], id: "order_projection")
    end

    test "does not depend on the module's name" do
      # The substance of the whole design: renaming a handler is free.
      first = declare(logs: [:orders], id: :order_projection)
      second = declare(logs: [:orders], id: :order_projection)

      assert first == second
      assert first == ["orders/order_projection"]
    end

    test "rejects a module that is not a handler" do
      assert_raise ArgumentError, ~r/is not a DMEx handler/, fn ->
        Handler.subscription_ids(Handlers.NotAHandler)
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
        compile_raw("logs: [:orders], id: :some_id", "")
      end
    end
  end

  # Decoding and "is this wanted" are one call, asked before the pipeline opens
  # a transaction — see `DMEx.Handler.Pipeline`, section 2. `nil` is the whole
  # vocabulary for "no", whether the type is undeclared or the value unwanted.
  describe "the generated cast" do
    test "returns what the clause matches and nil for a type it never declared" do
      module = compile_module(clause())

      assert %OrderPlaced{order_id: 1} = module.__dmex_cast__("order_placed", placed(1), %{})
      assert module.__dmex_cast__("order_shipped", %{"order_id" => 1}, %{}) == nil
    end

    test "carries a narrowed field" do
      body = """
      def handle_message(%DMEx.Test.Messages.OrderPlaced{customer_id: 7}, _md), do: :ok
      """

      module = compile_module(body)

      assert module.__dmex_cast__("order_placed", %{"customer_id" => 7}, %{})
      assert module.__dmex_cast__("order_placed", %{"customer_id" => 9}, %{}) == nil
    end

    # The binding this copy has no body to use, and the guard that does use one.
    test "carries the clause's guards" do
      body = """
      def handle_message(%DMEx.Test.Messages.OrderPlaced{total: total} = message, _md)
          when total > 100 do
        message
      end
      """

      module = compile_module(body)

      assert module.__dmex_cast__("order_placed", %{"total" => 500}, %{})
      assert module.__dmex_cast__("order_placed", %{"total" => 50}, %{}) == nil
    end

    test "matches on the metadata too" do
      body = """
      def handle_message(%DMEx.Test.Messages.OrderPlaced{}, %{"t" => "acme"}), do: :ok
      """

      module = compile_module(body)

      assert module.__dmex_cast__("order_placed", placed(1), %{"t" => "acme"})
      assert module.__dmex_cast__("order_placed", placed(1), %{"t" => "other"}) == nil
    end

    test "answers for every clause" do
      body = """
      def handle_message(%DMEx.Test.Messages.OrderPlaced{}, _metadata), do: :ok
      def handle_message(%DMEx.Test.Messages.OrderShipped{}, _metadata), do: :ok
      """

      module = compile_module(body)

      assert module.__dmex_cast__("order_placed", placed(1), %{})
      assert module.__dmex_cast__("order_shipped", %{"order_id" => 2}, %{})
    end

    # The type nothing declares never reaches a `cast/1` at all — it falls to
    # the same `nil` a turned-down value gets, one clause earlier.
    test "decodes nothing for a type it never declared" do
      module = compile_module(clause())

      assert module.__dmex_cast__("nothing_it_declares", %{"order_id" => 1}, %{}) == nil
    end

    test "casts each declared type into its own module" do
      body = """
      def handle_message(%DMEx.Test.Messages.OrderPlaced{}, _metadata), do: :ok
      def handle_message(%DMEx.Test.Messages.OrderShipped{}, _metadata), do: :ok
      """

      module = compile_module(body)

      assert %OrderPlaced{order_id: 1} = module.__dmex_cast__("order_placed", placed(1), %{})

      assert %OrderShipped{order_id: 2} =
               module.__dmex_cast__("order_shipped", %{"order_id" => 2}, %{})
    end

    # Otherwise the hook that reads the patterns is installed after them, the
    # test answers false for everything, and the subscription silently skips its
    # whole log.
    test "rejects clauses defined above the `use`" do
      assert_raise ArgumentError, ~r/saw 0 of them/, fn ->
        Code.eval_string("""
        defmodule DMEx.HandlerTest.Late#{System.unique_integer([:positive])} do
          def handle_message(%DMEx.Test.Messages.OrderPlaced{}, _metadata), do: :ok

          use DMEx.Handler, logs: [:orders], id: :some_id
        end
        """)
      end
    end
  end

  describe "validating the options" do
    test "requires :logs" do
      assert_raise ArgumentError, ~r/requires :logs/, fn ->
        compile_raw("id: :some_id", clause())
      end
    end

    test "requires :id" do
      assert_raise ArgumentError, ~r/requires :id/, fn ->
        compile_raw("logs: [:orders]", clause())
      end
    end

    test "accepts a bare name as a one-log list" do
      assert declare(logs: :orders, id: :order_projection) == ["orders/order_projection"]
    end

    test "rejects an empty :logs" do
      assert_raise ArgumentError, ~r/names no log/, fn ->
        compile_raw("logs: [], id: :some_id", clause())
      end
    end

    test "rejects a log named twice" do
      assert_raise ArgumentError, ~r/names "orders" twice/, fn ->
        compile_raw("logs: [:orders, :orders], id: :some_id", clause())
      end
    end

    test "rejects an unknown option" do
      assert_raise ArgumentError, ~r/unknown option \[:url\]/, fn ->
        compile_raw(~s(logs: [:orders], id: :some_id, url: "https://x.test"), clause())
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
          compile_raw("logs: [:orders], id: #{unquote(value)}", clause())
        end
      end
    end

    test "rejects a non-identifier log name" do
      assert_raise ArgumentError, ~r/:logs.*must be an atom or a string matching/s, fn ->
        compile_raw(~s(logs: ["Orders"], id: :some_id), clause())
      end
    end
  end

  ## Helpers

  defp clause do
    "def handle_message(%DMEx.Test.Messages.OrderPlaced{}, _metadata), do: :ok"
  end

  defp placed(order_id), do: %{"order_id" => order_id}

  defp declare(opts) do
    opts
    |> Enum.map_join(", ", fn {key, value} -> "#{key}: #{inspect(value)}" end)
    |> compile_raw(clause())
    |> then(&Enum.map(&1.logs, fn log -> log <> "/" <> &1.id end))
  end

  defp compile(body), do: compile_raw("logs: [:orders], id: :some_id", body)

  defp compile_module(body), do: compile_module_raw("logs: [:orders], id: :some_id", body)

  defp messages_of(body), do: body |> compile() |> Map.fetch!(:messages)

  defp compile_raw(declaration, body) do
    compile_module_raw(declaration, body).__dmex__()
  end

  # Compiled into a uniquely-named module each time, so nothing here depends on
  # a module name — which is the point of the design under test.
  defp compile_module_raw(declaration, body) do
    name = "DMEx.HandlerTest.Generated#{System.unique_integer([:positive])}"

    {{:module, module, _, _}, _} =
      Code.eval_string(
        """
        defmodule #{name} do
          use DMEx.Handler, #{declaration}

          #{body}
        end
        """,
        [],
        file: __ENV__.file
      )

    module
  end
end
