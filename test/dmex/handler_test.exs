defmodule DMEx.HandlerTest do
  use DMEx.Case, async: true

  alias DMEx.Handler
  alias DMEx.Handler.Fingerprint
  alias DMEx.Test.Handlers
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Messages.OrderShipped
  alias DMEx.Test.Messages.Renamed

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

    test "carries the logs, the name, the module and the derived message set" do
      assert %{
               handler: Handlers.EchoAll,
               logs: ["test"],
               name: "echo_all",
               messages: messages
             } = Handlers.EchoAll.__dmex_handler__()

      assert messages == MapSet.new([OrderPlaced, OrderShipped])
    end

    # The modules are the only copy of the set: the strings are read off them,
    # here as everywhere else, rather than cached beside them.
    test "carries no type strings of its own" do
      refute Map.has_key?(Handlers.EchoAll.__dmex_handler__(), :types)
    end

    test "the fingerprint is those modules' types encoded, so the two cannot disagree" do
      %{messages: messages, fingerprint: fingerprint} = Handlers.EchoAll.__dmex_handler__()

      assert {:ok, built} = messages |> Enum.map(& &1.type()) |> Enum.sort() |> Fingerprint.build()
      assert Fingerprint.encode(built) == fingerprint
    end

    # No separate boolean: a nil fingerprint *is* that answer, and two fields
    # saying the same thing could only ever disagree. `DMEx.Supervisor` passes
    # whatever is here with no decision of its own.
    test "a whole-log handler declares the same set, with no fingerprint" do
      assert %{messages: messages, fingerprint: nil} = Handlers.EchoWholeLog.__dmex_handler__()

      assert messages == MapSet.new([OrderPlaced])
    end

    # The message set's second job. Built here rather than at connect because
    # this is the first point `type/0` is callable — `Code.ensure_compiled!/1`
    # has just run on every matched module — and because a constant is what
    # makes "one URL per deployment" literal.
    test "carries the message set encoded as a selection fingerprint" do
      %{fingerprint: fingerprint, messages: messages} = Handlers.EchoAll.__dmex_handler__()

      assert is_binary(fingerprint)
      assert {:ok, decoded} = Fingerprint.decode(fingerprint)

      # No false negatives is the whole guarantee, so it is worth asserting on
      # the real handler rather than only in the property test.
      for message <- messages do
        assert Fingerprint.member?(decoded, message.type())
      end
    end

    test "selection defaults to on" do
      assert %{fingerprint: fingerprint} = Handlers.Echo.__dmex_handler__()
      assert is_binary(fingerprint)
    end

    test "a clause naming no type is what turns selection off, wherever it sits" do
      assert %{fingerprint: nil, messages: %MapSet{}} =
               compile("""
               def handle_message(_message, _metadata), do: :ok
               def handle_message(%DMEx.Test.Messages.OrderPlaced{}, _metadata), do: :ok
               """)
    end

    # `%module{}` matches every struct and every message is one, so it names no
    # type in exactly the way a bare variable does — and binds the module, which
    # is what a catch-all usually wants.
    test "a variable struct pattern names no type either" do
      for pattern <- ["%module{}", "%module{} = message", "message = %module{}"] do
        assert %{fingerprint: nil, messages: %MapSet{}} =
                 compile("""
                 def handle_message(%DMEx.Test.Messages.OrderPlaced{}, _metadata), do: :ok
                 def handle_message(#{pattern}, _metadata), do: {:other, module}
                 """)
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
      assert_raise ArgumentError, ~r/reads 2 logs \(test, other\).*`for: :test`/s, fn ->
        Handler.log!(Handlers.Fanned, [])
      end
    end

    test "takes the one the call names, as an atom or a string" do
      assert Handler.log!(Handlers.Fanned, for: :other) == "other"
      assert Handler.log!(Handlers.Fanned, for: "test") == "test"
    end

    test "rejects a log the handler does not read, even when it reads one" do
      assert_raise ArgumentError, ~r/does not read "elsewhere"/, fn ->
        Handler.log!(Handlers.Echo, for: :elsewhere)
      end
    end
  end

  describe "the subscription id" do
    # **Frozen.** This string is a text primary key in every deployment's
    # database. Changing the separator, the charset or the normalization resets
    # every cursor everywhere, so the literal is asserted rather than derived.
    test "is exactly \"log/id\"" do
      assert Handler.subscription_id!(Handlers.Echo, :test) == "test/echo"
    end

    test "is one per log the handler reads" do
      assert Handler.subscription_ids(Handlers.Echo) == ["test/echo"]
      assert Handler.subscription_ids(Handlers.Fanned) == ["test/fanned", "other/fanned"]
    end

    test "rejects a log the handler does not read" do
      assert_raise ArgumentError, ~r/does not read "elsewhere"/, fn ->
        Handler.subscription_id!(Handlers.Echo, :elsewhere)
      end
    end

    test "renders atoms and strings identically" do
      # Otherwise a config style change would silently mint a second identity.
      assert declare(logs: [:orders], name: :order_projection) ==
               declare(logs: ["orders"], name: "order_projection")
    end

    test "does not depend on the module's name" do
      # The substance of the whole design: renaming a handler is free.
      first = declare(logs: [:orders], name: :order_projection)
      second = declare(logs: [:orders], name: :order_projection)

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
    test "rejects a pattern no message could match" do
      for pattern <- [~s|%{"id" => id}|, ~s|"literal"|, "x = 5", "[_ | _]"] do
        assert_raise ArgumentError, ~r/matches something no message can be/, fn ->
          compile("def handle_message(#{pattern}, _metadata), do: :ok")
        end
      end
    end

    test "names the file and line of the offending clause" do
      assert_raise ArgumentError, ~r/handler_test\.exs:\d+: this handle_message/, fn ->
        compile(~s|def handle_message("literal", _metadata), do: :ok|)
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
        compile_raw("logs: [:orders], name: :some_id", "")
      end
    end
  end

  # What a whole-log handler decodes is not its clauses but its application: it
  # asked for every row, and a clause naming no type accepts every message.
  describe "casting on a whole-log handler" do
    test "resolves a type no clause named" do
      assert %OrderShipped{order_id: 1, carrier: "dhl"} =
               Handlers.EchoWholeLog.__dmex_cast__(
                 OrderShipped.type(),
                 %{"order_id" => 1, "carrier" => "dhl"},
                 %{}
               )
    end

    test "a handler naming no type at all is legal, and decodes the same way" do
      assert Handlers.Sink.__dmex_handler__().messages == MapSet.new([])

      assert %OrderPlaced{order_id: 7} =
               Handlers.Sink.__dmex_cast__(OrderPlaced.type(), %{"order_id" => 7}, %{})
    end

    # The limit of it, and what keeps such a handler a diagnostic rather than a
    # promise: nothing here can turn this into a struct, so it stays a skip.
    test "a type no module declares is still nil" do
      assert Handlers.Sink.__dmex_cast__("nothing.declares.this", %{}, %{}) == nil
    end

    # Why the fallback still asks whether a clause wants the message: naming no
    # type is a property of the first argument, where accepting one is the whole
    # head. Without the test this row would be delivered and `handle_message/2`
    # would raise `FunctionClauseError` — a handler failure, retried and
    # dead-lettered, for a message nothing ever wanted.
    test "a whole-log clause that narrows elsewhere still turns a message down" do
      shipped = %{"order_id" => 1, "carrier" => "dhl"}

      assert Handlers.WholeLogNarrowed.__dmex_cast__(OrderShipped.type(), shipped, %{}) == nil

      assert %OrderShipped{order_id: 1} =
               Handlers.WholeLogNarrowed.__dmex_cast__(
                 OrderShipped.type(),
                 shipped,
                 %{"replay" => true}
               )
    end

    test "a selective handler resolves nothing it did not name" do
      assert Handlers.Echo.__dmex_cast__(
               OrderShipped.type(),
               %{"order_id" => 1, "carrier" => "dhl"},
               %{}
             ) == nil
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

    # The whole point of a retirement: rows appended before the rename still
    # decode, through the same module and the same `cast/1`.
    test "decodes a row arriving under a retired type" do
      body = """
      def handle_message(%DMEx.Test.Messages.Renamed{} = message, _md), do: message
      """

      module = compile_module(body)

      assert %Renamed{order_id: 1} = module.__dmex_cast__("orders.placed", %{"order_id" => 1}, %{})

      assert %Renamed{order_id: 1} =
               module.__dmex_cast__("order_placed_legacy", %{"order_id" => 1}, %{})
    end

    # Decoding a retired type is worth nothing if the subscription never asks
    # for it: the producer builds its SQL predicate out of the types this
    # fingerprint admits.
    test "selects a retired type in the fingerprint" do
      body = """
      def handle_message(%DMEx.Test.Messages.Renamed{}, _md), do: :ok
      """

      %{fingerprint: encoded} = compile(body)
      {:ok, filter} = Fingerprint.decode(encoded)

      assert Fingerprint.member?(filter, "orders.placed")
      assert Fingerprint.member?(filter, "order_placed_legacy")
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

          use DMEx.Handler, logs: [:orders], name: :some_id
        end
        """)
      end
    end
  end

  describe "validating the options" do
    test "requires :logs" do
      assert_raise ArgumentError, ~r/requires :logs/, fn ->
        compile_raw("name: :some_id", clause())
      end
    end

    test "requires :name" do
      assert_raise ArgumentError, ~r/requires :name/, fn ->
        compile_raw("logs: [:orders]", clause())
      end
    end

    test "accepts a bare name as a one-log list" do
      assert declare(logs: :orders, name: :order_projection) == ["orders/order_projection"]
    end

    test "rejects an empty :logs" do
      assert_raise ArgumentError, ~r/names no log/, fn ->
        compile_raw("logs: [], name: :some_id", clause())
      end
    end

    test "rejects a log named twice" do
      assert_raise ArgumentError, ~r/names "orders" twice/, fn ->
        compile_raw("logs: [:orders, :orders], name: :some_id", clause())
      end
    end

    test "rejects an unknown option" do
      assert_raise ArgumentError, ~r/unknown option \[:url\]/, fn ->
        compile_raw(~s(logs: [:orders], name: :some_id, url: "https://x.test"), clause())
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
      test "rejects #{label} in :name" do
        assert_raise ArgumentError, ~r/must be an atom or a string matching/, fn ->
          compile_raw("logs: [:orders], name: #{unquote(value)}", clause())
        end
      end
    end

    test "rejects a non-identifier log name" do
      assert_raise ArgumentError, ~r/:logs.*must be an atom or a string matching/s, fn ->
        compile_raw(~s(logs: ["Orders"], name: :some_id), clause())
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
    |> then(&Enum.map(&1.logs, fn log -> log <> "/" <> &1.name end))
  end

  defp compile(body), do: compile_raw("logs: [:orders], name: :some_id", body)

  defp compile_module(body), do: compile_module_raw("logs: [:orders], name: :some_id", body)

  defp messages_of(body), do: body |> compile() |> Map.fetch!(:messages)

  defp compile_raw(declaration, body) do
    compile_module_raw(declaration, body).__dmex_handler__()
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
