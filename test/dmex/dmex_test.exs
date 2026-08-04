defmodule DMExTest do
  @moduledoc """
  Message-module discovery, and the application key it caches under.

  Both are load-bearing and neither was covered. `DMEx.Message.message?/1`
  decides what `use DMEx.Message` means at all — a handler's compile-time
  derivation and `append/2` both ask it — and it once answered `false` for every
  message module, which surfaced as a compile error in a fixture file rather
  than as a test. `DMEx.message_types/1` is what a selective connect resolves a
  filter against, keyed by the *application owning the log module*, which has a
  sharp edge worth pinning.
  """

  use DMEx.Case, async: true

  alias DMEx.Test.Messages.OrderPlaced

  describe "DMEx.Message.message?/1" do
    test "is true for a module built with use DMEx.Message" do
      # The positive direction, which nothing else asserts: the negative one
      # holds just as well when the predicate is broken for everything.
      assert DMEx.Message.message?(OrderPlaced)
    end

    test "is false for a look-alike exporting type/0 and cast/1" do
      # The reason the predicate tests a persisted attribute rather than the
      # module's exports: this shape is one a struct can have by coincidence.
      defmodule NotAMessage do
        @moduledoc false
        defstruct [:order_id]
        def type, do: "order_placed"
        def cast(data), do: data
      end

      refute DMEx.Message.message?(NotAMessage)
    end

    test "is false for a module that does not exist" do
      refute DMEx.Message.message?(No.Such.Module)
    end
  end

  describe "DMEx.Message.discover/1" do
    test "finds the suite's message modules under :dmex" do
      discovered = DMEx.Message.discover(:dmex)

      assert OrderPlaced in discovered
      assert DMEx.Test.Messages.OrderShipped in discovered
      refute DMEx.Test.Handlers.Echo in discovered
    end

    test "is sorted, so a collision is reported against the same pair everywhere" do
      discovered = DMEx.Message.discover(:dmex)

      assert discovered == Enum.sort(discovered)
    end
  end

  describe "DMEx.message_types/1" do
    test "resolves a log module against the application that owns it" do
      types = DMEx.message_types(DMEx.Test.IntegrationLog)

      assert MapSet.member?(types, "order_placed")
      assert MapSet.member?(types, "order_shipped")
    end

    test "a log module owned by no application resolves to nothing" do
      # The trap: `:application.get_application/1` answers `:undefined` for a
      # module defined at runtime or inside a `.exs`, so it caches under the
      # `nil` key — which sees only applications that *depend on* `:dmex`, and
      # `:dmex` does not depend on itself. A selective connect against such a
      # log 404s every time. It is why a test's log module belongs in
      # `test/support/`, not in the test file.
      [{unowned, _}] =
        Code.compile_string("""
        defmodule DMExTest.UnownedLog do
          use DMEx.Log, repo: DMEx.Test.Repo, app: :dmex_unowned
        end
        """)

      assert :application.get_application(unowned) == :undefined
      assert DMEx.message_types(unowned) == MapSet.new()
    after
      :code.purge(DMExTest.UnownedLog)
      :code.delete(DMExTest.UnownedLog)
    end
  end
end
