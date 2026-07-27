defmodule DMEx.Consumer.HandlerTest do
  use DMEx.Case, async: true

  alias DMEx.Consumer.Handler
  alias DMEx.Test.Handlers

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

  test "`use` takes no options" do
    # Subscription settings belong on subscribe/2 — a handler declares how to
    # process a message, not which stream it came from.
    assert_raise ArgumentError, ~r/takes no options.*subscribe\/2/s, fn ->
      Code.eval_string("""
      defmodule DMEx.Consumer.HandlerTest.WithOpts do
        use DMEx.Consumer.Handler, url: "https://example.test/stream"
      end
      """)
    end
  end

  test "no default handle_message/2 is injected" do
    # A silently-succeeding default would let a handler that forgot to implement
    # anything advance its cursor over every message it was given.
    {result, _} =
      Code.eval_string("""
      defmodule DMEx.Consumer.HandlerTest.Empty do
        use DMEx.Consumer.Handler
      end
      """)

    {:module, module, _, _} = result

    refute function_exported?(module, :handle_message, 2)
  end
end
