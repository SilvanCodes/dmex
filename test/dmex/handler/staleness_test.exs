defmodule DMEx.Handler.StalenessTest do
  @moduledoc """
  A handler caches its message set as strings, and a compile-time edge on the
  modules those strings came from is what keeps the two in step — so no
  `mix compile` can produce a stale handler. What can is a module replaced
  without one, which is what redefining a message module here is: the same thing
  `Code.eval_string/2`, a partial hot load, or a release assembled from two
  builds does to a running node.
  """

  use DMEx.Case, async: false

  import ExUnit.CaptureIO

  alias DMEx.Handler
  alias DMEx.Test.Handlers

  @message DMEx.Handler.StalenessTest.Message

  describe "verify_fresh!/1" do
    test "every handler this suite compiles is fresh" do
      for handler <- Handler.discover(:dmex) do
        assert :ok = handler |> Handler.declaration!() |> Handler.verify_fresh!()
      end
    end

    test "a handler whose message changed its type is stale" do
      define_message!("staleness_before")
      handler = define_handler!()

      assert :ok = Handler.verify_fresh!(handler.__dmex__())
      baked = handler.__dmex__().fingerprint

      define_message!("staleness_after")

      # The defect itself: the message reports the new type, the handler still
      # requests — and decodes — the old one.
      # `apply/3`: the module only exists at runtime, so a direct call is a
      # compile-time warning about an undefined function.
      # credo:disable-for-next-line Credo.Check.Refactor.Apply
      assert apply(@message, :type, []) == "staleness_after"
      assert handler.__dmex__().fingerprint == baked
      assert handler.__dmex_cast__("staleness_after", %{}, %{}) == nil

      assert_raise ArgumentError, ~r/is stale/, fn ->
        Handler.verify_fresh!(handler.__dmex__())
      end
    end

    test "the error names the handler, its live types and the fix" do
      define_message!("staleness_before")
      handler = define_handler!()
      define_message!("staleness_after")

      error = assert_raise(ArgumentError, fn -> Handler.verify_fresh!(handler.__dmex__()) end)
      message = error.message

      assert message =~ inspect(handler)
      assert message =~ "staleness_after"
      assert message =~ "mix compile --force"
    end

    test "a non-selective handler is checked too" do
      assert :ok = Handler.verify_fresh!(Handlers.EchoWholeLog.__dmex__())
    end

    # `selective: false` caches no fingerprint, but it caches decode clauses
    # just the same — and it subscribes to the whole log, so the rows do arrive
    # to be dropped.
    test "a non-selective handler whose message changed its type is stale" do
      define_message!("staleness_before")
      handler = define_handler!(selective: false)

      assert :ok = Handler.verify_fresh!(handler.__dmex__())
      assert handler.__dmex__().fingerprint == nil

      define_message!("staleness_after")

      assert handler.__dmex_cast__("staleness_after", %{order_id: 1}, %{}) == nil

      assert_raise ArgumentError, ~r/is stale/, fn ->
        Handler.verify_fresh!(handler.__dmex__())
      end
    end
  end

  describe "the check at boot" do
    test "the boot check refuses one before any child starts" do
      define_message!("staleness_before")
      handler = define_handler!()
      define_message!("staleness_after")

      assert_raise ArgumentError, ~r/is stale/, fn ->
        DMEx.Supervisor.__startable__!([handler], %{"test" => "http://127.0.0.1:1/stream"})
      end
    end

    test "the boot check refuses a stale non-selective one too" do
      define_message!("staleness_before")
      handler = define_handler!(selective: false)
      define_message!("staleness_after")

      assert_raise ArgumentError, ~r/is stale/, fn ->
        DMEx.Supervisor.__startable__!([handler], %{"test" => "http://127.0.0.1:1/stream"})
      end
    end

    test "a fresh handler passes" do
      define_message!("staleness_before")
      handler = define_handler!()

      assert :ok =
               Handler.verify_fresh!(handler.__dmex__())

      assert [^handler] =
               DMEx.Supervisor.__startable__!([handler], %{
                 "test" => "http://127.0.0.1:1/stream"
               })
    end

    # Deliberately once per boot, so this pins the absence rather than leaving it
    # to be re-added as an obvious improvement: `subscribe/2` still fails here,
    # for want of a supervisor, and says nothing about staleness.
    test "subscribe/2 does not repeat the check" do
      define_message!("staleness_before")
      handler = define_handler!()
      define_message!("staleness_after")

      error =
        assert_raise ArgumentError, fn ->
          DMEx.Supervisor.subscribe(handler, supervisor: NoSuchSupervisor)
        end

      refute error.message =~ "is stale"
    end
  end

  describe "the comparison itself" do
    # The cached list rather than the fingerprint built from it, which is what
    # makes one check cover a handler that has no fingerprint.
    test "a cached list naming a different set is what is detected" do
      declaration = Handlers.Echo.__dmex__()

      assert_raise ArgumentError, ~r/is stale/, fn ->
        Handler.verify_fresh!(%{declaration | types: ["some.other.type"]})
      end
    end
  end

  ## Helpers

  # Redefining the module in place is what an incremental recompile does to it:
  # the BEAM is replaced, every handler compiled against it is not. The warning
  # that redefinition prints is the only thing captured.
  defp define_message!(type) do
    capture_io(:stderr, fn ->
      Code.eval_string("""
      defmodule #{inspect(@message)} do
        use DMEx.Message, type: "#{type}", fields: [:order_id]
      end
      """)
    end)

    @message
  end

  defp define_handler!(opts \\ []) do
    name = "DMEx.Handler.StalenessTest.Handler#{System.unique_integer([:positive])}"
    selective = Keyword.get(opts, :selective, true)

    {{:module, module, _, _}, _} =
      Code.eval_string("""
      defmodule #{name} do
        use DMEx.Handler, logs: [:test], id: :staleness, selective: #{selective}

        @impl true
        def handle_message(%#{inspect(@message)}{}, _metadata), do: :ok
      end
      """)

    module
  end
end
