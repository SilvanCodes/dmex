defmodule DMEx.Handler.StalenessTest do
  @moduledoc """
  A handler caches its message set as strings, and depends on the modules those
  strings came from by *export* — so editing a `type:` recompiles the message
  module and not the handler. Reproduced here by redefining a message module
  after a handler has compiled against it, which is exactly what an incremental
  `mix compile` leaves behind.
  """

  use DMEx.Case, async: false

  import ExUnit.CaptureIO

  alias DMEx.Handler
  alias DMEx.Handler.Fingerprint
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

    # `selective: false` carries no fingerprint, so there is nothing cached to
    # disagree with.
    test "a non-selective handler has nothing to compare" do
      assert :ok = Handler.verify_fresh!(Handlers.EchoWholeLog.__dmex__())
    end
  end

  describe "the check at the sites a subscription starts" do
    test "subscribe/2 refuses a stale handler" do
      define_message!("staleness_before")
      handler = define_handler!()
      define_message!("staleness_after")

      assert_raise ArgumentError, ~r/is stale/, fn ->
        DMEx.Supervisor.subscribe(handler, supervisor: NoSuchSupervisor)
      end
    end

    test "the boot check refuses one before any child starts" do
      define_message!("staleness_before")
      handler = define_handler!()
      define_message!("staleness_after")

      assert_raise ArgumentError, ~r/is stale/, fn ->
        DMEx.Supervisor.__startable__!([handler], %{"test" => "http://127.0.0.1:1/stream"})
      end
    end

    test "a fresh handler passes both" do
      define_message!("staleness_before")
      handler = define_handler!()

      assert :ok =
               Handler.verify_fresh!(handler.__dmex__())

      assert [^handler] =
               DMEx.Supervisor.__startable__!([handler], %{
                 "test" => "http://127.0.0.1:1/stream"
               })
    end
  end

  describe "the comparison itself" do
    test "a fingerprint over a different set is what is detected" do
      declaration = Handlers.Echo.__dmex__()
      {:ok, other} = Fingerprint.build(["some.other.type"])

      assert_raise ArgumentError, ~r/is stale/, fn ->
        Handler.verify_fresh!(%{declaration | fingerprint: Fingerprint.encode(other)})
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

  defp define_handler! do
    name = "DMEx.Handler.StalenessTest.Handler#{System.unique_integer([:positive])}"

    {{:module, module, _, _}, _} =
      Code.eval_string("""
      defmodule #{name} do
        use DMEx.Handler, logs: [:test], id: :staleness

        @impl true
        def handle_message(%#{inspect(@message)}{}, _metadata), do: :ok
      end
      """)

    module
  end
end
