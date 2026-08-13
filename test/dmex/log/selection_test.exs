defmodule DMEx.Log.SelectionTest do
  use DMEx.Case, async: true

  alias DMEx.Handler
  alias DMEx.Handler.Fingerprint
  alias DMEx.Log.Selection
  alias DMEx.Test.Handlers.Echo
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Messages.Renamed

  defmodule RenamedConsumer do
    @moduledoc false
    use Handler, logs: [:test], name: :renamed_selection

    @impl true
    def handle_message(%Renamed{}, _metadata), do: :ok
  end

  describe "declared/2" do
    test "is the handler's own types, exactly" do
      declaration = Handler.declaration!(Echo)

      assert Selection.declared(declaration.messages, declaration.fingerprint) ==
               MapSet.new([OrderPlaced.type()])
    end

    test "carries the retired types the same handler requests over the wire" do
      declaration = Handler.declaration!(RenamedConsumer)

      assert Selection.declared(declaration.messages, declaration.fingerprint) ==
               MapSet.new(DMEx.Message.declared(Renamed))
    end

    # The two transports must not read different rows for one declaration: the
    # local set has to be a subset of what the fingerprint admits, or a row the
    # SSE path delivers is one the local path never reads.
    test "selects no type the handler's own fingerprint would reject" do
      declaration = Handler.declaration!(RenamedConsumer)
      {:ok, filter} = Fingerprint.decode(declaration.fingerprint)

      local = Selection.declared(declaration.messages, declaration.fingerprint)

      assert MapSet.subset?(local, Selection.expand(filter))
    end

    test "a nil fingerprint is the whole log, whatever the handler declares" do
      declaration = Handler.declaration!(Echo)

      assert Selection.declared(declaration.messages, nil) == :all
    end
  end

  describe "expand/1" do
    test "leaves a whole-log request alone, without touching the index" do
      assert Selection.expand(:all) == :all
    end
  end

  test "answers every question the log asks of a selection" do
    selection = MapSet.new(["order_placed"])

    assert Selection.concerns?(selection, ["order_placed", "order_shipped"])
    refute Selection.concerns?(selection, ["order_shipped"])
    assert Selection.size(selection) == 1
    assert Selection.to_list(selection) == ["order_placed"]
    assert Selection.selective?(selection)
  end

  test "a whole-log selection answers the same questions" do
    assert Selection.concerns?(:all, ["anything"])
    # `nil`, not `0`: no filter was applied, so no count is meaningful — and
    # `DMEx.Log.Plug` reads an empty expansion as a 404.
    assert Selection.size(:all) == nil
    assert Selection.to_list(:all) == :all
    refute Selection.selective?(:all)
  end
end
