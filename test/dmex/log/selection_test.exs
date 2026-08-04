defmodule DMEx.Log.SelectionTest do
  use DMEx.Case, async: true

  alias DMEx.Handler
  alias DMEx.Log.Selection
  alias DMEx.Test.Handlers.Echo
  alias DMEx.Test.Messages.OrderPlaced

  describe "declared/2" do
    test "is the handler's own types, exactly" do
      declaration = Handler.declaration!(Echo)

      assert Selection.declared(declaration.messages, declaration.fingerprint) ==
               MapSet.new([OrderPlaced.type()])
    end

    test "a nil fingerprint is the whole log, whatever the handler declares" do
      declaration = Handler.declaration!(Echo)

      assert Selection.declared(declaration.messages, nil) == :all
    end
  end

  describe "expand/2" do
    test "leaves a whole-log request alone, without asking the log anything" do
      assert Selection.expand(:no_such_log, :all) == :all
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
