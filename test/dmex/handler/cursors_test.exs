defmodule DMEx.Handler.CursorsTest do
  use DMEx.Case, async: true, database: true

  alias DMEx.Handler.Cursors
  alias DMEx.Storage.Postgres
  alias DMEx.Test.Messages.OrderPlaced

  defmodule Old do
    @moduledoc false
    use DMEx.Handler, logs: [:orders], name: :old

    @impl true
    def handle_message(%OrderPlaced{}, _metadata), do: :ok
  end

  defmodule New do
    @moduledoc false
    use DMEx.Handler, logs: [:orders], name: :new

    @impl true
    def handle_message(%OrderPlaced{}, _metadata), do: :ok
  end

  defmodule Both do
    @moduledoc "Two logs, so every call here has to say which cursor it means."
    use DMEx.Handler, logs: [:orders, :returns], name: :both

    @impl true
    def handle_message(%OrderPlaced{}, _metadata), do: :ok
  end

  @old "orders/old"
  @new "orders/new"

  setup do
    %{opts: [repo: Repo], storage_opts: %{repo: Repo, prefix: nil}}
  end

  describe "position/2" do
    test "is nil for a handler that has never committed a position", ctx do
      assert {:ok, nil} = Cursors.position(Old, ctx.opts)
    end

    test "reports the position and when it last moved", ctx do
      :ok = Postgres.advance_cursor(@old, 42, ctx.storage_opts)

      assert {:ok, cursor} = Cursors.position(Old, ctx.opts)
      assert cursor.last_seen == 42
      assert cursor.updated_at
    end
  end

  describe "identity is the declaration, not the module name" do
    # Benefit (b) of keying on `{log, id}`, asserted directly: a module
    # under a different name with the same declaration is the same subscription,
    # so an ordinary rename carries its cursor with it.
    defmodule Renamed do
      @moduledoc false
      use DMEx.Handler, logs: [:orders], name: :old

      @impl true
      def handle_message(%OrderPlaced{}, _metadata), do: :ok
    end

    test "a renamed module reads the same cursor", ctx do
      :ok = Cursors.move!(Old, 4_242, ctx.opts)

      assert {:ok, %{last_seen: 4_242}} = Cursors.position(Renamed, ctx.opts)
    end
  end

  describe "move!/3" do
    test "creates a cursor where there was none", ctx do
      assert :ok = Cursors.move!(Old, 100, ctx.opts)

      assert {:ok, 100} = Postgres.last_seen(@old, ctx.storage_opts)
    end

    test "rewinds an existing one, so the next connect replays", ctx do
      :ok = Postgres.advance_cursor(@old, 100, ctx.storage_opts)

      assert :ok = Cursors.move!(Old, 10, ctx.opts)

      assert {:ok, 10} = Postgres.last_seen(@old, ctx.storage_opts)
    end

    test "moving to 0 replays the whole log but keeps the row", ctx do
      # Which is the difference from reset!/2: a cursor at 0 still exists, so
      # `last_seen:` stays ignored.
      :ok = Postgres.advance_cursor(@old, 100, ctx.storage_opts)

      assert :ok = Cursors.move!(Old, 0, ctx.opts)

      assert {:ok, 0} = Postgres.last_seen(@old, ctx.storage_opts)
    end
  end

  describe "reset!/2" do
    test "removes the row entirely, so last_seen: applies again", ctx do
      :ok = Postgres.advance_cursor(@old, 100, ctx.storage_opts)

      assert :ok = Cursors.reset!(Old, ctx.opts)

      # nil, not 0 — the distinction that makes `last_seen:` apply exactly once.
      assert {:ok, nil} = Postgres.last_seen(@old, ctx.storage_opts)
    end

    test "is fine on a handler that has no cursor", ctx do
      assert :ok = Cursors.reset!(Old, ctx.opts)
    end
  end

  describe "rename!/3" do
    # Not for a module rename — that is free. This is for the migration surface
    # that remains: changing a handler's `name:`, or a log's key in the
    # consumer's map, mints a new identity. Hence the id strings — the old
    # identity is the one no module declares.
    test "carries the position over and removes the old row", ctx do
      :ok = Postgres.advance_cursor(@old, 4_242, ctx.storage_opts)

      assert :ok = Cursors.rename!(@old, @new, ctx.opts)

      assert {:ok, 4_242} = Postgres.last_seen(@new, ctx.storage_opts)
      assert {:ok, nil} = Postgres.last_seen(@old, ctx.storage_opts)
    end

    test "carries the log URL over too", ctx do
      :ok = Postgres.advance_cursor(@old, 10, ctx.storage_opts)

      :ok = Cursors.rename!(@old, @new, ctx.opts)

      assert {:ok, %{last_seen: 10}} = Cursors.position(New, ctx.opts)
    end

    test "refuses to overwrite a cursor the target already has", ctx do
      :ok = Postgres.advance_cursor(@old, 10, ctx.storage_opts)
      :ok = Postgres.advance_cursor(@new, 99, ctx.storage_opts)

      assert_raise ArgumentError, ~r/already has a cursor at 99/, fn ->
        Cursors.rename!(@old, @new, ctx.opts)
      end
    end

    test "leaves both cursors untouched when it refuses", ctx do
      :ok = Postgres.advance_cursor(@old, 10, ctx.storage_opts)
      :ok = Postgres.advance_cursor(@new, 99, ctx.storage_opts)

      catch_error(Cursors.rename!(@old, @new, ctx.opts))

      # One transaction, so a refusal cannot half-apply.
      assert {:ok, 10} = Postgres.last_seen(@old, ctx.storage_opts)
      assert {:ok, 99} = Postgres.last_seen(@new, ctx.storage_opts)
    end

    test "is a no-op when the old handler never had a cursor", ctx do
      assert :ok = Cursors.rename!(@old, @new, ctx.opts)

      assert {:ok, nil} = Postgres.last_seen(@new, ctx.storage_opts)
    end
  end

  # A cursor is one {log, id} pair, so a handler reading several has to name one.
  describe "a handler reading several logs" do
    test "refuses a call that does not say which cursor", ctx do
      assert_raise ArgumentError, ~r/reads 2 logs/, fn -> Cursors.position(Both, ctx.opts) end

      assert_raise ArgumentError, ~r/reads 2 logs/, fn -> Cursors.move!(Both, 0, ctx.opts) end

      assert_raise ArgumentError, ~r/reads 2 logs/, fn -> Cursors.reset!(Both, ctx.opts) end
    end

    test "addresses the cursor `for:` names, and only that one", ctx do
      :ok = Cursors.move!(Both, 7, [for: :returns] ++ ctx.opts)

      assert {:ok, %{last_seen: 7}} = Cursors.position(Both, [for: :returns] ++ ctx.opts)
      assert {:ok, nil} = Cursors.position(Both, [for: :orders] ++ ctx.opts)
    end
  end
end
