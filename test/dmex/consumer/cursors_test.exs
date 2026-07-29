defmodule DMEx.Consumer.CursorsTest do
  use DMEx.Case, async: true, database: true

  alias DMEx.Consumer.Cursors
  alias DMEx.Storage.Postgres

  defmodule Old do
    @moduledoc false
    use DMEx.Consumer.Handler, producer: :orders, id: :old

    @impl true
    def handle_message(%DMEx.Test.Messages.OrderPlaced{}, _metadata), do: :ok
  end

  defmodule New do
    @moduledoc false
    use DMEx.Consumer.Handler, producer: :orders, id: :new

    @impl true
    def handle_message(%DMEx.Test.Messages.OrderPlaced{}, _metadata), do: :ok
  end

  @old "orders/old"
  @new "orders/new"

  setup do
    %{opts: [repo: Repo], storage_opts: %{repo: Repo, prefix: nil}}
  end

  describe "inspect/2" do
    test "is nil for a handler that has never committed a position", ctx do
      assert {:ok, nil} = Cursors.inspect(Old, ctx.opts)
    end

    test "reports the position and when it last moved", ctx do
      :ok = Postgres.advance(ctx.storage_opts, @old, 42)

      assert {:ok, cursor} = Cursors.inspect(Old, ctx.opts)
      assert cursor.last_seen == 42
      assert cursor.updated_at
    end
  end

  describe "identity is the declaration, not the module name" do
    # Benefit (b) of keying on `{producer, id}`, asserted directly: a module
    # under a different name with the same declaration is the same subscription,
    # so an ordinary rename carries its cursor with it.
    defmodule Renamed do
      @moduledoc false
      use DMEx.Consumer.Handler, producer: :orders, id: :old

      @impl true
      def handle_message(%DMEx.Test.Messages.OrderPlaced{}, _metadata), do: :ok
    end

    test "a renamed module reads the same cursor", ctx do
      :ok = Cursors.move!(Old, 4_242, ctx.opts)

      assert {:ok, %{last_seen: 4_242}} = Cursors.inspect(Renamed, ctx.opts)
    end
  end

  describe "move!/3" do
    test "creates a cursor where there was none", ctx do
      assert :ok = Cursors.move!(Old, 100, ctx.opts)

      assert {:ok, 100} = Postgres.last_seen(ctx.storage_opts, @old)
    end

    test "rewinds an existing one, so the next connect replays", ctx do
      :ok = Postgres.advance(ctx.storage_opts, @old, 100)

      assert :ok = Cursors.move!(Old, 10, ctx.opts)

      assert {:ok, 10} = Postgres.last_seen(ctx.storage_opts, @old)
    end

    test "moving to 0 replays the whole log but keeps the row", ctx do
      # Which is the difference from reset!/2: a cursor at 0 still exists, so
      # `start:` stays ignored.
      :ok = Postgres.advance(ctx.storage_opts, @old, 100)

      assert :ok = Cursors.move!(Old, 0, ctx.opts)

      assert {:ok, 0} = Postgres.last_seen(ctx.storage_opts, @old)
    end
  end

  describe "reset!/2" do
    test "removes the row entirely, so start: applies again", ctx do
      :ok = Postgres.advance(ctx.storage_opts, @old, 100)

      assert :ok = Cursors.reset!(Old, ctx.opts)

      # nil, not 0 — the distinction that makes `start: :latest` meaningful.
      assert {:ok, nil} = Postgres.last_seen(ctx.storage_opts, @old)
    end

    test "is fine on a handler that has no cursor", ctx do
      assert :ok = Cursors.reset!(Old, ctx.opts)
    end
  end

  describe "rename!/3" do
    # Not for a module rename — that is free. This is for the migration surface
    # that remains: changing a handler's `id:`, or a producer's key in the
    # consumer's map, mints a new identity. Hence the id strings — the old
    # identity is the one no module declares.
    test "carries the position over and removes the old row", ctx do
      :ok = Postgres.advance(ctx.storage_opts, @old, 4_242)

      assert :ok = Cursors.rename!(@old, @new, ctx.opts)

      assert {:ok, 4_242} = Postgres.last_seen(ctx.storage_opts, @new)
      assert {:ok, nil} = Postgres.last_seen(ctx.storage_opts, @old)
    end

    test "carries the producer URL over too", ctx do
      :ok = Postgres.advance(ctx.storage_opts, @old, 10)

      :ok = Cursors.rename!(@old, @new, ctx.opts)

      assert {:ok, %{last_seen: 10}} = Cursors.inspect(New, ctx.opts)
    end

    test "refuses to overwrite a cursor the target already has", ctx do
      :ok = Postgres.advance(ctx.storage_opts, @old, 10)
      :ok = Postgres.advance(ctx.storage_opts, @new, 99)

      assert_raise ArgumentError, ~r/already has a cursor at 99/, fn ->
        Cursors.rename!(@old, @new, ctx.opts)
      end
    end

    test "leaves both cursors untouched when it refuses", ctx do
      :ok = Postgres.advance(ctx.storage_opts, @old, 10)
      :ok = Postgres.advance(ctx.storage_opts, @new, 99)

      catch_error(Cursors.rename!(@old, @new, ctx.opts))

      # One transaction, so a refusal cannot half-apply.
      assert {:ok, 10} = Postgres.last_seen(ctx.storage_opts, @old)
      assert {:ok, 99} = Postgres.last_seen(ctx.storage_opts, @new)
    end

    test "is a no-op when the old handler never had a cursor", ctx do
      assert :ok = Cursors.rename!(@old, @new, ctx.opts)

      assert {:ok, nil} = Postgres.last_seen(ctx.storage_opts, @new)
    end
  end
end
