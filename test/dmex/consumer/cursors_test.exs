defmodule DMEx.Consumer.CursorsTest do
  use DMEx.Case, async: true, database: true

  alias DMEx.Consumer.Cursors
  alias DMEx.Storage.Postgres

  @url "https://orders.internal/stream"

  defmodule Old do
    @moduledoc false
    use DMEx.Consumer.Handler

    @impl true
    def handle_message(_message, _metadata), do: :ok
  end

  defmodule New do
    @moduledoc false
    use DMEx.Consumer.Handler

    @impl true
    def handle_message(_message, _metadata), do: :ok
  end

  setup do
    %{opts: [repo: Repo], storage_opts: %{repo: Repo, prefix: nil}}
  end

  describe "inspect/2" do
    test "is nil for a handler that has never committed a position", ctx do
      assert {:ok, nil} = Cursors.inspect(Old, ctx.opts)
    end

    test "reports the position and the producer it read from", ctx do
      :ok = Postgres.advance(ctx.storage_opts, Old, 42, @url)

      assert {:ok, cursor} = Cursors.inspect(Old, ctx.opts)
      assert cursor.last_seen == 42
      assert cursor.producer_url == @url
      assert cursor.updated_at
    end
  end

  describe "move!/4" do
    test "creates a cursor where there was none", ctx do
      assert :ok = Cursors.move!(Old, 100, @url, ctx.opts)

      assert {:ok, 100} = Postgres.last_seen(ctx.storage_opts, Old)
    end

    test "rewinds an existing one, so the next connect replays", ctx do
      :ok = Postgres.advance(ctx.storage_opts, Old, 100, @url)

      assert :ok = Cursors.move!(Old, 10, @url, ctx.opts)

      assert {:ok, 10} = Postgres.last_seen(ctx.storage_opts, Old)
    end

    test "moving to 0 replays the whole log but keeps the row", ctx do
      # Which is the difference from reset!/2: a cursor at 0 still exists, so
      # `start:` stays ignored.
      :ok = Postgres.advance(ctx.storage_opts, Old, 100, @url)

      assert :ok = Cursors.move!(Old, 0, @url, ctx.opts)

      assert {:ok, 0} = Postgres.last_seen(ctx.storage_opts, Old)
    end
  end

  describe "reset!/2" do
    test "removes the row entirely, so start: applies again", ctx do
      :ok = Postgres.advance(ctx.storage_opts, Old, 100, @url)

      assert :ok = Cursors.reset!(Old, ctx.opts)

      # nil, not 0 — the distinction that makes `start: :latest` meaningful.
      assert {:ok, nil} = Postgres.last_seen(ctx.storage_opts, Old)
    end

    test "is fine on a handler that has no cursor", ctx do
      assert :ok = Cursors.reset!(Old, ctx.opts)
    end
  end

  describe "rename!/3" do
    # The foot-gun this module exists for: without it, a renamed handler is a
    # brand-new subscription that replays the producer's log from the start.
    test "carries the position over and removes the old row", ctx do
      :ok = Postgres.advance(ctx.storage_opts, Old, 4_242, @url)

      assert :ok = Cursors.rename!(Old, New, ctx.opts)

      assert {:ok, 4_242} = Postgres.last_seen(ctx.storage_opts, New)
      assert {:ok, nil} = Postgres.last_seen(ctx.storage_opts, Old)
    end

    test "carries the producer URL over too", ctx do
      :ok = Postgres.advance(ctx.storage_opts, Old, 10, @url)

      :ok = Cursors.rename!(Old, New, ctx.opts)

      assert {:ok, %{producer_url: @url}} = Cursors.inspect(New, ctx.opts)
    end

    test "refuses to overwrite a cursor the target already has", ctx do
      :ok = Postgres.advance(ctx.storage_opts, Old, 10, @url)
      :ok = Postgres.advance(ctx.storage_opts, New, 99, @url)

      assert_raise ArgumentError, ~r/already has a cursor at 99/, fn ->
        Cursors.rename!(Old, New, ctx.opts)
      end
    end

    test "leaves both cursors untouched when it refuses", ctx do
      :ok = Postgres.advance(ctx.storage_opts, Old, 10, @url)
      :ok = Postgres.advance(ctx.storage_opts, New, 99, @url)

      catch_error(Cursors.rename!(Old, New, ctx.opts))

      # One transaction, so a refusal cannot half-apply.
      assert {:ok, 10} = Postgres.last_seen(ctx.storage_opts, Old)
      assert {:ok, 99} = Postgres.last_seen(ctx.storage_opts, New)
    end

    test "is a no-op when the old handler never had a cursor", ctx do
      assert :ok = Cursors.rename!(Old, New, ctx.opts)

      assert {:ok, nil} = Postgres.last_seen(ctx.storage_opts, New)
    end
  end
end
