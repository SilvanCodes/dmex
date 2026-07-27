defmodule DMEx.Consumer.DeadMessages do
  @moduledoc """
  Inspecting, replaying and discarding messages a handler gave up on.

  `dmex_dead_messages` is **structurally a message log**: the stored columns are
  `dmex_messages`' columns — `id`, `message_type`, `data`, `metadata`,
  `inserted_at` — with the handler and producer URL alongside to say where each
  row belongs. `id` is still the producer's own log id and `inserted_at` is
  still the original publish time.

  That is the whole reason replay is a few lines rather than a subsystem: a dead
  row *is* a log row, so it can be handed straight back to a handler with no
  reshaping, no field lifting, and no second decode path to keep in step with
  the first.

  Nothing about the failure is stored — no exception, no message, no attempt
  count. A failure is an event, not a property of the message; it is in your
  logs and in `[:dmex, :consumer, :failed]`.

  One caveat on "the same row": the payload is equal as a *value*, not
  byte-for-byte. It was decoded out of a frame and re-encoded on the way in, so
  key order and whitespace come from Elixir's encoder rather than from the
  producer's `jsonb` rendering. Nothing should hash or sign a replayed payload
  and expect it to match the original bytes — which is already true of anything
  read back out of the log itself.

      # what is stuck
      DMEx.Consumer.DeadMessages.list(MyApp.OrderHandler, repo: MyApp.Repo)

      # fix the handler, deploy, then
      DMEx.Consumer.DeadMessages.replay(MyApp.OrderHandler,
        repo: MyApp.Repo,
        message_modules: [MyApp.Messages.OrderPlaced]
      )

  ## Replay does not touch the cursor

  A replayed message is delivered out of band: the subscription has long since
  advanced past it, and moving the cursor backwards to re-reach it would
  redeliver everything in between. So `replay/2` calls the handler directly and
  deletes the row on success, in one transaction — the same atomicity the
  original failure was recorded with.

  It follows that replay bypasses duplicate suppression, because the cursor is
  the only thing suppression consults. Replaying twice calls the handler twice.

  ## Options

  `repo:` (required), plus optional `storage:` and `prefix:`, the same as
  `DMEx.Consumer`. `replay/2` additionally needs `message_modules:` — the same
  list the subscription declares, since decoding a row is exactly as
  consumer-declared here as it is on the live path.
  """

  alias DMEx.Consumer
  alias DMEx.Message

  require Logger

  @doc """
  Every message `handler` gave up on, oldest first.

  Rows come back in `t:DMEx.Storage.row/0` shape — `data` and `metadata` as JSON
  text, exactly as stored.
  """
  @spec list(module(), keyword()) :: {:ok, [DMEx.Storage.row()]} | {:error, term()}
  def list(handler, opts) when is_atom(handler) do
    {storage, storage_opts} = Consumer.storage!(opts)

    storage.dead_messages(storage_opts, handler)
  end

  @doc """
  Feeds every dead message back to `handler`, deleting the ones it accepts.

  Each message gets its own transaction containing the handler call and the
  delete, so a message is removed from the table exactly when the handler
  finally accepts it. One that raises again is left in place and does not stop
  the rest.

  Returns `%{replayed: n, failed: n}`.
  """
  @spec replay(module(), keyword()) :: %{replayed: non_neg_integer(), failed: non_neg_integer()}
  def replay(handler, opts) when is_atom(handler) do
    {storage, storage_opts} = Consumer.storage!(opts)
    modules = opts |> Keyword.fetch!(:message_modules) |> Message.registry()

    {:ok, rows} = storage.dead_messages(storage_opts, handler)

    Enum.reduce(rows, %{replayed: 0, failed: 0}, fn row, counts ->
      case replay_one(row, handler, modules, storage, storage_opts) do
        :ok -> Map.update!(counts, :replayed, &(&1 + 1))
        :error -> Map.update!(counts, :failed, &(&1 + 1))
      end
    end)
  end

  @doc """
  Drops one dead message without running it.

  For a message that should never have been published, or one whose effects were
  applied by hand.
  """
  @spec discard(module(), non_neg_integer(), keyword()) :: :ok
  def discard(handler, id, opts) when is_atom(handler) and is_integer(id) do
    {storage, storage_opts} = Consumer.storage!(opts)

    :ok = storage.delete_dead_message(storage_opts, handler, id)
  end

  ## Internals

  defp replay_one(row, handler, modules, storage, storage_opts) do
    case Map.fetch(modules, row.message_type) do
      :error ->
        Logger.warning(
          "DMEx cannot replay dead message #{row.id}: no module declared for " <>
            "#{row.message_type}. Add it to :message_modules."
        )

        :error

      {:ok, module} ->
        run(row, module, handler, storage, storage_opts)
    end
  end

  defp run(row, module, handler, storage, storage_opts) do
    # Stored as text, decoded here, exactly as the live path does it.
    message = module.cast(JSON.decode!(row.data))
    metadata = JSON.decode!(row.metadata)

    {:ok, _} =
      storage.transaction(storage_opts, fn ->
        _ = handler.handle_message(message, metadata)

        :ok = storage.delete_dead_message(storage_opts, handler, row.id)
      end)

    :ok
  rescue
    exception ->
      Logger.warning(
        "DMEx replay of dead message #{row.id} for #{inspect(handler)} failed again: " <>
          Exception.message(exception)
      )

      :error
  end
end
