defmodule DMEx.Handler.DeadMessages do
  @moduledoc """
  Inspects, replays and discards messages a handler gave up on.

      # what is stuck
      DMEx.Handler.DeadMessages.list(MyApp.OrderProjection, repo: MyApp.Repo)

      # fix the handler, deploy, then
      DMEx.Handler.DeadMessages.replay(MyApp.OrderProjection, repo: MyApp.Repo)

  The stored columns are `dmex_messages`' columns, so a dead row *is* a log row
  and replay hands it straight back with no reshaping — see `DMEx.Storage.Migration`
  for the shape and for what is deliberately not stored.

  ## Replay does not touch the cursor

  The subscription advanced past a dead message long ago, and moving the cursor
  backwards would redeliver everything in between. So `replay/2` calls the
  handler directly and deletes the row on success, in one transaction. It
  follows that replay bypasses duplicate suppression, which consults only the
  cursor — so a handler can be called for a message whose side effects partly
  landed on an earlier attempt.

  ## Options

  `repo:` (required), plus optional `storage:`, `prefix:` and — for a multi-log
  handler — `log:`. See `DMEx.Supervisor.storage!/1`.
  """

  alias DMEx.Handler
  alias DMEx.Message

  require Logger

  @doc """
  Lists every message `handler`'s subscription gave up on, oldest first.

  Rows come back in `t:DMEx.Storage.row/0` shape — `data` and `metadata` as JSON
  text, exactly as stored.
  """
  @spec list(module(), keyword()) :: {:ok, [DMEx.Storage.row()]} | {:error, term()}
  def list(handler, opts) when is_atom(handler) do
    {storage, storage_opts} = DMEx.Supervisor.storage!(opts)

    storage.dead_messages(storage_opts, subscription!(handler, opts))
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
    {storage, storage_opts} = DMEx.Supervisor.storage!(opts)

    declaration = Handler.declaration!(handler)
    modules = Message.registry(declaration.messages)
    subscription = subscription!(handler, opts)

    {:ok, rows} = storage.dead_messages(storage_opts, subscription)

    Enum.reduce(rows, %{replayed: 0, failed: 0}, fn row, counts ->
      case replay_one(row, declaration, subscription, modules, storage, storage_opts) do
        :ok -> Map.update!(counts, :replayed, &(&1 + 1))
        :error -> Map.update!(counts, :failed, &(&1 + 1))
      end
    end)
  end

  @doc """
  Drops one dead message without running it.

  For a message that should never have been appended, or one whose effects were
  applied by hand.
  """
  @spec discard(module(), non_neg_integer(), keyword()) :: :ok
  def discard(handler, id, opts) when is_atom(handler) and is_integer(id) do
    {storage, storage_opts} = DMEx.Supervisor.storage!(opts)

    :ok = storage.delete_dead_message(storage_opts, subscription!(handler, opts), id)
  end

  ## Internals

  defp subscription!(handler, opts) do
    Handler.subscription_id(handler, Handler.log!(handler, opts))
  end

  defp replay_one(row, declaration, subscription, modules, storage, storage_opts) do
    case Map.fetch(modules, row.message_type) do
      :error ->
        Logger.warning(
          "DMEx cannot replay dead message #{row.id}: #{inspect(declaration.handler)} " <>
            "has no clause for #{row.message_type}. Add one to handle_message/2."
        )

        :error

      {:ok, module} ->
        run(row, module, declaration, subscription, storage, storage_opts)
    end
  end

  defp run(row, module, declaration, subscription, storage, storage_opts) do
    message = module.cast(JSON.decode!(row.data))
    metadata = JSON.decode!(row.metadata)

    {:ok, _} =
      storage.transaction(storage_opts, fn ->
        _ = declaration.handler.handle_message(message, metadata)

        :ok = storage.delete_dead_message(storage_opts, subscription, row.id)
      end)

    :ok
  rescue
    exception ->
      Logger.warning(
        "DMEx replay of dead message #{row.id} for #{subscription} failed again: " <>
          Exception.message(exception)
      )

      :error
  end
end
