defmodule DMEx.Handler.DeadMessages do
  @moduledoc """
  Inspects, replays and discards messages a handler gave up on.

      # what is stuck
      DMEx.Handler.DeadMessages.list(MyApp.OrderProjection, repo: MyApp.Repo)

      # fix the handler, deploy, then
      DMEx.Handler.DeadMessages.replay(MyApp.OrderProjection, repo: MyApp.Repo)

  `dmex_dead_messages` is **structurally a message log**: the stored columns are
  `dmex_messages`' columns, with the subscription id alongside to say whose row
  it is. A dead row *is* a log row, so replay hands it straight back to a
  handler with no reshaping and no second decode path — which is why replay is a
  few lines rather than a subsystem.

  One column differs: `inserted_at` is when the row landed in *this* table, not
  when the message was appended. That is what makes the table prunable, and
  nothing downstream notices, since a handler is given its struct and the
  publisher's metadata, never the row's timestamps.

  Nothing about the failure is stored — no exception, no attempt count. A
  failure is an event, not a property of the message; it lives in your logs and
  in `[:dmex, :handler, :failed]`.

  The payload is equal as a *value*, not byte-for-byte: it was decoded out of a
  frame and re-encoded, so key order and whitespace come from Elixir's encoder.
  The same caveat already applies to anything read back out of the log.

  ## Replay does not touch the cursor

  The subscription advanced past a dead message long ago, and moving the cursor
  backwards would redeliver everything in between. So `replay/2` calls the
  handler directly and deletes the row on success, in one transaction. It
  follows that replay bypasses duplicate suppression, which consults only the
  cursor: replaying twice calls the handler twice.

  ## Options

  `repo:` (required), plus optional `storage:` and `prefix:`, and `log:` for a
  handler reading more than one log. Nothing else: which messages to decode and
  which subscription a row belongs to both come off the handler module.
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

  # A dead message belongs to one subscription, so a multi-log handler has to
  # say which. `DMEx.Handler.log!/2` raises when it did not.
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
    # Stored as text, decoded here, exactly as the live path does it.
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
