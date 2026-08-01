defmodule DMEx.Handler.DeadMessages do
  @moduledoc """
  Inspecting, replaying and discarding messages a handler gave up on.

  `dmex_dead_messages` is **structurally a message log**: the stored columns are
  `dmex_messages`' columns — `id`, `message_type`, `data`, `metadata`,
  `inserted_at` — with the subscription id alongside to say whose row it is.
  `id` is still the log's own id.

  One column means something different here: `inserted_at` is when the row
  landed in *this* table, not when the message was appended. That is what makes
  the table prunable — "abandoned more than 30 days ago" is a question append
  time cannot answer — and nothing downstream notices, since a handler is given
  its struct and the publisher's metadata, never the row's timestamps.

  That is the whole reason replay is a few lines rather than a subsystem: a dead
  row *is* a log row, so it can be handed straight back to a handler with no
  reshaping, no field lifting, and no second decode path to keep in step with
  the first.

  Nothing about the failure is stored — no exception, no message, no attempt
  count. A failure is an event, not a property of the message; it is in your
  logs and in `[:dmex, :handler, :failed]`.

  One caveat on "the same row": the payload is equal as a *value*, not
  byte-for-byte. It was decoded out of a frame and re-encoded on the way in, so
  key order and whitespace come from Elixir's encoder rather than from the
  log's `jsonb` rendering. Nothing should hash or sign a replayed payload
  and expect it to match the original bytes — which is already true of anything
  read back out of the log itself.

      # what is stuck
      DMEx.Handler.DeadMessages.list(MyApp.OrderProjection, repo: MyApp.Repo)

      # fix the handler, deploy, then
      DMEx.Handler.DeadMessages.replay(MyApp.OrderProjection, repo: MyApp.Repo)

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
  `DMEx.Supervisor`. Nothing else: which messages to decode and which subscription
  a row belongs to both come off the handler module, so there is no second place
  to keep them in step with the live path.
  """

  alias DMEx.Handler
  alias DMEx.Message

  require Logger

  @doc """
  Every message `handler`'s subscription gave up on, oldest first.

  Rows come back in `t:DMEx.Storage.row/0` shape — `data` and `metadata` as JSON
  text, exactly as stored.
  """
  @spec list(module(), keyword()) :: {:ok, [DMEx.Storage.row()]} | {:error, term()}
  def list(handler, opts) when is_atom(handler) do
    {storage, storage_opts} = DMEx.Supervisor.storage!(opts)

    storage.dead_messages(storage_opts, Handler.subscription_id(handler))
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

    declaration = declaration!(handler)
    modules = Message.registry(declaration.messages)

    {:ok, rows} = storage.dead_messages(storage_opts, declaration.id)

    Enum.reduce(rows, %{replayed: 0, failed: 0}, fn row, counts ->
      case replay_one(row, declaration, modules, storage, storage_opts) do
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

    :ok = storage.delete_dead_message(storage_opts, Handler.subscription_id(handler), id)
  end

  ## Internals

  defp declaration!(handler) do
    unless Handler.handler?(handler) do
      raise ArgumentError,
            "#{inspect(handler)} is not a DMEx handler. " <>
              "Add `use DMEx.Handler` to it."
    end

    handler.__dmex__()
  end

  defp replay_one(row, declaration, modules, storage, storage_opts) do
    case Map.fetch(modules, row.message_type) do
      :error ->
        Logger.warning(
          "DMEx cannot replay dead message #{row.id}: #{inspect(declaration.handler)} " <>
            "has no clause for #{row.message_type}. Add one to handle_message/2."
        )

        :error

      {:ok, module} ->
        run(row, module, declaration, storage, storage_opts)
    end
  end

  defp run(row, module, declaration, storage, storage_opts) do
    # Stored as text, decoded here, exactly as the live path does it.
    message = module.cast(JSON.decode!(row.data))
    metadata = JSON.decode!(row.metadata)

    {:ok, _} =
      storage.transaction(storage_opts, fn ->
        _ = declaration.handler.handle_message(message, metadata)

        :ok = storage.delete_dead_message(storage_opts, declaration.id, row.id)
      end)

    :ok
  rescue
    exception ->
      Logger.warning(
        "DMEx replay of dead message #{row.id} for #{declaration.id} failed again: " <>
          Exception.message(exception)
      )

      :error
  end
end
