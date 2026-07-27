defmodule DMEx.Consumer.Failure do
  @moduledoc """
  What happens to a message once its local retries are exhausted.

  The retry step beforehand is identical in all three modes; only the terminal
  action differs.

  ## `:crash` (the default)

  Re-raises, with the original stacktrace. The stream process dies, its
  supervisor restarts it, and on restart it re-reads its persisted cursor —
  still pointing at the message *before* this one, since that transaction never
  committed — so the producer's ordinary range scan re-streams the very message
  that killed it.

  A permanently-failing handler therefore crash-loops on the same message
  forever rather than silently dropping it. That is the intended behaviour: the
  alternative is losing messages quietly. Reconnect backoff inside
  `DMEx.Consumer.Stream` is what keeps that loop from hammering the producer's
  database, and `DMEx.Consumer.Supervisor`'s restart budget is set so the loop
  cannot take its sibling subscriptions down with it.

  ## `:skip`

  Commits an advance past the failing message *without* the handler having
  succeeded, and carries on. The message is simply gone from this consumer's
  perspective.

  DMEx records nothing durable: the failure is logged and emitted as
  `[:dmex, :consumer, :failed]`, and that is the only trace. Worth knowing
  before choosing this mode.

  ## `:dead_message`

  Same advance, but the message is first written to `dmex_dead_messages` **in
  the same transaction**. That atomicity is the point: a dead message and the
  cursor that skipped past it commit together or not at all, so there is no
  state where a message was skipped without being recorded, or recorded without
  being skipped.

  **What is stored is the message, and only the message.** The dead-messages
  table is structurally `dmex_messages` plus the handler and producer URL that
  say where the row belongs, so a stored row *is* a log row — see
  `DMEx.Consumer.DeadMessages`, where replaying one is feeding it back to a
  handler with no reshaping in between. Equal as a value rather than
  byte-for-byte: the payload is re-encoded from the term the frame decoded.

  The exception, its message and the attempt count are **not** stored. A failure
  is an event, not a property of the message: it goes to the log and to
  `[:dmex, :consumer, :failed]`. Persisting it would make each row a mixture of
  a message and an incident, and stop it being replayable as the former.
  """

  alias DMEx.Consumer.Pipeline

  require Logger

  @doc """
  Resolves a message that has exhausted its retries.

  Returns the new cursor for `:skip` and `:dead_message`; re-raises for
  `:crash`.
  """
  @spec resolve(
          Pipeline.t(),
          map(),
          pos_integer(),
          Exception.t(),
          Exception.stacktrace(),
          pos_integer()
        ) ::
          {:ok, pos_integer()}
  def resolve(state, row, id, exception, stacktrace, attempts) do
    case state.config.on_failure do
      :crash -> reraise(exception, stacktrace)
      :skip -> skip(state, row, id, exception, attempts)
      :dead_message -> dead_message(state, row, id, exception, attempts)
    end
  end

  defp skip(state, row, id, exception, attempts) do
    {:ok, _} =
      state.storage.transaction(state.storage_opts, fn ->
        :ok = state.storage.advance(state.storage_opts, state.handler, id, state.url)
      end)

    report(state, row, id, exception, attempts, :skip)

    {:ok, id}
  end

  defp dead_message(state, row, id, exception, attempts) do
    {:ok, _} =
      state.storage.transaction(state.storage_opts, fn ->
        # Both inside one transaction, deliberately: a dead message and the
        # cursor that moved past it are the same fact.
        :ok =
          state.storage.dead_message(state.storage_opts, %{
            handler: state.handler,
            producer_url: state.url,
            id: id,
            message_type: Map.fetch!(row, "message_type"),
            data: encode(Map.get(row, "data")),
            metadata: encode(Map.get(row, "metadata")),
            inserted_at: Map.fetch!(row, "inserted_at")
          })

        :ok = state.storage.advance(state.storage_opts, state.handler, id, state.url)
      end)

    report(state, row, id, exception, attempts, :dead_message)

    {:ok, id}
  end

  # The row arrived through `DMEx.Frame`, which decodes one level, so `data` and
  # `metadata` are terms here and the storage adapter wants JSON text.
  defp encode(nil), do: "{}"
  defp encode(value), do: JSON.encode!(value)

  defp report(state, row, id, exception, attempts, mode) do
    Logger.error(fn ->
      "DMEx #{inspect(state.handler)} gave up on message #{id} " <>
        "(#{Map.fetch!(row, "message_type")}) from #{state.url} after #{attempts} " <>
        "attempt(s): #{Exception.message(exception)} — on_failure: #{mode}"
    end)

    :telemetry.execute(
      [:dmex, :consumer, :failed],
      %{id: id, attempts: attempts},
      %{
        handler: state.handler,
        message_type: Map.fetch!(row, "message_type"),
        url: state.url,
        on_failure: mode,
        reason: Exception.message(exception)
      }
    )
  end
end
