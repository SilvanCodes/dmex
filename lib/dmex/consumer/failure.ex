defmodule DMEx.Consumer.Failure do
  @moduledoc """
  What happens to a message once its local retries are exhausted.

  The retry step beforehand is identical in all three modes; only the terminal
  action differs.

  ## `:dead_message` (the default)

  Advances past the failing message, but writes it to `dmex_dead_messages`
  first, **in the same transaction**. That atomicity is the point: a dead
  message and the cursor that skipped past it commit together or not at all, so
  there is no state where a message was skipped without being recorded, or
  recorded without being skipped.

  It is the default because it is the only mode that refuses both bad outcomes.
  One message a handler cannot process is a bug in that handler or a
  malformed payload, not a reason for every *later* message to stop arriving —
  and it is not a reason to lose the failing one either. Recording it and moving
  on keeps the subscription live and keeps the evidence, and
  `DMEx.Consumer.DeadMessages` is how you inspect, replay or discard what
  accumulated.

  **What is stored is the message, and only the message.** The dead-messages
  table is structurally `dmex_messages` plus the subscription id that says whose
  row it is, so a stored row *is* a log row — see `DMEx.Consumer.DeadMessages`,
  where replaying one is feeding it back to a handler with no reshaping in
  between. Equal as a value rather than byte-for-byte: the payload is re-encoded
  from the term the frame decoded.

  The exception, its message and the attempt count are **not** stored. A failure
  is an event, not a property of the message: it goes to the log and to
  `[:dmex, :consumer, :failed]`. Persisting it would make each row a mixture of
  a message and an incident, and stop it being replayable as the former.

  ## `:crash`

  Re-raises, with the original stacktrace. The stream process dies, its
  supervisor restarts it, and on restart it re-reads its persisted cursor —
  still pointing at the message *before* this one, since that transaction never
  committed — so the producer's ordinary range scan re-streams the very message
  that killed it. A permanently-failing handler crash-loops on it forever.

  Choose this when a stuck subscription is the *better* failure: when later
  messages genuinely must not be processed without this one, or when a loud,
  impossible-to-ignore alarm beats a growing table nobody is watching. Reconnect
  backoff inside `DMEx.Consumer.Stream` keeps the loop from hammering the
  producer's database, and `DMEx.Consumer.Supervisor`'s restart budget is set so
  it cannot take its sibling subscriptions down with it.

  ## `:skip`

  Commits an advance past the failing message *without* the handler having
  succeeded, and carries on. The message is simply gone from this consumer's
  perspective.

  DMEx records nothing durable: the failure is logged and emitted as
  `[:dmex, :consumer, :failed]`, and that is the only trace. It is
  `:dead_message` minus the evidence, which is worth being deliberate about.
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
        :ok = state.storage.advance(state.storage_opts, state.subscription, id)
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
            subscription: state.subscription,
            id: id,
            message_type: Map.fetch!(row, "message_type"),
            data: encode(Map.get(row, "data")),
            metadata: encode(Map.get(row, "metadata"))
          })

        :ok = state.storage.advance(state.storage_opts, state.subscription, id)
      end)

    report(state, row, id, exception, attempts, :dead_message)

    {:ok, id}
  end

  # The row arrived through `DMEx.SSE.Frame`, which decodes one level, so `data` and
  # `metadata` are terms here and the storage adapter wants JSON text.
  defp encode(nil), do: "{}"
  defp encode(value), do: JSON.encode!(value)

  defp report(state, row, id, exception, attempts, mode) do
    Logger.error(fn ->
      "DMEx #{state.subscription} (#{inspect(state.handler)}) gave up on message #{id} " <>
        "(#{Map.fetch!(row, "message_type")}) from #{state.url} after #{attempts} " <>
        "attempt(s): #{Exception.message(exception)} — on_failure: #{mode}"
    end)

    :telemetry.execute(
      [:dmex, :consumer, :failed],
      %{id: id, attempts: attempts},
      %{
        subscription: state.subscription,
        handler: state.handler,
        message_type: Map.fetch!(row, "message_type"),
        url: state.url,
        on_failure: mode,
        reason: Exception.message(exception)
      }
    )
  end
end
