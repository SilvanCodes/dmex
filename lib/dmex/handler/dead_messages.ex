defmodule DMEx.Handler.DeadMessages do
  @moduledoc """
  Inspects, replays and discards messages a handler gave up on.

      # what is stuck
      DMEx.Handler.DeadMessages.list(MyApp.OrderProjection, repo: MyApp.Repo)

      # fix the handler, deploy, then
      DMEx.Handler.DeadMessages.replay!(MyApp.OrderProjection, repo: MyApp.Repo)

  The stored columns are `dmex_messages`' columns, so a dead row *is* a log row:
  replay hands the same message back rather than rebuilding one from parts. It
  does have to restate it, though — a stored row is `t:DMEx.Storage.row/0`
  (atom keys, JSON text, as a driver returns it) while the pipeline consumes
  `t:DMEx.Handler.Pipeline.row/0` (string keys, decoded one level, as a
  transport yields it). See `c:DMEx.Storage.dead_message/2` for the shape and
  for what is deliberately not stored.

  ## Replay is the pipeline, minus `on_failure`

  Everything a delivery does to a message — the retry schedule, the
  `[:dmex, :handler, :message]` span, the rule that storage failing is not the
  handler failing — is `DMEx.Handler.Pipeline.deliver/3`, and replay calls it.
  A message dead-lettered after `max_attempts` transient failures therefore gets
  `max_attempts` again here, which matters most on the one operation whose
  purpose is recovery.

  The one section it does not inherit is section 6, `on_failure`: dead-lettering
  a message that is already in `dmex_dead_messages`, or crashing a console
  command, resolves nothing. A replay that fails leaves the row where it was and
  is counted, which is the terminal action for this caller.

  ## Replay does not touch the cursor

  The subscription advanced past a dead message long ago, and moving the cursor
  backwards would redeliver everything in between. So `replay!/2` calls the
  handler directly and deletes the row on success, in one transaction. It
  follows that replay bypasses duplicate suppression, which consults only the
  cursor — so a handler can be called for a message whose side effects partly
  landed on an earlier attempt.

  ## Options

  `repo:` (required), plus optional `storage:`, `prefix:` and — for a multi-log
  handler — `for:`. See `DMEx.Supervisor.storage!/1`.
  """

  alias DMEx.Config
  alias DMEx.Handler
  alias DMEx.Handler.Pipeline

  require Logger

  # Handler telemetry carries where the rows came from, and these did not come
  # off a stream.
  @origin :dead_messages

  @doc """
  Lists every message `handler`'s subscription gave up on, oldest first.

  Rows come back in `t:DMEx.Storage.row/0` shape — `data` and `metadata` as JSON
  text, exactly as stored.

  Returns storage's `{:error, reason}` rather than raising, because a listing is
  something a caller inspects; `replay!/2` raises `DMEx.Storage.Error` on the same
  failure, having no tagged return to carry it and no business reporting an
  unreachable database as a count.
  """
  @spec list(module(), keyword()) :: {:ok, [DMEx.Storage.row()]} | {:error, term()}
  def list(handler, opts) when is_atom(handler) do
    {storage, storage_opts} = DMEx.Supervisor.storage!(opts)

    storage.list_dead_messages(storage_opts, Handler.subscription_id!(handler, opts))
  end

  @doc """
  Feeds every dead message back to `handler`, deleting the ones it accepts.

  Each message gets its own transaction containing the handler call and the
  delete, so a message is removed from the table exactly when the handler
  finally accepts it. One that raises through its retries is left in place and
  does not stop the rest.

  Takes the handler's own `use DMEx.Handler` config, so replay retries on the
  schedule that subscription is configured for; any of `DMEx.Config`'s handler
  keys may be overridden here for this one run.

  Returns `%{replayed: n, failed: n}`, where `failed` also counts a row no
  clause matches — it is a row still in the table, and the log line says what to
  do about it. Raises `DMEx.Storage.Error` if storage itself fails, which is not
  a message a handler rejected and not a count.
  """
  @spec replay!(module(), keyword()) :: %{replayed: non_neg_integer(), failed: non_neg_integer()}
  def replay!(handler, opts) when is_atom(handler) do
    {storage, storage_opts} = DMEx.Supervisor.storage!(opts)

    subscription = Handler.subscription_id!(handler, opts)

    state =
      Pipeline.build(
        storage: storage,
        storage_opts: storage_opts,
        handler: handler,
        subscription: subscription,
        origin: @origin,
        config: Config.handler!(Keyword.merge(Handler.declaration!(handler).config, opts))
      )

    subscription
    |> dead_messages!(storage, storage_opts)
    |> Enum.reduce(%{replayed: 0, failed: 0}, fn row, counts ->
      case replay_one(row, state, subscription) do
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
  @spec discard!(module(), non_neg_integer(), keyword()) :: :ok
  def discard!(handler, id, opts) when is_atom(handler) and is_integer(id) do
    {storage, storage_opts} = DMEx.Supervisor.storage!(opts)

    :ok = storage.delete_dead_message(storage_opts, Handler.subscription_id!(handler, opts), id)
  end

  ## Internals

  # A `DMEx.Storage.Error` rather than a failed match: an unreachable database is
  # not a verdict on any message, and the whole run has to stop rather than
  # report zero of them.
  defp dead_messages!(subscription, storage, storage_opts) do
    case storage.list_dead_messages(storage_opts, subscription) do
      {:ok, rows} -> rows
      {:error, reason} -> raise DMEx.Storage.Error, reason
    end
  end

  # The delete is what `deliver/3` commits inside the handler's transaction,
  # where a stream commits the cursor advance — so a row leaves the table
  # exactly when the handler accepts it. Nothing else here is replay's own:
  # retries, telemetry, and letting `DMEx.Storage.Error` through are the
  # pipeline's, and `on_failure` is the pipeline's and deliberately not applied
  # (see the module documentation).
  defp replay_one(row, state, subscription) do
    commit = fn ->
      :ok = state.storage.delete_dead_message(state.storage_opts, subscription, row.id)
    end

    case Pipeline.deliver(state, pipeline_row(row), commit) do
      {:handled, _attempts} ->
        :ok

      # Already reported as `[:dmex, :handler, :skip]`, but a stream skipping a
      # row it does not want is routine where a replay that cannot decode one is
      # a stuck row an operator has to act on.
      :skipped ->
        Logger.warning(
          "DMEx cannot replay dead message #{row.id}: no #{inspect(state.handler)} clause " <>
            "matches a #{row.type}. Widen one and replay again, or `discard!/3` it."
        )

        :error

      {:failed, exception, _stacktrace, attempts} ->
        Logger.warning(
          "DMEx replay of dead message #{row.id} for #{subscription} failed again after " <>
            "#{attempts} attempt(s): #{Exception.message(exception)}"
        )

        :error
    end
  end

  # Same columns, different shape: a stored row is what a driver returned, a
  # pipeline row is what a transport yields.
  defp pipeline_row(row) do
    %{
      "id" => row.id,
      "type" => row.type,
      "data" => JSON.decode!(row.data),
      "metadata" => JSON.decode!(row.metadata)
    }
  end
end
