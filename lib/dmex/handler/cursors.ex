defmodule DMEx.Handler.Cursors do
  @moduledoc """
  Reads and moves a subscription's read position.

  DMEx owns `last_seen` and application code never touches it in normal
  operation. These are the operator-facing exceptions: inspecting where a
  subscription has got to, replaying part of a log, and migrating a cursor when
  a subscription's declared identity changes.

  ## Changing a declaration costs a migration

  A different `id:`, or a different key for that log in `DMEx.Supervisor`'s
  `logs:`, mints a new identity with no cursor, which then begins wherever its
  `last_seen:` says. (Renaming the module does not — see `DMEx.Handler`.) Migrate it
  next to the code change that causes it:

      defmodule MyApp.Repo.Migrations.RenameOrderProjection do
        use Ecto.Migration

        def up do
          DMEx.Handler.Cursors.rename!(
            "orders/order_handler",
            "orders/order_projection",
            repo: MyApp.Repo
          )
        end
      end

  ## Options

  `repo:` (required), plus optional `storage:`, `prefix:` and — for a multi-log
  handler — `log:`. See `DMEx.Supervisor.storage!/1`.

  ## These do not touch a running subscription

  A live `DMEx.Handler.Stream` holds its cursor in memory and re-reads the
  durable one only on connect, so a move takes effect at its next reconnect. Do
  this while it is stopped, or restart it afterwards.
  """

  alias DMEx.Handler

  @doc """
  Returns where `handler`'s subscription has got to, and when it last moved.

  `nil` if it has never committed a position — the normal state for a
  subscription started with the default `last_seen:` on a quiet log, since
  nothing writes a cursor until it has consumed something.
  """
  @spec position(module(), keyword()) ::
          {:ok, %{last_seen: non_neg_integer(), updated_at: term()} | nil}
          | {:error, term()}
  def position(handler, opts) when is_atom(handler) do
    {storage, storage_opts} = DMEx.Supervisor.storage!(opts)

    storage.cursor(storage_opts, Handler.subscription_id!(handler, opts))
  end

  @doc """
  Moves `handler`'s cursor to `last_seen`, so the next connect resumes after it.

  A cursor is **exclusive**, which is exactly what the `last_seen:` option is —
  one domain, one unit, one name, so `move!(h, 41)` and `last_seen: 41` name the
  same place. The difference is only durability: this writes the row, the option
  stands in for one until there is a row.

  `0` replays the whole log — not the same as `reset!/2`, since a cursor at 0
  still exists and `last_seen:` stays ignored. Every message after that position
  which this subscription selects is redelivered, so the handler must tolerate
  it.

  A cursor is nothing but a subscription and a position, so this runs from a
  migration or a console with no handler running.
  """
  @spec move!(module(), non_neg_integer(), keyword()) :: :ok
  def move!(handler, last_seen, opts) when is_atom(handler) and is_integer(last_seen) and last_seen >= 0 do
    {storage, storage_opts} = DMEx.Supervisor.storage!(opts)
    subscription = Handler.subscription_id!(handler, opts)

    # No transaction: one statement is already atomic, and `rename!/3` — which
    # genuinely needs one, for two — is the contrast.
    :ok = storage.advance(storage_opts, subscription, last_seen)
  end

  @doc """
  Removes `handler`'s cursor entirely.

  Distinct from moving it to 0: with no row at all, the subscription's
  `last_seen:` option applies again on its next start. That is the difference between "replay
  everything" and "begin however this subscription was configured to begin".
  """
  @spec reset!(module(), keyword()) :: :ok
  def reset!(handler, opts) when is_atom(handler) do
    {storage, storage_opts} = DMEx.Supervisor.storage!(opts)

    :ok = storage.delete_cursor(storage_opts, Handler.subscription_id!(handler, opts))
  end

  @doc """
  Carries the position of subscription `from` over to `to`, removing the old row.

  Both are identifier strings, since `from` is by definition the identity
  nothing declares any longer. Both steps happen in one transaction, so a rename
  cannot half-apply and leave two cursors — or none. Raises if `to` already has
  a cursor.

  Returns `:ok` even when `from` has no cursor: there was nothing to carry, and
  the rename is still correctly applied.
  """
  @spec rename!(String.t(), String.t(), keyword()) :: :ok
  def rename!(from, to, opts) when is_binary(from) and is_binary(to) do
    {storage, storage_opts} = DMEx.Supervisor.storage!(opts)

    {:ok, :ok} =
      storage.transaction(storage_opts, fn ->
        case {storage.cursor(storage_opts, from), storage.cursor(storage_opts, to)} do
          {{:ok, nil}, _} ->
            :ok

          {_, {:ok, %{last_seen: existing}}} ->
            raise ArgumentError,
                  "#{inspect(to)} already has a cursor at #{existing}. " <>
                    "Renaming #{inspect(from)} onto it would overwrite that " <>
                    "subscription's position — reset one of them first if that is what you want."

          {{:ok, %{last_seen: id}}, {:ok, nil}} ->
            :ok = storage.advance(storage_opts, to, id)
            :ok = storage.delete_cursor(storage_opts, from)
        end
      end)

    :ok
  end
end
