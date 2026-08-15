defmodule DMEx.Handler.Cursors do
  @moduledoc """
  Reads and moves a subscription's read position.

  DMEx owns `last_seen` and application code never touches it in normal
  operation; these are the operator-facing exceptions.

  ## Changing a declaration costs a migration

  A different `name:`, or a different key for that log in `DMEx.Supervisor`'s
  `bindings:`, mints a new identity with no cursor — `DMEx.Handler` covers what does
  and does not change one. Migrate it in an `Ecto.Migration`'s `up/0`, next to
  the code change that causes it:

      DMEx.Handler.Cursors.rename!("orders/order_handler", "orders/order_projection", repo: MyApp.Repo)

  ## Options

  `repo:` (required), plus optional `storage:`, `prefix:` and — for a multi-log
  handler — `for:`. See `DMEx.Supervisor.storage!/1`.

  ## These do not touch a running subscription

  A live `DMEx.Handler.Subscription` re-reads the durable cursor only on
  connect, so a move takes effect at its next reconnect. Do this while it is
  stopped, or restart it afterwards.
  """

  alias DMEx.Handler
  alias DMEx.Storage

  @doc """
  Returns where `handler`'s subscription has got to, and when it last moved.

  `nil` if it has never committed a position: nothing writes a cursor until the
  subscription has consumed something.
  """
  @spec position(module(), keyword()) ::
          {:ok, %{last_seen: non_neg_integer(), updated_at: term()} | nil}
          | {:error, term()}
  def position(handler, opts) when is_atom(handler) do
    storage = DMEx.Supervisor.storage!(opts)

    Storage.cursor(storage, Handler.resolve_subscription_id!(handler, opts))
  end

  @doc """
  Moves `handler`'s cursor to `last_seen`, so the next connect resumes after it.

  `move!(h, 41)` and `last_seen: 41` name the same exclusive position in the same
  units — `DMEx.Config` covers that vocabulary — differing only in durability.
  `0` replays every message this subscription selects, and is not `reset!/2`: a
  cursor at 0 still exists, so `last_seen:` stays ignored.
  """
  @spec move!(module(), non_neg_integer(), keyword()) :: :ok
  def move!(handler, last_seen, opts) when is_atom(handler) and is_integer(last_seen) and last_seen >= 0 do
    storage = DMEx.Supervisor.storage!(opts)

    # No transaction: one statement is already atomic, and `rename!/3` — which
    # genuinely needs one, for two — is the contrast.
    Storage.advance_cursor(storage, Handler.resolve_subscription_id!(handler, opts), last_seen)
  end

  @doc """
  Removes `handler`'s cursor entirely.

  Distinct from moving it to 0: with no row at all, the subscription's
  `last_seen:` option applies again on its next start.
  """
  @spec reset!(module(), keyword()) :: :ok
  def reset!(handler, opts) when is_atom(handler) do
    storage = DMEx.Supervisor.storage!(opts)

    Storage.delete_cursor(storage, Handler.resolve_subscription_id!(handler, opts))
  end

  @doc """
  Carries the position of subscription `from` over to `to`, removing the old row.

  Identifier strings rather than modules: `from` is by definition the identity
  nothing declares any longer. Both steps run in one transaction, so a rename
  cannot half-apply. Raises if `to` already has a cursor; returns `:ok` when
  `from` has none, since there was nothing to carry.
  """
  @spec rename!(String.t(), String.t(), keyword()) :: :ok
  def rename!(from, to, opts) when is_binary(from) and is_binary(to) do
    storage = DMEx.Supervisor.storage!(opts)

    {:ok, :ok} =
      Storage.transaction(storage, fn ->
        # `from` first, and through `cursor!/2`, so a failed read cannot be
        # mistaken for an answer: reading both at once let a `to` that has a
        # cursor report "already has a cursor" when what actually happened was
        # that `from` could not be read at all.
        with %{last_seen: id} <- cursor!(storage, from),
             nil <- cursor!(storage, to) do
          Storage.advance_cursor(storage, to, id)
          Storage.delete_cursor(storage, from)
        else
          # Only the first read can answer `nil` here, and only the second a
          # cursor: nothing left to move, versus a destination already taken.
          nil ->
            :ok

          %{last_seen: existing} ->
            raise ArgumentError,
                  "#{inspect(to)} already has a cursor at #{existing}. " <>
                    "Renaming #{inspect(from)} onto it would overwrite that " <>
                    "subscription's position — reset one of them first if that is what you want."
        end
      end)

    :ok
  end

  # Raising rather than returning: this runs inside the transaction, so a storage
  # failure rolls the rename back and says which read failed.
  defp cursor!(storage, subscription) do
    case Storage.cursor(storage, subscription) do
      {:ok, cursor} ->
        cursor

      {:error, reason} ->
        raise ArgumentError, "could not read the cursor for #{inspect(subscription)}: #{inspect(reason)}"
    end
  end
end
