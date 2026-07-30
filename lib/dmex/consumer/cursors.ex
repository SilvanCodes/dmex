defmodule DMEx.Consumer.Cursors do
  @moduledoc """
  Reading and moving a subscription's read position, deliberately.

  DMEx owns `last_seen` and application code never touches it during normal
  operation. These are the operator-facing exceptions: inspecting where a
  subscription has got to, deliberately replaying part of a log, and — the one
  that actually bites — migrating a cursor when a subscription's declared
  identity changes.

  ## Renaming the handler module is free; changing its declaration is not

  `last_seen` is keyed by the identifier a handler declares —
  `"orders/order_projection"` for a module with `producer: :orders,
  id: :order_projection` — not by the module's name. So renaming
  `MyApp.OrderHandler` to `MyApp.Orders.Handler` costs nothing at all, which is
  the point: a rename is something a refactoring tool does without asking.

  What does still reset a cursor is changing the *declaration*: a different
  `id:`, or a different key for that producer in `DMEx.Consumer`'s `producer:`
  map. Both are deliberate acts rather than incidental ones, but the effect is
  the same — a new identity with no cursor, which then replays the producer's
  log from the beginning. Do it in a migration, next to the code change that
  causes it:

      defmodule MyApp.Repo.Migrations.RenameOrderProjection do
        use Ecto.Migration

        def up do
          DMEx.Consumer.Cursors.rename!(
            "orders/order_handler",
            "orders/order_projection",
            repo: MyApp.Repo
          )
        end
      end

  Note that `rename!/3` takes identifier **strings**, while everything else here
  takes the handler module. That asymmetry is honest rather than accidental: the
  old identity is precisely the one no module declares, so there is no module to
  name it with.

  ## Adding a message type does not replay its history

  A handler's message set is not part of its identity, so adding a clause for a
  new type leaves the cursor where it is and that type is handled from the tip
  onward.
  When you want that type's history too, `move!/3` is how you ask for it — and
  it replays *everything* after the id you name, not only the new type, so
  handlers must tolerate the redelivery.

  ## Options

  All four take `repo:` (required), plus optional `storage:` and `prefix:`,
  the same as `DMEx.Consumer`.

  ## These do not touch a running subscription

  A live `DMEx.Consumer.Stream` holds its cursor in memory and re-reads the
  durable one only when it connects. Moving a cursor under a running
  subscription therefore takes effect on its next reconnect, not immediately —
  so do this while it is stopped, or restart it afterwards.
  """

  alias DMEx.Consumer
  alias DMEx.Consumer.Handler

  @doc """
  Where `handler`'s subscription has got to, and when it last moved.

  Returns `nil` if it has never committed a position — which is also the normal
  state for a subscription started with `start: :latest` on a quiet log, since
  nothing writes a cursor until the first message is handled.
  """
  @spec inspect(module(), keyword()) ::
          {:ok, %{last_seen: non_neg_integer(), updated_at: term()} | nil}
          | {:error, term()}
  def inspect(handler, opts) when is_atom(handler) do
    {storage, storage_opts} = Consumer.storage!(opts)

    storage.cursor(storage_opts, Handler.subscription_id(handler))
  end

  @doc """
  Moves `handler`'s cursor to `id`, so the next connect resumes after it.

  `id: 0` replays the producer's whole log. Note this is *not* the same as
  `reset!/2`: a cursor at 0 still exists, so `start:` stays ignored.

  A cursor is nothing but a subscription and a position, so this needs no
  producer — it runs from a migration or a console, with no consumer running and
  nothing to resolve a producer name against.
  """
  @spec move!(module(), non_neg_integer(), keyword()) :: :ok
  def move!(handler, id, opts)
      when is_atom(handler) and is_integer(id) and id >= 0 do
    {storage, storage_opts} = Consumer.storage!(opts)
    subscription = Handler.subscription_id(handler)

    {:ok, :ok} =
      storage.transaction(storage_opts, fn ->
        storage.advance(storage_opts, subscription, id)
      end)

    :ok
  end

  @doc """
  Removes `handler`'s cursor entirely.

  Distinct from moving it to 0: with no row at all, the subscription's `start:`
  option applies again on its next start. That is the difference between "replay
  everything" and "begin however this subscription was configured to begin".
  """
  @spec reset!(module(), keyword()) :: :ok
  def reset!(handler, opts) when is_atom(handler) do
    {storage, storage_opts} = Consumer.storage!(opts)

    :ok = storage.delete_cursor(storage_opts, Handler.subscription_id(handler))
  end

  @doc """
  Carries the position of subscription `from` over to `to` and removes the old
  row.

  Both are identifier strings — `"orders/order_handler"`,
  `"orders/order_projection"` — since `from` is by definition the identity
  nothing declares any longer.

  Both steps happen in one transaction, so a rename cannot half-apply and leave
  two cursors — or none. Raises if `to` already has a cursor, since silently
  overwriting one subscription's position with another's is not a thing to do
  quietly.

  Returns `:ok` even when `from` has no cursor: there was nothing to carry, and
  the rename is still correctly applied.
  """
  @spec rename!(String.t(), String.t(), keyword()) :: :ok
  def rename!(from, to, opts) when is_binary(from) and is_binary(to) do
    {storage, storage_opts} = Consumer.storage!(opts)

    {:ok, :ok} =
      storage.transaction(storage_opts, fn ->
        case {storage.cursor(storage_opts, from), storage.cursor(storage_opts, to)} do
          {{:ok, nil}, _} ->
            :ok

          {_, {:ok, %{last_seen: existing}}} ->
            raise ArgumentError,
                  "#{Kernel.inspect(to)} already has a cursor at #{existing}. " <>
                    "Renaming #{Kernel.inspect(from)} onto it would overwrite that " <>
                    "subscription's position — reset one of them first if that is what you want."

          {{:ok, %{last_seen: id}}, {:ok, nil}} ->
            :ok = storage.advance(storage_opts, to, id)
            :ok = storage.delete_cursor(storage_opts, from)
        end
      end)

    :ok
  end
end
