defmodule DMEx.Consumer.Cursors do
  @moduledoc """
  Reading and moving a subscription's read position, deliberately.

  DMEx owns `last_seen` and application code never touches it during normal
  operation. These are the operator-facing exceptions: inspecting where a
  subscription has got to, deliberately replaying part of a log, and — the one
  that actually bites — migrating a cursor across a handler rename.

  ## Renaming a handler resets its cursor

  `last_seen` is keyed by handler module, because the handler *is* the
  subscription's identity. So renaming `MyApp.OrderHandler` to
  `MyApp.Orders.Handler` does not move that subscription's position: it creates
  a *new* subscription with no cursor at all, which then replays the producer's
  log from the beginning.

  That is a routine refactor with a decidedly non-routine consequence. Do it in
  a migration, next to the code change that causes it:

      defmodule MyApp.Repo.Migrations.RenameOrderHandler do
        use Ecto.Migration

        def up do
          DMEx.Consumer.Cursors.rename!(MyApp.OrderHandler, MyApp.Orders.Handler,
            repo: MyApp.Repo
          )
        end
      end

  ## Options

  All three take `repo:` (required), plus optional `storage:` and `prefix:`,
  the same as `DMEx.Consumer`.

  ## These do not touch a running subscription

  A live `DMEx.Consumer.Stream` holds its cursor in memory and re-reads the
  durable one only when it connects. Moving a cursor under a running
  subscription therefore takes effect on its next reconnect, not immediately —
  so do this while it is stopped, or restart it afterwards.
  """

  alias DMEx.Consumer

  @doc """
  Where `handler` has got to, with the producer it read from and when.

  Returns `nil` if it has never committed a position — which is also the normal
  state for a subscription started with `start: :latest` on a quiet log, since
  nothing writes a cursor until the first message is handled.
  """
  @spec inspect(module(), keyword()) ::
          {:ok, %{last_seen: non_neg_integer(), producer_url: String.t()} | nil}
          | {:error, term()}
  def inspect(handler, opts) when is_atom(handler) do
    {storage, storage_opts} = Consumer.storage!(opts)

    storage.cursor(storage_opts, handler)
  end

  @doc """
  Moves `handler`'s cursor to `id`, so the next connect resumes after it.

  `id: 0` replays the producer's whole log. Note this is *not* the same as
  `reset!/2`: a cursor at 0 still exists, so `start:` stays ignored.

  Requires the producer URL, because a cursor row records which producer it was
  reading from and there is nothing sensible to invent when creating one.
  """
  @spec move!(module(), non_neg_integer(), String.t(), keyword()) :: :ok
  def move!(handler, id, url, opts)
      when is_atom(handler) and is_integer(id) and id >= 0 and is_binary(url) do
    {storage, storage_opts} = Consumer.storage!(opts)

    {:ok, :ok} =
      storage.transaction(storage_opts, fn -> storage.advance(storage_opts, handler, id, url) end)

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

    :ok = storage.delete_cursor(storage_opts, handler)
  end

  @doc """
  Carries `from`'s position over to `to` and removes the old row.

  Both happen in one transaction, so a rename cannot half-apply and leave two
  cursors — or none. Raises if `to` already has a cursor, since silently
  overwriting one subscription's position with another's is not a thing to do
  quietly.

  Returns `:ok` even when `from` has no cursor: there was nothing to carry, and
  the rename is still correctly applied.
  """
  @spec rename!(module(), module(), keyword()) :: :ok
  def rename!(from, to, opts) when is_atom(from) and is_atom(to) do
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

          {{:ok, %{last_seen: id, producer_url: url}}, {:ok, nil}} ->
            :ok = storage.advance(storage_opts, to, id, url)
            :ok = storage.delete_cursor(storage_opts, from)
        end
      end)

    :ok
  end
end
