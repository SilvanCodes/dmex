defmodule DMEx.Publisher do
  @moduledoc """
  Appends a message to a producer's log.

      DMEx.Publisher.publish(MyApp.Producer, %MyApp.Messages.OrderPlaced{order_id: 7})
      #=> {:ok, 1042}

  This is the explicit form, for generic code holding a producer module in a
  variable. `MyApp.Producer.publish/2` is the same call with the producer
  already bound, and is the ergonomic one at an ordinary call site.

  **Publishing is just writing a row.** There is no client/server RPC, no broker
  call, and no network hop on the write path — the insert, plus the notification
  that wakes live streams afterwards, is the entire story. It is a normal
  database write against the application's own database, no different from any
  other write that application makes.

  ## What it does, and what it deliberately does not

    * The wire type comes from the message's **own module** (`c:DMEx.Message.type/0`),
      never from a call argument, so it cannot disagree with the struct it
      describes.
    * `metadata` is passed through **untouched**. DMEx merges nothing into it,
      so there are no reserved key names a publisher has to avoid.
    * Nothing records which producer published the message. `message_type` is
      fully qualified, so `MyApp.Messages.OrderPlaced` already identifies the
      publishing application on every message — an `origin` field would restate
      what the type string already establishes.

  ## Ordering

  The append takes a transaction-scoped advisory lock keyed on the producer's
  app key, so ids commit in the order they were allocated and every reader's
  `id > cursor` scan is sound by construction. See `DMEx.Producer` for what that
  costs and for the one case that bites: publishing inside a long enclosing
  transaction.

  ## Telemetry

  Emits `[:dmex, :publish, :start | :stop | :exception]` with
  `%{producer: module, message_type: String.t()}` metadata.
  """

  @doc """
  Appends `message` to `producer`'s log and returns its assigned id.

  ## Options

    * `:metadata` — a map stored alongside the message and delivered to the
      consumer's handler verbatim. Defaults to `%{}`.

  Raises `ArgumentError` if `message` is not a struct built with
  `use DMEx.Message`, which is a clearer failure than the `UndefinedFunctionError`
  that calling `type/0` on it would otherwise produce.
  """
  @spec publish(module(), struct(), keyword()) :: {:ok, pos_integer()} | {:error, term()}
  def publish(producer, message, opts \\ []) when is_atom(producer) and is_struct(message) do
    validate_opts!(opts)

    %{storage: storage, storage_opts: storage_opts} = producer.__dmex__()

    type = message_type!(message)
    data = JSON.encode!(message)
    metadata = opts |> Keyword.get(:metadata, %{}) |> JSON.encode!()

    :telemetry.span(
      [:dmex, :publish],
      %{producer: producer, message_type: type},
      fn ->
        result = storage.append(storage_opts, type, data, metadata)

        {result, %{producer: producer, message_type: type}}
      end
    )
  end

  defp message_type!(%module{} = message) do
    if function_exported?(module, :type, 0) do
      module.type()
    else
      raise ArgumentError,
            "#{inspect(module)} is not a DMEx message module, so #{inspect(message)} " <>
              "cannot be published. Add `use DMEx.Message` to #{inspect(module)}."
    end
  end

  defp validate_opts!(opts) do
    case Enum.reject(Keyword.keys(opts), &(&1 == :metadata)) do
      [] -> :ok
      unknown -> raise ArgumentError, "unknown publish option(s): #{inspect(unknown)}"
    end
  end
end
