defmodule DMEx.StorageError do
  @moduledoc """
  A `c:DMEx.Storage.transaction/2` that failed as infrastructure, not as a
  handler.

  Its own type because the pipeline has to tell the two apart: a handler raising
  is a verdict on one message and is retried, then resolved by `on_failure`,
  while this says the message was never processed at all. Nothing rescues it —
  see section 4 of `DMEx.Handler.Pipeline`.
  """

  defexception [:reason]

  @type t :: %__MODULE__{reason: term()}

  @impl true
  def exception(reason), do: %__MODULE__{reason: reason}

  @impl true
  def message(%__MODULE__{reason: reason}) do
    "storage transaction failed: #{inspect(reason)}"
  end
end
