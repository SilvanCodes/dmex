# Defined here rather than in test/support deliberately: modules compiled by
# ExUnit at runtime are not in `:application.get_key(:dmex, :modules)`, so the
# boot-time collision check never sees this declaring the same `type:` as
# `DMEx.Test.Messages.OrderPlaced`. That collision is the whole point — this is
# the *same message* under a different Elixir name.
defmodule DMEx.Integration.MessageTypeTest.Renamed do
  @moduledoc false
  # Nothing about this name matches the producer's module. Only `type:` does.
  use DMEx.Message, type: "order_placed", fields: [:order_id, :customer_id, :total]
end

defmodule DMEx.Integration.MessageTypeTest.RenamedConsumer do
  @moduledoc false
  use DMEx.Handler, logs: [:test], name: :renamed_consumer

  alias DMEx.Integration.MessageTypeTest.Renamed
  alias DMEx.Test.Handlers

  @impl true
  def handle_message(%Renamed{} = message, metadata) do
    send(Handlers.owner(), {:handled, message, metadata})

    :ok
  end
end

defmodule DMEx.Integration.MessageTypeTest do
  @moduledoc """
  A message's identity is its declared `type:`, not its module name.

  This is the failure the declaration exists to remove. When the type was
  derived from the module name, renaming or moving a message module was a
  wire-breaking change twice over: rows already in the log became unresolvable,
  and a consumer that had not redeployed fingerprinted the old name, so its
  server-side selection stopped matching anything the producer emitted.

  Here the producer appends `DMEx.Test.Messages.OrderPlaced` and the consumer
  matches on a module sharing nothing with it but the declared type. If the two
  are genuinely decoupled, the consumer decodes rows the producer wrote — both
  ones already in the log at connect and ones appended live.
  """

  use DMEx.IntegrationCase

  alias DMEx.Integration.MessageTypeTest.Renamed
  alias DMEx.Integration.MessageTypeTest.RenamedConsumer
  alias DMEx.Test.Handlers
  alias DMEx.Test.IntegrationLog, as: Log
  alias DMEx.Test.Messages.OrderPlaced

  setup do
    start_stack!()
  end

  test "a consumer resolves rows by declared type, not by the appending module's name" do
    # Appended *before* the subscription exists, so these are rows already in the
    # log — the case a module rename used to orphan permanently.
    {:ok, _} = Log.append(%OrderPlaced{order_id: 1, customer_id: 10, total: 999})

    {:ok, _} =
      DMEx.Supervisor.subscribe(RenamedConsumer, supervisor: TestSupervisor, last_seen: 0)

    # Decoded into the consumer's own struct, with the producer's field values
    # intact: the type carried the identity across two unrelated module names.
    assert_receive {:handled, %Renamed{order_id: 1, customer_id: 10, total: 999}, _}, 5_000

    # And live, after the connection is up — the selection the producer resolved
    # at connect matched on the declared type too, or this never arrives.
    {:ok, _} = Log.append(%OrderPlaced{order_id: 2, customer_id: 20, total: 1_999})

    assert_receive {:handled, %Renamed{order_id: 2, customer_id: 20, total: 1_999}, _}, 5_000

    # Two unrelated modules, one type. The `%Renamed{}` matches above are what
    # prove the consumer decoded into its own struct rather than the producer's.
    assert Renamed.type() == OrderPlaced.type()
    assert inspect(Renamed) != inspect(OrderPlaced)
  end
end
