defmodule DMEx.Integration.CatchupTest do
  @moduledoc """
  Catch-up is the ordinary loop running hot, not a distinct mode.

  There is no separate endpoint, no separate process, and no branch anywhere
  that distinguishes a consumer draining a backlog from one tailing the tip.
  They differ only in whether the loop finds rows waiting when it reads.
  """

  use DMEx.IntegrationCase

  alias DMEx.Consumer
  alias DMEx.Test.Handlers
  alias DMEx.Test.IntegrationProducer, as: Producer
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Server
  alias Ecto.Adapters.SQL

  @moduletag :capture_log
  @moduletag timeout: 180_000

  @backlog 2_000

  setup do
    Handlers.report_to(self())

    start_supervised!(Producer)

    # The server binds port 0, so the producer map is necessarily built at
    # runtime — which is exactly why it lives on the child spec rather than in
    # application config.
    url = Server.start!() <> "/stream"

    start_supervised!(
      {Consumer,
       name: TestConsumer, repo: DMEx.Test.IntegrationRepo, producer: Server.producers(url)}
    )

    seed(@backlog)

    %{url: url}
  end

  test "drains a large backlog in order, in frames no larger than requested", _ctx do
    ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :stream, :frame]])

    {:ok, _} =
      Consumer.subscribe(Handlers.Echo,
        consumer: TestConsumer,
        max_batch_size: 250,
        batch_window: 20
      )

    ids =
      for _ <- 1..@backlog do
        assert_receive {:handled, %OrderPlaced{order_id: id}, _}, 30_000
        id
      end

    assert ids == Enum.to_list(1..@backlog)

    # `max_batch_size` is a hard cap on frame size, however far behind the
    # consumer is — the loop reads with it as the LIMIT every single time.
    frame_sizes = collect_frame_sizes(ref)

    assert frame_sizes != []
    assert Enum.max(frame_sizes) <= 250
  end

  test "the first frame's max_id tells the consumer how big the gap is", _ctx do
    ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :consumer, :lag]])

    {:ok, _} =
      Consumer.subscribe(Handlers.Echo,
        consumer: TestConsumer,
        max_batch_size: 100,
        batch_window: 20
      )

    # No HEAD preflight and no separate backlog endpoint: the frame the consumer
    # was going to receive anyway already carries the answer.
    assert_receive {[:dmex, :consumer, :lag], ^ref, %{lag: lag}, _}, 30_000
    assert lag > 0
  end

  test "and how much of the consumer's capacity is already spoken for", _ctx do
    ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :consumer, :lag]])

    {:ok, _} =
      Consumer.subscribe(Handlers.Echo,
        consumer: TestConsumer,
        max_batch_size: 100,
        batch_window: 20
      )

    # The signal lag alone cannot give: a consumer keeping up with room to spare
    # and one keeping up with none both report zero lag, and they are the
    # difference between healthy and one traffic increment from falling behind.
    assert_receive {[:dmex, :consumer, :lag], ^ref, %{utilization: utilization}, _}, 30_000

    assert utilization >= 0.0
    assert utilization <= 1.0

    # Draining a backlog flat out, so it must climb well off the floor.
    eventually(
      fn ->
        receive do
          {[:dmex, :consumer, :lag], ^ref, %{utilization: u}, _} when u > 0.05 -> true
        after
          0 -> false
        end
      end,
      30_000,
      20
    )
  end

  ## Helpers

  # Seeded with one statement rather than @backlog publishes: this test is about
  # the read path, and appends are deliberately serialized.
  defp seed(count) do
    SQL.query!(
      Repo,
      """
      INSERT INTO dmex_messages (message_type, data, metadata)
      SELECT $1, json_build_object('order_id', n)::text::jsonb, '{}'::jsonb
        FROM generate_series(1, $2) AS n
      """,
      ["DMEx.Test.Messages.OrderPlaced", count]
    )
  end

  defp collect_frame_sizes(ref, acc \\ []) do
    receive do
      {[:dmex, :stream, :frame], ^ref, %{rows: rows}, _} -> collect_frame_sizes(ref, [rows | acc])
    after
      0 -> acc
    end
  end
end
