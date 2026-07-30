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

  test "drains a large backlog in order, in writes no larger than requested", _ctx do
    ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :stream, :write]])

    {:ok, _} = Consumer.subscribe(Handlers.Echo, consumer: TestConsumer)

    ids =
      for _ <- 1..@backlog do
        assert_receive {:handled, %OrderPlaced{order_id: id}, _}, 30_000
        id
      end

    assert ids == Enum.to_list(1..@backlog)

    # `max_batch_size` is a hard cap on how much one write carries, however far
    # behind the consumer is — the loop reads with it as the LIMIT every time.
    # It is the *producer's* setting, so this is the producer's default.
    write_sizes = collect_write_sizes(ref)

    assert write_sizes != []
    assert Enum.max(write_sizes) <= DMEx.Config.producer!([]).max_batch_size
  end

  # How far behind is answered from `inserted_at`, which every row carries
  # anyway — no `max_id` on the wire, no HEAD preflight, and no separate backlog
  # endpoint. A consumer draining a backlog is handling rows published a while
  # ago, and that is exactly what this measures.
  test "the rows a catching-up consumer handles are visibly stale", _ctx do
    ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :consumer, :lag]])

    seeded_at = System.monotonic_time(:millisecond)

    {:ok, _} = Consumer.subscribe(Handlers.Echo, consumer: TestConsumer)

    assert_receive {[:dmex, :consumer, :lag], ^ref, %{lag: lag, rows: rows}, metadata}, 30_000

    # The backlog was seeded before the subscription started, so every row in it
    # is older than the subscription is.
    assert lag > 0
    assert lag < System.monotonic_time(:millisecond) - seeded_at + 60_000
    assert rows > 0
    assert metadata.subscription == "test/echo"
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

  defp collect_write_sizes(ref, acc \\ []) do
    receive do
      {[:dmex, :stream, :write], ^ref, %{rows: rows}, _} -> collect_write_sizes(ref, [rows | acc])
    after
      0 -> acc
    end
  end
end
