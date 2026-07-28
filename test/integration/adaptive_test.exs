defmodule DMEx.Integration.AdaptiveTest do
  @moduledoc """
  The backpressure loop, closed through a real socket.

  A consumer that cannot keep up would otherwise settle into a reconnect loop:
  the producer's `send_timeout` closes the stalled socket, the consumer
  reconnects, is handed frames at the same rate it just failed to drain, and
  stalls again — each cycle costing the producer a fresh catch-up read. The
  slower the consumer, the more often it makes the producer work.

  What these assert is that the loop converts that into a subscription running
  stably at its own speed.
  """

  use DMEx.IntegrationCase

  alias DMEx.Consumer
  alias DMEx.Consumer.Queue
  alias DMEx.Test.Handlers
  alias DMEx.Test.IntegrationProducer, as: Producer
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Server
  alias Ecto.Adapters.SQL

  @moduletag :capture_log
  @moduletag timeout: 120_000

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

  describe "a consumer that cannot keep up" do
    setup ctx do
      # A big batch against a slow handler: the producer can hand over far more
      # per window than the consumer drains, which is exactly the mismatch.
      {:ok, pid} =
        Consumer.subscribe(Handlers.Echo,
          consumer: TestConsumer,
          max_batch_size: 500,
          batch_window: 20,
          min_samples: 5,
          reparameterize_interval: 500,
          # Recovery is tested separately. Left at its default it would climb
          # back toward 500 as soon as the backlog drained, and these
          # assertions would be racing it rather than observing a back-off.
          recovery_windows: 100_000
        )

      Map.put(ctx, :pid, pid)
    end

    test "holds the socket off rather than buffering without limit", ctx do
      # Structural backpressure: the TCP receive window closes because the
      # consumer stops re-arming, not because anything decided to stop reading.
      eventually(fn -> :sys.get_state(ctx.pid).socket_mode == :passive end, 20_000, 20)

      state = :sys.get_state(ctx.pid)

      # And the bound really is a bound: peak depth never exceeds queue_limit,
      # because the gate reserves room for a whole frame.
      assert Queue.depth(state.queue) <= state.queue.limit
    end

    test "re-parameterizes down to something it can actually drain", _ctx do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :consumer, :reparameterize]])

      assert_receive {[:dmex, :consumer, :reparameterize], ^ref, measurements, metadata}, 30_000

      # Sized from measured service time, not guessed.
      assert measurements.max_batch_size < 500
      assert metadata.mean_service_ms > 0
      assert metadata.handler == Handlers.Echo
    end

    test "asks the producer for the smaller batch on the next connect", ctx do
      eventually(
        fn -> :sys.get_state(ctx.pid).requested.max_batch_size < 500 end,
        30_000,
        20
      )

      # And the producer echoed it back, so what the controller reasons about
      # next is what actually took effect rather than what was hoped for.
      eventually(
        fn ->
          state = :sys.get_state(ctx.pid)
          state.conn != nil and state.params.max_batch_size < 500
        end,
        30_000,
        20
      )
    end

    test "loses nothing across the re-parameterization", _ctx do
      # Close, drain locally, reconnect from the post-drain cursor: every queued
      # row goes through its own transaction, so nothing already transferred is
      # re-fetched and nothing is skipped.
      ids =
        for _ <- 1..@backlog do
          assert_receive {:handled, %OrderPlaced{order_id: id}, _}, 60_000
          id
        end

      assert ids == Enum.to_list(1..@backlog)
    end
  end

  describe "recovering" do
    test "climbs back toward the configured pacing once the pressure clears", _ctx do
      {:ok, pid} =
        Consumer.subscribe(Handlers.Echo,
          consumer: TestConsumer,
          max_batch_size: 500,
          batch_window: 20,
          min_samples: 5,
          reparameterize_interval: 200,
          recovery_windows: 1
        )

      # First it backs off under the backlog...
      backed_off =
        eventually(
          fn ->
            requested = :sys.get_state(pid).requested.max_batch_size
            if requested < 500, do: requested
          end,
          30_000,
          20
        )

      # ...then, with the backlog gone and the stream quiet, it steps back up.
      # Additively and one notch at a time: back off fast, recover slowly.
      eventually(
        fn -> :sys.get_state(pid).requested.max_batch_size > backed_off end,
        30_000,
        50
      )
    end
  end

  describe "a consumer that is keeping up" do
    test "never re-parameterizes", _ctx do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :consumer, :reparameterize]])

      # A small batch that the handler empties well within the window, so depth
      # never sustains above the mark and the trigger cannot fire.
      {:ok, _pid} =
        Consumer.subscribe(Handlers.Echo,
          consumer: TestConsumer,
          max_batch_size: 5,
          batch_window: 100,
          min_samples: 5,
          reparameterize_interval: 200
        )

      for _ <- 1..50 do
        assert_receive {:handled, %OrderPlaced{}, _}, 30_000
      end

      refute_received {[:dmex, :consumer, :reparameterize], ^ref, _, _}
    end
  end

  describe "adaptive: false" do
    test "pins the pacing however far behind it falls", _ctx do
      ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :consumer, :reparameterize]])

      {:ok, pid} =
        Consumer.subscribe(Handlers.Echo,
          consumer: TestConsumer,
          max_batch_size: 500,
          batch_window: 20,
          min_samples: 5,
          reparameterize_interval: 200,
          adaptive: false
        )

      # Enough pressure that an adaptive subscription would certainly have acted.
      eventually(fn -> :sys.get_state(pid).socket_mode == :passive end, 20_000, 20)
      Process.sleep(1_000)

      refute_received {[:dmex, :consumer, :reparameterize], ^ref, _, _}
      assert :sys.get_state(pid).requested.max_batch_size == 500
    end
  end

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
end
