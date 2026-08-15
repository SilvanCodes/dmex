# Defined in the test file rather than in test/support deliberately: modules
# compiled by ExUnit at runtime are not in `:application.get_key(:dmex,
# :modules)`, so this is a message type the log's scan genuinely cannot
# see — which is exactly the consumer-deployed-ahead-of-its-log case.
defmodule DMEx.Integration.SelectionTest.NotYetPublished do
  @moduledoc false
  # Nothing ever appends this, which is the point — it exists only for the
  # consumer's handler to match on. Elixir warns that the generated
  # `@derive JSON.Encoder` has no effect, protocols being consolidated long
  # before a test file defines a module; nothing here encodes it.
  use DMEx.Message, type: "not_yet_published", fields: [:order_id]
end

defmodule DMEx.Integration.SelectionTest.AheadOfLog do
  @moduledoc false
  use DMEx.Handler, logs: [:test], name: :ahead_of_log

  @impl true
  def handle_message(%DMEx.Integration.SelectionTest.NotYetPublished{}, _metadata), do: :ok
end

defmodule DMEx.Integration.SelectionTest do
  @moduledoc """
  Server-side message-type selection, end to end over a real socket and a real
  database.

  The unit suites pin each half — the encoding, the path routing, the wakeup
  routing. This is the one that pins that they compose: a consumer's handler
  clauses become a blob in the request path, the log resolves it against
  its own compiled message modules, and only matching rows are read and sent.
  """

  use DMEx.IntegrationCase

  alias DMEx.Handler.Fingerprint
  alias DMEx.Test.Handlers
  alias DMEx.Test.IntegrationLog, as: Log
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Messages.OrderShipped
  alias DMEx.Test.SSEClient

  setup do
    start_stack!()
  end

  describe "the wire" do
    test "a selective subscription requests the blob in the path", ctx do
      # The whole scheme in one assertion: what the handler's clauses declare is
      # what the URL says, with nothing declared anywhere in between.
      %{fingerprint: fingerprint} = Handlers.Echo.__dmex_handler__()

      assert {200, headers, _body} = connect(ctx, "/stream/" <> fingerprint)

      # Exactly one of the log's three message types matched.
      assert SSEClient.header(headers, "dmex-matched-types") == "1"
      assert SSEClient.header(headers, "content-type") =~ "text/event-stream"
    end

    test "a bare /stream is still the whole log, and says nothing about matching", ctx do
      # v1's behaviour, unchanged, for a browser EventSource or anything written
      # before selection existed.
      assert {200, headers, _body} = connect(ctx, "/stream")

      assert SSEClient.header(headers, "dmex-matched-types") == nil
    end

    test "a fingerprint over every type the log knows matches all of them", ctx do
      types = MapSet.to_list(DMEx.Message.types())
      {:ok, fingerprint} = Fingerprint.build(types)

      assert {200, headers, _body} = connect(ctx, "/stream/" <> Fingerprint.encode(fingerprint))

      assert SSEClient.header(headers, "dmex-matched-types") == to_string(length(types))
    end

    test "a segment that is not a decodable blob is a 400", ctx do
      assert {400, _headers, body} = connect(ctx, "/stream/not-a-blob!")

      assert body =~ "invalid filter segment"
    end

    test "a fingerprint matching none of the log's types is a 404", ctx do
      {:ok, fingerprint} = Fingerprint.build(["Some.Other.App.Messages.Whatever"])

      assert {404, _headers, body} = connect(ctx, "/stream/" <> Fingerprint.encode(fingerprint))

      assert body =~ "matched none of the message types"
    end
  end

  describe "delivery" do
    test "only the selected types cross the wire", _ctx do
      {:ok, _} = subscribe!(Handlers.Echo)

      {:ok, _} = Log.append(%OrderShipped{order_id: 1, carrier: "dhl"})
      {:ok, _} = Log.append(%OrderPlaced{order_id: 2})
      {:ok, id} = Log.append(%OrderPlaced{order_id: 3})

      assert_receive {:handled, %OrderPlaced{order_id: 2}, _}, 5_000
      assert_receive {:handled, %OrderPlaced{order_id: 3}, _}, 5_000
      refute_receive {:handled, %OrderShipped{}, _}, 200

      eventually(fn -> cursor("test/echo") == {:ok, id} end)
    end

    test "the log reads only matching rows, not all of them", ctx do
      # The point of resolving server-side rather than filtering after the
      # bytes arrive: unselected rows are never read, never framed and never
      # sent. Asserted on the wire, since that is where it is observable.
      for n <- 1..20, do: {:ok, _} = Log.append(%OrderShipped{order_id: n, carrier: "dhl"})
      {:ok, _} = Log.append(%OrderPlaced{order_id: 99})

      %{fingerprint: fingerprint} = Handlers.Echo.__dmex_handler__()

      body = read_frames(ctx, "/stream/" <> fingerprint)

      assert body =~ "order_placed"
      refute body =~ "order_shipped"
    end

    test "two subscriptions with different selections read the same id space", _ctx do
      # Not a partitioning: there is still one log and one id sequence, so a
      # selective subscription sees a sparse subset of the same ids rather than
      # a space of its own.
      {:ok, _} = subscribe!(Handlers.Echo)
      {:ok, _} = subscribe!(Handlers.EchoAll)

      {:ok, _} = Log.append(%OrderShipped{order_id: 1, carrier: "dhl"})
      {:ok, placed} = Log.append(%OrderPlaced{order_id: 2})

      assert_receive {:handled, %OrderPlaced{order_id: 2}, _}, 5_000
      assert_receive {:handled, %OrderShipped{order_id: 1}, _}, 5_000

      # Echo saw only id 2; EchoAll saw 1 and 2. Both cursors are denominated in
      # the same sequence, so they are directly comparable.
      eventually(fn -> cursor("test/echo") == {:ok, placed} end)
      eventually(fn -> cursor("test/echo_all") == {:ok, placed} end)
    end

    test "a 404 is retried rather than killing the subscription", _ctx do
      # A consumer deployed ahead of the log that will emit its types is
      # legitimately in this state, so the repair is the log's deploy —
      # and stopping here would turn a deploy-ordering choice into a
      # permanently dead subscription. Subscribed with the default `last_seen:`,
      # since a deploy-ahead consumer has no reason to ask for history.
      {:ok, [pid]} = DMEx.Supervisor.subscribe(__MODULE__.AheadOfLog, supervisor: TestSupervisor)

      # Observing the 404 itself, not just the absence of a death: without
      # this the assertions below hold for any healthy subscription, and the
      # retry policy would be untested end to end.
      telemetry = :telemetry_test.attach_event_handlers(self(), [[:dmex, :handler, :reconnect]])

      assert_receive {[:dmex, :handler, :reconnect], ^telemetry, _measurements, metadata}, 5_000
      assert {:no_matching_types, _url} = metadata.reason

      ref = Process.monitor(pid)

      refute_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
      assert Process.alive?(pid)
    end
  end

  ## Helpers

  # A 200 is a stream that never ends, so its headers are the whole of the
  # response there is to wait for; anything else is read to completion for its
  # body.
  defp connect(ctx, path) do
    SSEClient.request(ctx.host, ctx.port, path, [{"accept", "text/event-stream"}],
      until: :headers,
      timeout: 5_000
    )
  end

  # `last-event-id: 0` because this reads what was appended *before* the
  # request: with the header absent the log answers with its tip and sends
  # nothing at all.
  defp read_frames(ctx, path) do
    headers = [{"accept", "text/event-stream"}, {"last-event-id", "0"}]

    {200, _headers, body} =
      SSEClient.request(ctx.host, ctx.port, path, headers, timeout: 2_000)

    body
  end
end
