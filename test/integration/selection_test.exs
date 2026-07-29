# Defined in the test file rather than in test/support deliberately: modules
# compiled by ExUnit at runtime are not in `:application.get_key(:dmex,
# :modules)`, so this is a message type the producer's scan genuinely cannot
# see — which is exactly the consumer-deployed-ahead-of-its-producer case.
defmodule DMEx.Integration.SelectionTest.NotYetPublished do
  @moduledoc false
  use DMEx.Message

  # No `@derive JSON.Encoder`: nothing ever publishes this, which is the point.
  defstruct [:order_id]
end

defmodule DMEx.Integration.SelectionTest.AheadOfProducer do
  @moduledoc false
  use DMEx.Consumer.Handler, producer: :test, id: :ahead_of_producer

  @impl true
  def handle_message(%DMEx.Integration.SelectionTest.NotYetPublished{}, _metadata), do: :ok
end

defmodule DMEx.Integration.SelectionTest do
  @moduledoc """
  Server-side message-type selection, end to end over a real socket and a real
  database.

  The unit suites pin each half — the encoding, the path routing, the wakeup
  routing. This is the one that pins that they compose: a consumer's handler
  clauses become a blob in the request path, the producer resolves it against
  its own compiled message modules, and only matching rows are read and sent.
  """

  use DMEx.IntegrationCase

  alias DMEx.Consumer
  alias DMEx.Fingerprint
  alias DMEx.Producer.Messages
  alias DMEx.Storage.Postgres
  alias DMEx.Test.Handlers
  alias DMEx.Test.IntegrationProducer, as: Producer
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Messages.OrderShipped
  alias DMEx.Test.Server

  @moduletag :capture_log

  setup do
    Handlers.report_to(self())

    start_supervised!(Producer)

    base = Server.start!()

    start_supervised!(
      {Consumer,
       name: TestConsumer,
       repo: DMEx.Test.IntegrationRepo,
       producer: Server.producers(base <> "/stream")}
    )

    %{base: base}
  end

  describe "the wire" do
    test "a selective subscription requests the blob in the path", %{base: base} do
      # The whole scheme in one assertion: what the handler's clauses declare is
      # what the URL says, with nothing declared anywhere in between.
      %{fingerprint: fingerprint} = Handlers.Echo.__dmex__()

      assert {:ok, 200, headers, _ref} = connect(base <> "/stream/" <> fingerprint)

      # Exactly one of the producer's three message types matched.
      assert header(headers, "dmex-matched-messages") == "1"
      assert header(headers, "content-type") =~ "text/event-stream"
    end

    test "a bare /stream is still the whole log, and says nothing about matching",
         %{base: base} do
      # v1's behaviour, unchanged, for a browser EventSource or anything written
      # before selection existed.
      assert {:ok, 200, headers, _ref} = connect(base <> "/stream")

      assert header(headers, "dmex-matched-messages") == nil
    end

    test "a fingerprint over every type the producer knows matches all of them", %{base: base} do
      types = Producer |> Messages.types() |> MapSet.to_list()
      {:ok, fingerprint} = Fingerprint.build(types)

      assert {:ok, 200, headers, _ref} =
               connect(base <> "/stream/" <> Fingerprint.encode(fingerprint))

      assert header(headers, "dmex-matched-messages") == to_string(length(types))
    end

    test "a segment that is not a decodable blob is a 400", %{base: base} do
      assert {:ok, 400, _headers, body} = connect(base <> "/stream/not-a-blob!")

      assert body =~ "invalid filter segment"
    end

    test "a reserved sub-resource is a 404, not a fingerprint", %{base: base} do
      # `stats` is reserved before it is implemented, so a blob can never claim
      # the name later.
      assert {:ok, 404, _headers, body} = connect(base <> "/stream/stats")

      assert body =~ "reserved"
    end

    test "a fingerprint matching none of the producer's types is a 404", %{base: base} do
      {:ok, fingerprint} = Fingerprint.build(["Some.Other.App.Messages.Whatever"])

      assert {:ok, 404, _headers, body} =
               connect(base <> "/stream/" <> Fingerprint.encode(fingerprint))

      assert body =~ "matched none of the message types"
    end
  end

  describe "delivery" do
    test "only the selected types cross the wire", %{base: _base} do
      {:ok, _} = Consumer.subscribe(Handlers.Echo, consumer: TestConsumer, batch_window: 50)

      {:ok, _} = Producer.publish(%OrderShipped{order_id: 1, carrier: "dhl"})
      {:ok, _} = Producer.publish(%OrderPlaced{order_id: 2})
      {:ok, id} = Producer.publish(%OrderPlaced{order_id: 3})

      assert_receive {:handled, %OrderPlaced{order_id: 2}, _}, 5_000
      assert_receive {:handled, %OrderPlaced{order_id: 3}, _}, 5_000
      refute_receive {:handled, %OrderShipped{}, _}, 200

      eventually(fn -> cursor("test/echo") == {:ok, id} end)
    end

    test "the producer reads only matching rows, not all of them", %{base: base} do
      # The point of resolving server-side rather than filtering after the
      # bytes arrive: unselected rows are never read, never framed and never
      # sent. Asserted on the wire, since that is where it is observable.
      for n <- 1..20, do: {:ok, _} = Producer.publish(%OrderShipped{order_id: n, carrier: "dhl"})
      {:ok, _} = Producer.publish(%OrderPlaced{order_id: 99})

      %{fingerprint: fingerprint} = Handlers.Echo.__dmex__()

      assert {:ok, body} = read_frames(base <> "/stream/" <> fingerprint)

      assert body =~ "OrderPlaced"
      refute body =~ "OrderShipped"
    end

    test "two subscriptions with different selections read the same id space", %{base: _base} do
      # Not a partitioning: there is still one log and one id sequence, so a
      # selective subscription sees a sparse subset of the same ids rather than
      # a space of its own.
      {:ok, _} = Consumer.subscribe(Handlers.Echo, consumer: TestConsumer, batch_window: 50)
      {:ok, _} = Consumer.subscribe(Handlers.EchoAll, consumer: TestConsumer, batch_window: 50)

      {:ok, _} = Producer.publish(%OrderShipped{order_id: 1, carrier: "dhl"})
      {:ok, placed} = Producer.publish(%OrderPlaced{order_id: 2})

      assert_receive {:handled, %OrderPlaced{order_id: 2}, _}, 5_000
      assert_receive {:handled, %OrderShipped{order_id: 1}, _}, 5_000

      # Echo saw only id 2; EchoAll saw 1 and 2. Both cursors are denominated in
      # the same sequence, so they are directly comparable.
      eventually(fn -> cursor("test/echo") == {:ok, placed} end)
      eventually(fn -> cursor("test/echo_all") == {:ok, placed} end)
    end

    test "a 404 is retried rather than killing the subscription", %{base: _base} do
      # A consumer deployed ahead of the producer that will emit its types is
      # legitimately in this state, so the repair is the producer's deploy —
      # and stopping here would turn a deploy-ordering choice into a
      # permanently dead subscription.
      {:ok, pid} =
        Consumer.subscribe(__MODULE__.AheadOfProducer, consumer: TestConsumer, batch_window: 50)

      ref = Process.monitor(pid)

      refute_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
      assert Process.alive?(pid)
    end
  end

  ## Helpers

  defp connect(url) do
    uri = URI.parse(url)

    {:ok, conn} = Mint.HTTP.connect(:http, uri.host, uri.port)

    {:ok, conn, ref} =
      Mint.HTTP.request(conn, "GET", uri.path, [{"accept", "text/event-stream"}], nil)

    result = collect(conn, ref, nil, [], "", System.monotonic_time(:millisecond))

    Mint.HTTP.close(conn)

    result
  end

  # Reads until the response is understood: a non-200 is read to completion for
  # its body, and a 200 is a stream that never ends, so its headers are enough.
  defp collect(conn, ref, status, headers, body, started) do
    if System.monotonic_time(:millisecond) - started > 5_000 do
      {:error, :timeout}
    else
      receive do
        message ->
          case Mint.HTTP.stream(conn, message) do
            {:ok, conn, responses} ->
              case absorb(responses, ref, status, headers, body) do
                {:done, result} ->
                  result

                {:cont, status, headers, body} ->
                  collect(conn, ref, status, headers, body, started)
              end

            _other ->
              {:error, :closed}
          end
      after
        5_000 -> {:error, :timeout}
      end
    end
  end

  defp absorb([], _ref, status, headers, body), do: {:cont, status, headers, body}

  defp absorb([{:status, ref, status} | rest], ref, _status, headers, body),
    do: absorb(rest, ref, status, headers, body)

  defp absorb([{:headers, ref, headers} | rest], ref, status, _headers, body) do
    if status == 200 do
      # A 200 here is an SSE stream that never completes, so the headers are as
      # much of it as there is to wait for.
      {:done, {:ok, status, headers, ref}}
    else
      absorb(rest, ref, status, headers, body)
    end
  end

  defp absorb([{:data, ref, chunk} | rest], ref, status, headers, body),
    do: absorb(rest, ref, status, headers, body <> chunk)

  defp absorb([{:done, ref} | _rest], ref, status, headers, body),
    do: {:done, {:ok, status, headers, body}}

  defp absorb([_other | rest], ref, status, headers, body),
    do: absorb(rest, ref, status, headers, body)

  # Opens a stream and returns whatever it sends within a short window, which is
  # enough to see which message types made it onto the wire.
  defp read_frames(url) do
    uri = URI.parse(url)

    {:ok, conn} = Mint.HTTP.connect(:http, uri.host, uri.port)

    {:ok, conn, ref} =
      Mint.HTTP.request(conn, "GET", uri.path, [{"accept", "text/event-stream"}], nil)

    body = drain(conn, ref, "", System.monotonic_time(:millisecond))

    Mint.HTTP.close(conn)

    {:ok, body}
  end

  defp drain(conn, ref, body, started) do
    if System.monotonic_time(:millisecond) - started > 2_000 do
      body
    else
      receive do
        message ->
          case Mint.HTTP.stream(conn, message) do
            {:ok, conn, responses} ->
              chunk =
                for {:data, ^ref, data} <- responses, into: "", do: data

              drain(conn, ref, body <> chunk, started)

            _other ->
              body
          end
      after
        2_000 -> body
      end
    end
  end

  defp header(headers, name) do
    case List.keyfind(headers, name, 0) do
      {^name, value} -> value
      nil -> nil
    end
  end

  defp cursor(subscription) do
    Postgres.last_seen(%{repo: DMEx.Test.IntegrationRepo, prefix: nil}, subscription)
  end
end
