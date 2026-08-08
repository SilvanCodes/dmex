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
  alias DMEx.Storage.Postgres
  alias DMEx.Test.Handlers
  alias DMEx.Test.IntegrationLog, as: Log
  alias DMEx.Test.Messages.OrderPlaced
  alias DMEx.Test.Messages.OrderShipped

  setup do
    start_stack!()
  end

  describe "the wire" do
    test "a selective subscription requests the blob in the path", %{base: base} do
      # The whole scheme in one assertion: what the handler's clauses declare is
      # what the URL says, with nothing declared anywhere in between.
      %{fingerprint: fingerprint} = Handlers.Echo.__dmex__()

      assert {:ok, 200, headers, _ref} = connect(base <> "/stream/" <> fingerprint)

      # Exactly one of the log's three message types matched.
      assert header(headers, "dmex-matched-types") == "1"
      assert header(headers, "content-type") =~ "text/event-stream"
    end

    test "a bare /stream is still the whole log, and says nothing about matching",
         %{base: base} do
      # v1's behaviour, unchanged, for a browser EventSource or anything written
      # before selection existed.
      assert {:ok, 200, headers, _ref} = connect(base <> "/stream")

      assert header(headers, "dmex-matched-types") == nil
    end

    test "a fingerprint over every type the log knows matches all of them", %{base: base} do
      types = MapSet.to_list(DMEx.Message.types())
      {:ok, fingerprint} = Fingerprint.build(types)

      assert {:ok, 200, headers, _ref} =
               connect(base <> "/stream/" <> Fingerprint.encode(fingerprint))

      assert header(headers, "dmex-matched-types") == to_string(length(types))
    end

    test "a segment that is not a decodable blob is a 400", %{base: base} do
      assert {:ok, 400, _headers, body} = connect(base <> "/stream/not-a-blob!")

      assert body =~ "invalid filter segment"
    end

    test "a fingerprint matching none of the log's types is a 404", %{base: base} do
      {:ok, fingerprint} = Fingerprint.build(["Some.Other.App.Messages.Whatever"])

      assert {:ok, 404, _headers, body} =
               connect(base <> "/stream/" <> Fingerprint.encode(fingerprint))

      assert body =~ "matched none of the message types"
    end
  end

  describe "delivery" do
    test "only the selected types cross the wire", %{base: _base} do
      # `last_seen: 0` because the appends below follow the subscribe, and the
      # default resolves to the log's tip at connect. The log is empty here, so
      # the whole log is the same set of rows without racing that connect.
      {:ok, _} =
        DMEx.Supervisor.subscribe(Handlers.Echo, supervisor: TestSupervisor, last_seen: 0)

      {:ok, _} = Log.append(%OrderShipped{order_id: 1, carrier: "dhl"})
      {:ok, _} = Log.append(%OrderPlaced{order_id: 2})
      {:ok, id} = Log.append(%OrderPlaced{order_id: 3})

      assert_receive {:handled, %OrderPlaced{order_id: 2}, _}, 5_000
      assert_receive {:handled, %OrderPlaced{order_id: 3}, _}, 5_000
      refute_receive {:handled, %OrderShipped{}, _}, 200

      eventually(fn -> cursor("test/echo") == {:ok, id} end)
    end

    test "the log reads only matching rows, not all of them", %{base: base} do
      # The point of resolving server-side rather than filtering after the
      # bytes arrive: unselected rows are never read, never framed and never
      # sent. Asserted on the wire, since that is where it is observable.
      for n <- 1..20, do: {:ok, _} = Log.append(%OrderShipped{order_id: n, carrier: "dhl"})
      {:ok, _} = Log.append(%OrderPlaced{order_id: 99})

      %{fingerprint: fingerprint} = Handlers.Echo.__dmex__()

      assert {:ok, body} = read_frames(base <> "/stream/" <> fingerprint)

      assert body =~ "order_placed"
      refute body =~ "order_shipped"
    end

    test "two subscriptions with different selections read the same id space", %{base: _base} do
      # Not a partitioning: there is still one log and one id sequence, so a
      # selective subscription sees a sparse subset of the same ids rather than
      # a space of its own.
      {:ok, _} =
        DMEx.Supervisor.subscribe(Handlers.Echo, supervisor: TestSupervisor, last_seen: 0)

      {:ok, _} =
        DMEx.Supervisor.subscribe(Handlers.EchoAll, supervisor: TestSupervisor, last_seen: 0)

      {:ok, _} = Log.append(%OrderShipped{order_id: 1, carrier: "dhl"})
      {:ok, placed} = Log.append(%OrderPlaced{order_id: 2})

      assert_receive {:handled, %OrderPlaced{order_id: 2}, _}, 5_000
      assert_receive {:handled, %OrderShipped{order_id: 1}, _}, 5_000

      # Echo saw only id 2; EchoAll saw 1 and 2. Both cursors are denominated in
      # the same sequence, so they are directly comparable.
      eventually(fn -> cursor("test/echo") == {:ok, placed} end)
      eventually(fn -> cursor("test/echo_all") == {:ok, placed} end)
    end

    test "a 404 is retried rather than killing the subscription", %{base: _base} do
      # A consumer deployed ahead of the log that will emit its types is
      # legitimately in this state, so the repair is the log's deploy —
      # and stopping here would turn a deploy-ordering choice into a
      # permanently dead subscription.
      {:ok, [pid]} =
        DMEx.Supervisor.subscribe(__MODULE__.AheadOfLog, supervisor: TestSupervisor)

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

  defp absorb([{:status, ref, status} | rest], ref, _status, headers, body), do: absorb(rest, ref, status, headers, body)

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

  defp absorb([{:done, ref} | _rest], ref, status, headers, body), do: {:done, {:ok, status, headers, body}}

  defp absorb([_other | rest], ref, status, headers, body), do: absorb(rest, ref, status, headers, body)

  # Opens a stream and returns whatever it sends within a short window, which is
  # enough to see which message types made it onto the wire.
  # `last-event-id: 0` because these read what was appended *before* the
  # request: with the header absent the log answers with its tip and sends
  # nothing at all.
  defp read_frames(url) do
    uri = URI.parse(url)

    {:ok, conn} = Mint.HTTP.connect(:http, uri.host, uri.port)

    {:ok, conn, ref} =
      Mint.HTTP.request(
        conn,
        "GET",
        uri.path,
        [{"accept", "text/event-stream"}, {"last-event-id", "0"}],
        nil
      )

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
    Postgres.last_seen(subscription, %{repo: DMEx.Test.IntegrationRepo, prefix: nil})
  end
end
