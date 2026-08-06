defmodule DMEx.Integration.CatchupTest do
  @moduledoc """
  Catch-up is the ordinary loop running hot, not a distinct mode.

  There is no separate endpoint, no separate process, and no branch anywhere
  that distinguishes a consumer draining a backlog from one tailing the tip.
  They differ only in whether the loop finds rows waiting when it reads.
  """

  use DMEx.IntegrationCase

  alias DMEx.Test.Handlers
  alias DMEx.Test.Messages.OrderPlaced
  alias Ecto.Adapters.SQL

  @moduletag timeout: 180_000

  @backlog 2_000

  setup do
    context = start_stack!()

    seed(@backlog)

    context
  end

  test "drains a large backlog in order, in writes no larger than requested", _ctx do
    ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :stream, :write]])

    {:ok, _} =
      DMEx.Supervisor.subscribe(Handlers.Echo, supervisor: TestSupervisor, last_seen: 0)

    ids =
      for _ <- 1..@backlog do
        assert_receive {:handled, %OrderPlaced{order_id: id}, _}, 30_000
        id
      end

    assert ids == Enum.to_list(1..@backlog)

    # `max_batch_rows` is a hard cap on how much one write carries, however far
    # behind the consumer is — the loop reads with it as the LIMIT every time.
    # It is the *log's* setting, so this is the log's default.
    write_sizes = collect_write_sizes(ref)

    assert write_sizes != []
    assert Enum.max(write_sizes) <= DMEx.Config.log!([]).max_batch_rows
  end

  # The log's memory bound, over a real socket. `max_batch_rows` alone is
  # not one: 500 rows is half a megabyte at 1 KB each and half a gigabyte at
  # 1 MB each, and the difference is entirely the payloads a publisher happens
  # to send.
  test "a log of large payloads is read in byte-bounded batches, not row-bounded ones", _ctx do
    ref = :telemetry_test.attach_event_handlers(self(), [[:dmex, :stream, :write]])

    # 8 KB each, so the default 500-row limit would be a 4 MB read and the
    # 1 MiB byte budget is what has to bind instead.
    seed_large(200, 8_192)

    {:ok, _} =
      DMEx.Supervisor.subscribe(Handlers.Echo, supervisor: TestSupervisor, last_seen: 0)

    # Drain the small backlog and then the large rows behind it.
    for _ <- 1..(@backlog + 200), do: assert_receive({:handled, %OrderPlaced{}, _}, 30_000)

    writes = collect_writes(ref)
    budget = DMEx.Config.log!([]).max_batch_bytes
    rows_limit = DMEx.Config.log!([]).max_batch_rows

    # The claim, over every write the connection made: payload bytes never
    # exceed the budget. The slack is SSE framing, which the storage bound does
    # not count because framing is not storage's business.
    assert writes != []
    assert Enum.all?(writes, fn w -> w.bytes <= budget + w.rows * 200 end)

    # And the bound was not vacuous: at least one write stopped on bytes rather
    # than on the row limit, which is only possible because the *read* was
    # bounded — 500 rows of 8 KB would have been a 4 MB batch.
    assert Enum.any?(writes, fn w -> w.rows < rows_limit and w.bytes > div(budget, 2) end)
  end

  ## Helpers

  defp seed_large(count, payload_bytes) do
    note = String.duplicate("x", payload_bytes)

    SQL.query!(
      Repo,
      """
      INSERT INTO dmex_messages (type, data, metadata)
      SELECT $1, json_build_object('order_id', n, 'note', $3::text)::text::jsonb, '{}'::jsonb
        FROM generate_series(1, $2) AS n
      """,
      ["order_placed", count, note]
    )
  end

  # Seeded with one statement rather than @backlog appends: this test is about
  # the read path, and appends are deliberately serialized.
  defp seed(count) do
    SQL.query!(
      Repo,
      """
      INSERT INTO dmex_messages (type, data, metadata)
      SELECT $1, json_build_object('order_id', n)::text::jsonb, '{}'::jsonb
        FROM generate_series(1, $2) AS n
      """,
      ["order_placed", count]
    )
  end

  defp collect_write_sizes(ref, acc \\ []) do
    receive do
      {[:dmex, :stream, :write], ^ref, %{rows: rows}, _} -> collect_write_sizes(ref, [rows | acc])
    after
      0 -> acc
    end
  end

  defp collect_writes(ref, acc \\ []) do
    receive do
      {[:dmex, :stream, :write], ^ref, measurements, _} ->
        collect_writes(ref, [measurements | acc])
    after
      100 -> Enum.reverse(acc)
    end
  end
end
