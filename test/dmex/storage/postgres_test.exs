defmodule DMEx.Storage.PostgresTest do
  use DMEx.Case, async: true, database: true

  alias Ecto.Adapters.SQL

  alias DMEx.Storage.Postgres

  # Declared subscription identifiers, the shape DMEx.Consumer.Handler renders.
  @some "orders/some"
  @one "orders/one"
  @two "billing/two"

  setup do
    %{opts: %{repo: Repo, lock: {1_701, 4_242}, channel: "dmex_storage_test", prefix: nil}}
  end

  describe "append/4" do
    test "returns the assigned id, increasing", %{opts: opts} do
      assert {:ok, first} = append(opts, "A", %{n: 1})
      assert {:ok, second} = append(opts, "A", %{n: 2})

      assert is_integer(first)
      assert second > first
    end

    test "passes metadata through untouched", %{opts: opts} do
      {:ok, id} = append(opts, "A", %{n: 1}, %{trace_id: "abc", nested: %{k: [1, 2]}})

      assert %{metadata: metadata} = fetch!(opts, id)
      assert JSON.decode!(metadata) == %{"trace_id" => "abc", "nested" => %{"k" => [1, 2]}}
    end

    test "defaults metadata to an empty object rather than NULL", %{opts: opts} do
      {:ok, id} = append(opts, "A", %{n: 1})

      assert %{metadata: "{}"} = fetch!(opts, id)
    end
  end

  describe "read/3" do
    test "returns rows in ascending id order", %{opts: opts} do
      ids = for n <- 1..5, do: elem(append(opts, "A", %{n: n}), 1)

      assert {:ok, {rows, _max_id}} = Postgres.read(opts, 0, 100)
      assert Enum.map(rows, & &1.id) == Enum.sort(ids)
    end

    test "is exclusive on the cursor", %{opts: opts} do
      [_first, second, third] = for n <- 1..3, do: elem(append(opts, "A", %{n: n}), 1)

      assert {:ok, {rows, _}} = Postgres.read(opts, second, 100)
      assert Enum.map(rows, & &1.id) == [third]
    end

    test "honours the limit", %{opts: opts} do
      for n <- 1..10, do: append(opts, "A", %{n: n})

      assert {:ok, {rows, _}} = Postgres.read(opts, 0, 4)
      assert length(rows) == 4
    end

    test "returns every field DMEx.SSE.Frame needs, and nothing else", %{opts: opts} do
      {:ok, id} = append(opts, "MyApp.Messages.OrderPlaced", %{order_id: 7})

      assert {:ok, {[row], _}} = Postgres.read(opts, id - 1, 1)

      assert Map.keys(row) |> Enum.sort() ==
               [:data, :id, :inserted_at, :message_type, :metadata]

      assert row.id == id
      assert row.message_type == "MyApp.Messages.OrderPlaced"
    end

    # This is the splice invariant. DMEx.SSE.Frame.encode_batch/2 copies these two
    # columns into the wire frame byte-for-byte without parsing them, so an
    # adapter that let its driver decode jsonb into a map would hand Frame a map
    # where it expects text and build a wrong frame.
    test "returns data and metadata as JSON text, never as decoded maps", %{opts: opts} do
      {:ok, id} = append(opts, "A", %{order_id: 7, total: 1999}, %{trace_id: "abc"})

      assert {:ok, {[row], _}} = Postgres.read(opts, id - 1, 1)

      assert is_binary(row.data)
      assert is_binary(row.metadata)
      assert JSON.decode!(row.data) == %{"order_id" => 7, "total" => 1999}
      assert JSON.decode!(row.metadata) == %{"trace_id" => "abc"}
    end

    test "returns inserted_at as a DateTime, so the wire keeps its UTC suffix", %{opts: opts} do
      {:ok, id} = append(opts, "A", %{n: 1})

      assert {:ok, {[row], _}} = Postgres.read(opts, id - 1, 1)

      # timestamptz -> %DateTime{}; a plain `timestamp` column would come back
      # as %NaiveDateTime{} and silently drop the trailing Z from every frame.
      assert %DateTime{} = row.inserted_at
      assert row.inserted_at |> DateTime.to_iso8601() |> String.ends_with?("Z")
    end

    test "carries max_id on the same round trip", %{opts: opts} do
      for n <- 1..5, do: append(opts, "A", %{n: n})
      {:ok, tip} = Postgres.max_id(opts)

      # Read only the first two rows: max_id must still report the log's tip,
      # not the highest id in the batch — that is the whole "am I behind" signal.
      assert {:ok, {rows, max_id}} = Postgres.read(opts, 0, 2)
      assert length(rows) == 2
      assert max_id == tip
      assert max_id > List.last(rows).id
    end

    test "returns no max_id when the read is empty", %{opts: opts} do
      # Legal: a zero-row read sends no frame, so there is nothing to stamp.
      assert {:ok, {[], nil}} = Postgres.read(opts, 0, 10)
    end
  end

  describe "JSON payload fidelity" do
    # Regression guard. Writing the parameter as `$n::jsonb` rather than
    # `$n::text::jsonb` makes Postgres describe it as jsonb, at which point
    # Postgrex encodes the already-encoded binary through its own JSON codec and
    # stores the *string* "{\"order_id\":7}" instead of the object. The insert
    # succeeds, the row is valid jsonb, and only a consumer decoding the payload
    # ever notices — so it has to be asserted structurally.
    test "payloads are stored as JSON objects, not as JSON strings", %{opts: opts} do
      {:ok, _} = append(opts, "A", %{order_id: 7}, %{trace_id: "abc"})

      assert [["object", "object"]] =
               raw(opts, "SELECT jsonb_typeof(data), jsonb_typeof(metadata) FROM dmex_messages")
    end

    # A literal newline inside `data` would terminate the SSE `data:` field and
    # split the frame around it. JSON's grammar forbids raw control characters
    # in strings, so this holds by construction of the format — but only if the
    # column really is JSON, which is what this asserts.
    test "control characters stay escaped, so a frame cannot be split", %{opts: opts} do
      {:ok, id} = append(opts, "A", %{note: "line one\nline two\r\ttabbed"})

      assert %{data: data} = fetch!(opts, id)

      refute String.contains?(data, "\n")
      refute String.contains?(data, "\r")
      assert JSON.decode!(data) == %{"note" => "line one\nline two\r\ttabbed"}
    end

    test "jsonb normalization drops duplicate keys but still yields valid JSON", %{opts: opts} do
      # Written as raw text rather than through JSON.encode!/1, which cannot
      # produce a duplicate key. This is the documented cost of the jsonb
      # column: stored bytes are not the publisher's bytes.
      {:ok, id} = Postgres.append(opts, "A", ~s({"a":1,"a":2}), "{}")

      assert %{data: data} = fetch!(opts, id)
      assert JSON.decode!(data) == %{"a" => 2}
    end

    test "a unicode payload round-trips", %{opts: opts} do
      {:ok, id} = append(opts, "A", %{note: "naïve 日本語 🎉"})

      assert %{data: data} = fetch!(opts, id)
      assert JSON.decode!(data) == %{"note" => "naïve 日本語 🎉"}
    end
  end

  describe "max_id/1" do
    test "is 0 on an empty log", %{opts: opts} do
      assert {:ok, 0} = Postgres.max_id(opts)
    end

    test "tracks the highest id", %{opts: opts} do
      {:ok, id} = append(opts, "A", %{n: 1})

      assert {:ok, ^id} = Postgres.max_id(opts)
    end
  end

  describe "last_seen/2 and advance/4" do
    # nil, not 0. That distinction is what makes `start: :latest` meaningful
    # exactly once: a subscription with no cursor resolves against the tip, and
    # one with a cursor — even at 0 — ignores :start entirely.
    test "is nil before anything has committed", %{opts: opts} do
      assert {:ok, nil} = Postgres.last_seen(opts, @some)
    end

    test "advance/4 sets the cursor and last_seen/2 reads it back", %{opts: opts} do
      assert :ok = Postgres.advance(opts, @some, 42)

      assert {:ok, 42} = Postgres.last_seen(opts, @some)
    end

    test "advance/4 upserts rather than duplicating", %{opts: opts} do
      :ok = Postgres.advance(opts, @some, 42)
      :ok = Postgres.advance(opts, @some, 43)

      assert {:ok, 43} = Postgres.last_seen(opts, @some)
      assert count(opts, "dmex_cursors") == 1
    end

    test "cursors are keyed per subscription", %{opts: opts} do
      :ok = Postgres.advance(opts, @one, 10)
      :ok = Postgres.advance(opts, @two, 20)

      assert {:ok, 10} = Postgres.last_seen(opts, @one)
      assert {:ok, 20} = Postgres.last_seen(opts, @two)
    end

    # Duplicate suppression happens in the pipeline, which is the authority, and
    # an operator resetting a cursor deliberately needs this to go both ways.
    test "advance/4 does not refuse to move backwards", %{opts: opts} do
      :ok = Postgres.advance(opts, @some, 100)
      :ok = Postgres.advance(opts, @some, 5)

      assert {:ok, 5} = Postgres.last_seen(opts, @some)
    end

    test "stores the identifier verbatim, with no inspect/1 in the way", %{opts: opts} do
      # The id arrives as a string and goes straight into the query — there is
      # nothing left to render, and nothing that could ever be atomized back.
      :ok = Postgres.advance(opts, @some, 1)

      # inspect/1, not Atom.to_string/1: the stored key is the readable module
      # name an operator would recognise, and nothing ever converts it back.
      assert [[@some]] = raw(opts, "SELECT subscription FROM dmex_cursors")
    end
  end

  # These two are why nothing outside this module knows how Postgres delivers a
  # wakeup. Before them, DMEx.Producer compared the storage module against this
  # one by name and carried a list of Postgrex connection options.
  describe "child_specs/1" do
    test "supplies a notification connection named after the producer", %{opts: opts} do
      opts = Map.put(opts, :producer, DMEx.Test.Producer)

      assert [%{id: id, start: {Postgrex.Notifications, :start_link, [conn_opts]}}] =
               Postgres.child_specs(opts)

      assert id == DMEx.Test.Producer.Notifications
      assert conn_opts[:name] == DMEx.Test.Producer.Notifications
      assert conn_opts[:auto_reconnect] == true
      assert conn_opts[:database] == "dmex_test"
    end

    test "passes Postgrex only keys it understands", %{opts: opts} do
      # repo.config/0 carries Ecto's own settings — a pool module, a telemetry
      # prefix — and handing those to Postgrex would be a connection that fails
      # to start rather than one that runs without notifications.
      [%{start: {_, _, [conn_opts]}}] =
        Postgres.child_specs(Map.put(opts, :producer, DMEx.Test.Producer))

      for key <- [:pool, :pool_size, :telemetry_prefix, :otp_app, :log, :scheme, :url] do
        refute Keyword.has_key?(conn_opts, key), "#{inspect(key)} leaked into Postgrex opts"
      end
    end
  end

  describe "listen/1" do
    # The behaviour promises best-effort: polling is what makes dispatch robust,
    # so a missing notifier must be an error value rather than an exit that
    # takes the dispatcher down with it.
    test "reports a notifier that is not running instead of exiting", %{opts: opts} do
      opts = Map.put(opts, :producer, DMEx.Test.Producer)

      assert {:error, :not_running} = Postgres.listen(opts)
    end
  end

  describe "dead_message/2" do
    test "stores the log row verbatim, plus whose row it is", %{opts: opts} do
      assert :ok = Postgres.dead_message(opts, dead_attrs())

      assert [[subscription, id, type, data, metadata, inserted_at]] =
               raw(opts, """
               SELECT subscription, id, message_type,
                      data::text, metadata::text, inserted_at
                 FROM dmex_dead_messages
               """)

      assert subscription == @some
      assert id == 1042
      assert type == "MyApp.Messages.OrderPlaced"
      assert JSON.decode!(data) == %{"order_id" => 7}
      assert JSON.decode!(metadata) == %{"trace_id" => "abc"}
      assert %DateTime{} = inserted_at
    end

    test "two subscriptions may give up on the same producer row", %{opts: opts} do
      assert :ok = Postgres.dead_message(opts, dead_attrs(subscription: @one))
      assert :ok = Postgres.dead_message(opts, dead_attrs(subscription: @two))

      assert count(opts, "dmex_dead_messages") == 2
    end

    test "one subscription cannot record the same message twice", %{opts: opts} do
      assert :ok = Postgres.dead_message(opts, dead_attrs())

      assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
               Postgres.dead_message(opts, dead_attrs())
    end
  end

  describe "dead_messages/2 and delete_dead_message/3" do
    # The point of the table's shape: what comes back is a log row, in exactly
    # the shape read/3 returns and the pipeline consumes.
    test "reads back rows in the same shape read/3 returns", %{opts: opts} do
      :ok = Postgres.dead_message(opts, dead_attrs())

      assert {:ok, [row]} = Postgres.dead_messages(opts, @some)

      assert Map.keys(row) |> Enum.sort() ==
               [:data, :id, :inserted_at, :message_type, :metadata]

      assert row.id == 1042
      assert is_binary(row.data)
      assert is_binary(row.metadata)
    end

    test "returns only the given subscription's messages, oldest first", %{opts: opts} do
      :ok = Postgres.dead_message(opts, dead_attrs(subscription: @one, id: 3))
      :ok = Postgres.dead_message(opts, dead_attrs(subscription: @one, id: 1))
      :ok = Postgres.dead_message(opts, dead_attrs(subscription: @two, id: 2))

      assert {:ok, rows} = Postgres.dead_messages(opts, @one)
      assert Enum.map(rows, & &1.id) == [1, 3]
    end

    test "is empty for a subscription that has given up on nothing", %{opts: opts} do
      assert {:ok, []} = Postgres.dead_messages(opts, @some)
    end

    test "delete removes one message by subscription and log id", %{opts: opts} do
      :ok = Postgres.dead_message(opts, dead_attrs(id: 1))
      :ok = Postgres.dead_message(opts, dead_attrs(id: 2))

      assert :ok = Postgres.delete_dead_message(opts, @some, 1)

      assert {:ok, [row]} = Postgres.dead_messages(opts, @some)
      assert row.id == 2
    end

    test "deleting something absent is fine", %{opts: opts} do
      assert :ok = Postgres.delete_dead_message(opts, @some, 999)
    end
  end

  ## Helpers

  defp append(opts, type, data, metadata \\ %{}) do
    Postgres.append(opts, type, JSON.encode!(data), JSON.encode!(metadata))
  end

  defp fetch!(opts, id) do
    {:ok, {[row], _}} = Postgres.read(opts, id - 1, 1)
    row
  end

  defp raw(%{repo: repo}, sql), do: SQL.query!(repo, sql, []).rows

  defp count(opts, table) do
    [[count]] = raw(opts, "SELECT count(*) FROM #{table}")
    count
  end

  defp dead_attrs(overrides \\ []) do
    %{
      subscription: Keyword.get(overrides, :subscription, @some),
      id: Keyword.get(overrides, :id, 1042),
      message_type: "MyApp.Messages.OrderPlaced",
      data: ~s({"order_id":7}),
      metadata: ~s({"trace_id":"abc"}),
      inserted_at: "2026-07-27T10:31:04.221Z"
    }
  end
end
