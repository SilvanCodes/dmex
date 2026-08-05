defmodule DMEx.ConfigTest do
  use DMEx.Case, async: true

  alias DMEx.Config

  describe "log!/1" do
    test "returns the documented defaults when given nothing" do
      assert %{
               max_batch_frequency: 100,
               max_batch_size: 500,
               heartbeat_ms: 15_000,
               poll_interval: 1_000,
               max_batch_bytes: 1_048_576,
               stall_timeout: 30_000,
               stall_check_interval: 1_000
             } = Config.log!([])
    end

    test "overrides only the settings it is given" do
      config = Config.log!(max_batch_frequency: 50, heartbeat_ms: 30_000)

      assert config.max_batch_frequency == 50
      assert config.heartbeat_ms == 30_000
      assert config.max_batch_size == Config.log!([]).max_batch_size
    end

    test "ignores keys that belong to the log module rather than its pacing" do
      # `use DMEx.Log` hands its whole option list over; rejecting typos is
      # its job, not this one's.
      assert %{} =
               Config.log!(repo: MyApp.Repo, storage: SomeAdapter, app: :thing, prefix: "x")
    end

    test "raises on a non-integer or non-positive setting" do
      assert_raise ArgumentError, ~r/:max_batch_frequency must be a positive number/, fn ->
        Config.log!(max_batch_frequency: "5")
      end

      assert_raise ArgumentError, ~r/:max_batch_size must be a positive integer/, fn ->
        Config.log!(max_batch_size: 0)
      end

      assert_raise ArgumentError, ~r/:heartbeat_ms must be a positive integer/, fn ->
        Config.log!(heartbeat_ms: -1)
      end

      assert_raise ArgumentError, ~r/:stall_check_interval must be a positive integer/, fn ->
        Config.log!(stall_check_interval: 0)
      end
    end

    # The one setting that may be switched off rather than tuned, since a
    # log whose streams legitimately block for longer than any budget worth
    # naming would rather have no watcher than a wrong one.
    test "takes :infinity for the stall timeout, and nothing else non-integer" do
      assert Config.log!(stall_timeout: :infinity).stall_timeout == :infinity
      assert Config.log!(stall_timeout: 1).stall_timeout == 1

      for value <- [0, -1, :never, "30000"] do
        assert_raise ArgumentError,
                     ~r/:stall_timeout must be a positive integer or :infinity/,
                     fn ->
                       Config.log!(stall_timeout: value)
                     end
      end
    end

    # There are no floors and no ceilings any more, because there is nothing to
    # clamp: pacing is log configuration and no consumer ever asks for a
    # different value. A log that wants a 10 ms window may simply have one.
    test "takes any positive pacing value, with nothing to clamp it against" do
      assert Config.log!(max_batch_frequency: 200).max_batch_frequency == 200
      assert Config.log!(max_batch_size: 9_000).max_batch_size == 9_000
    end
  end

  describe "handler!/1" do
    test "returns the documented defaults when given nothing" do
      assert %{
               last_seen: nil,
               on_failure: :dead_message,
               max_attempts: 3,
               retry_backoff: 100,
               retry_backoff_max: 5_000,
               reconnect_backoff: 500,
               reconnect_backoff_max: 30_000,
               max_frame_bytes: 4_194_304,
               missed_heartbeats: 3
             } = Config.handler!([])
    end

    # Every pacing setting is the log's now, so a subscription simply has
    # no field for one — and `consumer!/1` ignores what it does not know, the
    # way it ignores `:repo`.
    test "carries no pacing settings at all" do
      refute Map.has_key?(Config.handler!([]), :max_batch_frequency)
      refute Map.has_key?(Config.handler!([]), :max_batch_size)
    end

    test "ignores keys that belong to the subscription rather than its pacing" do
      assert %{} =
               Config.handler!(
                 supervisor: MyApp.Supervisor,
                 log: %{orders: "https://example.test/stream"},
                 repo: MyApp.Repo
               )
    end

    test "accepts every legal :last_seen value" do
      assert Config.handler!(last_seen: nil).last_seen == nil
      assert Config.handler!(last_seen: 1042).last_seen == 1042
    end

    # A cursor, not a starting id: 0 is a position rather than a mistake, and it
    # is the whole log.
    test "accepts :last_seen 0, which is the whole log" do
      assert Config.handler!(last_seen: 0).last_seen == 0
    end

    # A position is one cursor or nothing at all — no atoms, no timestamps.
    test "raises on an illegal :last_seen" do
      for last_seen <- [
            :current,
            :beginning,
            :latest,
            -1,
            "42",
            "2026-08-03T10:00:00Z",
            ~U[2026-08-03 10:00:00Z]
          ] do
        assert_raise ArgumentError, ~r/:last_seen must be a non-negative/, fn ->
          Config.handler!(last_seen: last_seen)
        end
      end
    end

    test "raises on an illegal :on_failure" do
      assert_raise ArgumentError, ~r/:on_failure must be one of/, fn ->
        Config.handler!(on_failure: :retry_forever)
      end
    end
  end

  # `max_batch_frequency` is stated as a rate because a rate is what an operator
  # chooses — queries per second against their database — and the loop needs a
  # period. This is the one place the reciprocal is taken.
  describe "batch_interval/1" do
    test "is the reciprocal of the frequency, in ms" do
      assert [max_batch_frequency: 5] |> Config.log!() |> Config.batch_interval() == 200
      assert [max_batch_frequency: 1] |> Config.log!() |> Config.batch_interval() == 1_000
      assert [max_batch_frequency: 100] |> Config.log!() |> Config.batch_interval() == 10
    end

    # The slow end has no integer spelling as a frequency, which is why this is
    # the one setting that accepts a fraction.
    test "a fractional frequency is a long interval" do
      assert [max_batch_frequency: 0.2] |> Config.log!() |> Config.batch_interval() == 5_000
      assert [max_batch_frequency: 0.5] |> Config.log!() |> Config.batch_interval() == 2_000
    end

    test "rounds to whole milliseconds, which is all Process.sleep/1 resolves" do
      assert [max_batch_frequency: 3] |> Config.log!() |> Config.batch_interval() == 333
      assert [max_batch_frequency: 1_000] |> Config.log!() |> Config.batch_interval() == 1

      # Past the point where a period is expressible, the throttle stops
      # throttling rather than pretending to.
      assert [max_batch_frequency: 5_000] |> Config.log!() |> Config.batch_interval() == 0
    end
  end

  describe "effective_heartbeat/1" do
    test "is the sum, because the throttle runs after the heartbeat branch" do
      # The loop sleeps out one batch interval *after* deciding whether to send
      # a heartbeat, so the sum — not the interval — is the real upper bound on
      # silence, and that is what a consumer measures against.
      config = Config.log!(heartbeat_ms: 15_000, max_batch_frequency: 5)

      assert Config.effective_heartbeat(config) == 15_200
    end
  end

  describe "documentation" do
    # The tables in DMEx.Config's @moduledoc are the user-facing statement of
    # what DMEx does by default. Nothing stops them drifting from the structs
    # except this test.
    setup do
      {:docs_v1, _, _, _, %{"en" => moduledoc}, _, _} = Code.fetch_docs(Config)
      %{moduledoc: moduledoc}
    end

    test "the log table matches the struct exactly", %{moduledoc: moduledoc} do
      assert documented_defaults(moduledoc, "Log defaults") ==
               Map.new(Config.defaults().log)
    end

    test "the consumer table matches the struct exactly", %{moduledoc: moduledoc} do
      assert documented_defaults(moduledoc, "Handler defaults") ==
               Map.new(Config.defaults().handler)
    end
  end

  describe "settings" do
    # A setting is spelled three times: the defaults keyword, the `@type`, and
    # the validator. Added to the defaults alone it gets a default, a
    # `*_keys/0` entry and acceptance by `cast!/2` for free — so the other two
    # spellings are the ones that go missing, both of them silently.
    test "every log setting is validated" do
      assert unvalidated(Config.log_keys(), &Config.log!/1) == []
    end

    test "every handler setting is validated" do
      assert unvalidated(Config.handler_keys(), &Config.handler!/1) == []
    end

    test "the log type names exactly the log settings" do
      assert type_keys(:log) == Enum.sort(Config.log_keys())
    end

    test "the handler type names exactly the handler settings" do
      assert type_keys(:handler) == Enum.sort(Config.handler_keys())
    end
  end

  # A reference is outside every setting's domain, so a key that accepts one is
  # a key nothing validates.
  defp unvalidated(keys, build) do
    Enum.reject(keys, fn key ->
      try do
        build.([{key, make_ref()}])
        false
      rescue
        ArgumentError -> true
      end
    end)
  end

  # The field names of `@type log` / `@type handler`, read back out of the BEAM.
  defp type_keys(name) do
    {:ok, types} = Code.Typespec.fetch_types(Config)

    fields =
      Enum.find_value(types, fn
        {:type, {^name, {:type, _, :map, fields}, []}} -> fields
        _other -> nil
      end)

    fields
    |> Enum.map(fn {:type, _, :map_field_exact, [{:atom, _, key}, _]} -> key end)
    |> Enum.sort()
  end

  # Pulls `| `setting` | `value` | why |` rows out of one `## <heading>` section
  # and evaluates each cell back into the term it denotes.
  defp documented_defaults(moduledoc, heading) do
    moduledoc
    |> String.split(~r/^## /m)
    |> Enum.find(&String.starts_with?(&1, heading))
    |> String.split("\n")
    |> Enum.flat_map(fn line ->
      case Regex.run(~r/^\|\s*`(\w+)`\s*\|\s*`([^`]+)`\s*\|/, line) do
        [_, key, value] -> [{String.to_existing_atom(key), eval!(value)}]
        nil -> []
      end
    end)
    |> Map.new()
  end

  defp eval!(source) do
    {term, _binding} = Code.eval_string(source)
    term
  end
end
