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
               stall_check_interval: 1_000,
               metadata: []
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

    # Columns, not free-form keys: the point is to surface what the log already
    # stores, so a name it does not store is a mistake worth refusing.
    test "takes only liftable columns for :metadata" do
      assert Config.log!(metadata: [:id, :inserted_at]).metadata == [:id, :inserted_at]
      assert Config.log!(metadata: [:message_type]).metadata == [:message_type]

      for value <- [[:data], [:metadata], [:previous_id], [:id, :oops]] do
        assert_raise ArgumentError, ~r/:metadata may only lift/, fn ->
          Config.log!(metadata: value)
        end
      end

      assert_raise ArgumentError, ~r/:metadata must be a list of columns/, fn ->
        Config.log!(metadata: :id)
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
               start: :beginning,
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

    test "accepts every legal :start value" do
      assert Config.handler!(start: :beginning).start == :beginning
      assert Config.handler!(start: :latest).start == :latest
      assert Config.handler!(start: 0).start == 0
      assert Config.handler!(start: 1042).start == 1042
    end

    test "raises on an illegal :start" do
      for start <- [:tip, -1, "42", nil] do
        assert_raise ArgumentError, ~r/:start must be :beginning, :latest/, fn ->
          Config.handler!(start: start)
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
      assert Config.log!(max_batch_frequency: 5) |> Config.batch_interval() == 200
      assert Config.log!(max_batch_frequency: 1) |> Config.batch_interval() == 1_000
      assert Config.log!(max_batch_frequency: 100) |> Config.batch_interval() == 10
    end

    # The slow end has no integer spelling as a frequency, which is why this is
    # the one setting that accepts a fraction.
    test "a fractional frequency is a long interval" do
      assert Config.log!(max_batch_frequency: 0.2) |> Config.batch_interval() == 5_000
      assert Config.log!(max_batch_frequency: 0.5) |> Config.batch_interval() == 2_000
    end

    test "rounds to whole milliseconds, which is all Process.sleep/1 resolves" do
      assert Config.log!(max_batch_frequency: 3) |> Config.batch_interval() == 333
      assert Config.log!(max_batch_frequency: 1_000) |> Config.batch_interval() == 1

      # Past the point where a period is expressible, the throttle stops
      # throttling rather than pretending to.
      assert Config.log!(max_batch_frequency: 5_000) |> Config.batch_interval() == 0
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
