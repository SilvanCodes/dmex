defmodule DMEx.ConfigTest do
  use DMEx.Case, async: true

  alias DMEx.Config
  alias DMEx.Config.Consumer
  alias DMEx.Config.Producer

  describe "producer!/1" do
    test "returns the documented defaults when given nothing" do
      assert %Producer{
               batch_window: 200,
               max_batch_size: 500,
               batch_window_floor: 50,
               batch_window_ceiling: 5_000,
               max_batch_size_floor: 1,
               max_batch_size_ceiling: 5_000,
               heartbeat_ms: 15_000,
               poll_interval: 1_000,
               frame_max_bytes: 1_048_576
             } = Config.producer!([])
    end

    test "overrides only the settings it is given" do
      config = Config.producer!(batch_window: 500, heartbeat_ms: 30_000)

      assert config.batch_window == 500
      assert config.heartbeat_ms == 30_000
      assert config.max_batch_size == %Producer{}.max_batch_size
    end

    test "ignores keys that belong to the producer module rather than its pacing" do
      # `use DMEx.Producer` hands its whole option list over; rejecting typos is
      # its job, not this one's.
      assert %Producer{} =
               Config.producer!(repo: MyApp.Repo, storage: SomeAdapter, app: :thing, prefix: "x")
    end

    test "raises on a non-integer or non-positive setting" do
      assert_raise ArgumentError, ~r/:batch_window must be a positive integer/, fn ->
        Config.producer!(batch_window: "200")
      end

      assert_raise ArgumentError, ~r/:max_batch_size must be a positive integer/, fn ->
        Config.producer!(max_batch_size: 0)
      end

      assert_raise ArgumentError, ~r/:heartbeat_ms must be a positive integer/, fn ->
        Config.producer!(heartbeat_ms: -1)
      end
    end

    test "raises when a floor exceeds its ceiling" do
      assert_raise ArgumentError, ~r/:batch_window_floor \(900\) must not exceed/, fn ->
        Config.producer!(batch_window_floor: 900, batch_window_ceiling: 800)
      end

      assert_raise ArgumentError, ~r/:max_batch_size_floor \(50\) must not exceed/, fn ->
        Config.producer!(max_batch_size_floor: 50, max_batch_size_ceiling: 10)
      end
    end

    test "raises when the configured default lies outside its own bounds" do
      # An unreachable default is a configuration mistake: every connection that
      # did not name a value would be clamped away from it on arrival.
      assert_raise ArgumentError, ~r/:batch_window \(10\) must lie within/, fn ->
        Config.producer!(batch_window: 10)
      end

      assert_raise ArgumentError, ~r/:max_batch_size \(9_?000\) must lie within/, fn ->
        Config.producer!(max_batch_size: 9000)
      end
    end
  end

  describe "consumer!/1" do
    test "returns the documented defaults when given nothing" do
      assert %Consumer{
               batch_window: 200,
               max_batch_size: 500,
               queue_limit: nil,
               start: :beginning,
               on_failure: :crash,
               max_attempts: 3,
               retry_backoff: 100,
               retry_backoff_max: 5_000,
               reconnect_backoff: 500,
               reconnect_backoff_max: 30_000,
               max_frame_bytes: 4_194_304,
               missed_heartbeats: 3,
               adaptive: true
             } = Config.consumer!([])
    end

    test "ignores keys that belong to the subscription rather than its pacing" do
      assert %Consumer{} =
               Config.consumer!(
                 consumer: MyApp.Consumer,
                 producer: %{orders: "https://example.test/stream"},
                 repo: MyApp.Repo
               )
    end

    test "accepts every legal :start value" do
      assert Config.consumer!(start: :beginning).start == :beginning
      assert Config.consumer!(start: :latest).start == :latest
      assert Config.consumer!(start: 0).start == 0
      assert Config.consumer!(start: 1042).start == 1042
    end

    test "raises on an illegal :start" do
      for start <- [:tip, -1, "42", nil] do
        assert_raise ArgumentError, ~r/:start must be :beginning, :latest/, fn ->
          Config.consumer!(start: start)
        end
      end
    end

    test "raises on an illegal :on_failure" do
      assert_raise ArgumentError, ~r/:on_failure must be one of/, fn ->
        Config.consumer!(on_failure: :retry_forever)
      end
    end

    test "raises on a non-boolean :adaptive" do
      assert_raise ArgumentError, ~r/:adaptive must be one of/, fn ->
        Config.consumer!(adaptive: :yes)
      end
    end

    test "allows queue_limit to be nil but not zero" do
      assert Config.consumer!(queue_limit: nil).queue_limit == nil
      assert Config.consumer!(queue_limit: 10).queue_limit == 10

      assert_raise ArgumentError, ~r/:queue_limit must be a positive integer/, fn ->
        Config.consumer!(queue_limit: 0)
      end
    end
  end

  describe "clamp/3" do
    setup do
      %{config: Config.producer!([])}
    end

    test "raises a value up to the floor and lowers it to the ceiling", %{config: config} do
      assert Config.clamp(:batch_window, 0, config) == config.batch_window_floor
      assert Config.clamp(:batch_window, 999_999, config) == config.batch_window_ceiling
      assert Config.clamp(:max_batch_size, 0, config) == config.max_batch_size_floor
      assert Config.clamp(:max_batch_size, 999_999, config) == config.max_batch_size_ceiling
    end

    test "leaves an in-range value alone", %{config: config} do
      assert Config.clamp(:batch_window, 200, config) == 200
      assert Config.clamp(:max_batch_size, 250, config) == 250
    end

    test "is idempotent", %{config: config} do
      for setting <- [:batch_window, :max_batch_size], value <- [0, 1, 200, 5_000, 999_999] do
        once = Config.clamp(setting, value, config)
        assert Config.clamp(setting, once, config) == once
      end
    end

    test "is monotone", %{config: config} do
      for setting <- [:batch_window, :max_batch_size] do
        clamped = Enum.map([0, 10, 100, 1_000, 10_000], &Config.clamp(setting, &1, config))
        assert clamped == Enum.sort(clamped)
      end
    end
  end

  describe "effective_heartbeat/2" do
    test "is the sum, because the throttle runs after the heartbeat branch" do
      # The loop sleeps out `batch_window` *after* deciding whether to send a
      # heartbeat, so the sum — not the interval — is the real upper bound on
      # silence, and that is what a consumer measures against.
      assert Config.effective_heartbeat(15_000, 200) == 15_200
    end
  end

  describe "queue_limit/1" do
    test "derives two frames' worth when unset" do
      assert Config.consumer!(max_batch_size: 500) |> Config.queue_limit() == 1_000
      assert Config.consumer!(max_batch_size: 40) |> Config.queue_limit() == 80
    end

    test "respects an explicit limit" do
      assert Config.consumer!(max_batch_size: 500, queue_limit: 1_500)
             |> Config.queue_limit() == 1_500
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

    test "the producer table matches the struct exactly", %{moduledoc: moduledoc} do
      assert documented_defaults(moduledoc, "Producer defaults") ==
               Map.new(Config.defaults().producer)
    end

    test "the consumer table matches the struct exactly", %{moduledoc: moduledoc} do
      assert documented_defaults(moduledoc, "Consumer defaults") ==
               Map.new(Config.defaults().consumer)
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
