defmodule DMEx.Consumer.Stream do
  @moduledoc """
  One subscription: one outbound SSE connection, one handler, one cursor.

  Owns a Mint connection to a producer's `/stream`, decodes the frames off it,
  and runs each row through `DMEx.Consumer.Pipeline` in order, one at a time.

  ## Resumption is entirely cursor-driven

  On every connect — first or thousandth — the stream reads its durable
  `last_seen` and sends it as `Last-Event-ID`. It never resumes from the last
  frame's `id:`, and it never trusts anything it holds in memory. That is what
  makes a dropped connection a non-event: nothing is lost and nothing is
  skipped, because the producer holds no consumer state and resumption is the
  same `id > cursor` read a cold connect does.

  `start:` only applies when there is no cursor at all. Once one exists it is
  ignored entirely, because the durable position is always the authority.

  ## Reconnects are jittered, and that matters more than it looks

  Backoff lives here rather than in the supervisor, so a subscription that
  crash-loops on a poison message does so at backoff pace instead of as fast as
  a `DynamicSupervisor` will restart it. Each restart reopens the connection and
  triggers a fresh producer-side read — against a *different application's*
  database — so an un-backed-off crash-loop is something DMEx would be
  inflicting on someone else.

  ## A 4xx is fatal, with one exception; everything else is retried

  A `400` means this client sent something the producer could not parse, and
  reconnecting cannot fix it — so the process stops with a descriptive reason
  rather than hammering the producer with the same bad request. A `5xx`, a
  dropped socket, or a connect failure are all transient and get the backoff.

  A **`404` is retried**, and it is the one 4xx that is not this client's fault:
  it means the selection matched none of the producer's message types. The same
  request becomes valid the moment the producer deploys them, which is exactly
  the position a consumer shipped ahead of its producer is in — so stopping
  would turn an ordering choice into a permanently dead subscription. It is
  logged at warning level, because the other cause of a `404` is a subscription
  pointed at the wrong producer, and those look identical from here.

  ## Liveness

  A connection can die without either end noticing: the peer vanishes, a NAT
  rebinds, an intermediary drops the flow, and the socket simply never delivers
  anything again. Heartbeats are what turn that from "quiet" into "dead" — the
  producer advertises its effective interval as SSE's `retry:` field, and this
  process drops and reconnects after `missed_heartbeats` intervals of complete
  silence.

  Silence means *no bytes at all*, not no messages. A heartbeat comment is
  bytes, so a caught-up stream on an idle log stays healthy indefinitely.

  ## Skips are checkpointed on a timer, not per row

  A skipped row runs no handler, so `DMEx.Consumer.Pipeline` defers its cursor
  advance instead of committing one per row. This process owns settling that:
  a `checkpoint_interval` timer, armed for the process's whole life rather than
  per connection, plus a flush before each of the two points that re-read the
  durable cursor — a reconnect and `terminate/2`. Without those a run of skips
  would be re-read and re-skipped for nothing on the next connect.

  The `terminate/2` half only works because this process **traps exits**: a
  non-trapping child of a `DynamicSupervisor` is killed outright by
  `terminate_child/2`, and the callback never runs.

  ## A gap in the stream is detected, not stepped over

  Every frame states the id of the frame before it, and this process refuses one
  that does not claim the row it last saw. That closes a hole the cursor model
  cannot see on its own: a message that goes missing between two that arrive
  would otherwise have its id stepped straight over by the next row's advance,
  and the cursor is the only record of what was processed — so the loss would be
  permanent, silent, and indistinguishable from the message never having been
  published.

  The check runs **before** duplicate suppression, because suppression drops
  rows at or below the cursor and would otherwise hide the very gap this is
  looking for.

  A break drops the connection and reconnects from the durable cursor rather
  than crashing. If the gap was transport damage the re-read repairs it and
  nothing is lost; if it is a producer that genuinely skips rows, the same gap
  recurs and the subscription becomes a visible reconnect loop making no
  progress — which is the correct failure, and a loud one, where silently
  advancing was neither.

  ## Backpressure is the absence of a buffer

  Rows are run through the pipeline **inline, on the socket path**, one at a
  time, as they are decoded. There is no queue, no drain loop and no explicit
  socket gate, and that is what makes the backpressure exact rather than
  approximate.

  `Mint.HTTP.stream/2` re-arms `active: :once` itself, so while this process is
  inside a handler it is not calling `stream/2` — nothing is re-armed, the TCP
  receive window closes, and the producer's `chunk/2` blocks on it. The most
  that can be in flight is one decoded chunk in hand plus one socket message
  already in the mailbox. Nothing accumulates, so there is nothing to bound and
  no depth to observe.

  A buffer would only have relocated the problem: a few hundred rows of slack
  absorb nothing at a sustained mismatch, and every row of slack is one more row
  the socket keeps accepting before it says stop.
  """

  use GenServer

  alias DMEx.Backoff
  alias DMEx.Consumer.Pipeline
  alias DMEx.Consumer.Request
  alias DMEx.SSE.Decoder

  require Logger

  # Only used before a producer has advertised its own interval, which in
  # practice is the first chunk of the first response.
  @assumed_retry 60_000

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :subscription)},
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent
    }
  end

  @impl GenServer
  def init(opts) do
    handler = Keyword.fetch!(opts, :handler)
    subscription = Keyword.fetch!(opts, :subscription)
    url = Keyword.fetch!(opts, :url)
    config = Keyword.fetch!(opts, :config)

    # Without this the flush in `terminate/2` is dead code: a non-trapping child
    # of a `DynamicSupervisor` is killed outright by `terminate_child/2` and on
    # shutdown, so the callback never runs and a pending run of skips is lost.
    Process.flag(:trap_exit, true)

    # Registering here — before any socket opens — is what enforces one live
    # stream per subscription id: a second subscribe/2 for the same identity
    # fails in init rather than after both have started clobbering one cursor.
    case register(opts[:registry], subscription) do
      {:ok, _} ->
        state = %{
          uri: URI.parse(url),
          url: url,
          # Both, deliberately. The id is the durable identity everything is
          # keyed by; the module is what invokes the callback and what a human
          # reads in a log line.
          handler: handler,
          subscription: subscription,
          config: config,
          # Compiled into the handler by `DMEx.Consumer.Handler`, so this is a
          # constant lookup rather than a construction — and it is fixed for the
          # process's whole life, for the same reason pacing is fixed for a
          # connection's: a different selection is a different resource.
          fingerprint: Keyword.get(opts, :fingerprint),
          pipeline: Pipeline.build(opts),
          storage: Keyword.fetch!(opts, :storage),
          storage_opts: Keyword.fetch!(opts, :storage_opts),
          conn: nil,
          ref: nil,
          last_activity: nil,
          liveness_timer: nil,
          decoder: Decoder.new(config.max_frame_bytes),
          cursor: nil,
          # The id the next frame must claim as its predecessor. `:unknown`
          # until the first frame of a `start: :latest` connection, which is the
          # one case where the consumer cannot know what its own resume token
          # resolved to.
          expected_previous: :unknown,
          checkpoint_timer: nil,
          attempt: 0
        }

        # Armed once for the process's whole life rather than per connection: a
        # pending run of skips outlives a disconnect.
        {:ok, schedule_checkpoint(state), {:continue, :connect}}

      {:error, {:already_registered, _pid}} ->
        {:stop, {:already_subscribed, subscription}}
    end
  end

  @impl GenServer
  def handle_continue(:connect, state), do: connect(state)

  @impl GenServer
  def handle_info(:connect, state), do: connect(state)

  @impl GenServer
  def handle_info(:checkpoint, state) do
    {:noreply, state |> flush() |> schedule_checkpoint()}
  end

  @impl GenServer
  def handle_info(:liveness, %{conn: nil} = state), do: {:noreply, state}

  @impl GenServer
  def handle_info(:liveness, state) do
    silent_for = System.monotonic_time(:millisecond) - state.last_activity

    if silent_for > deadline(state) do
      # The socket never closed and nothing ever arrived — the shape a vanished
      # peer takes. Dropping costs a re-read from the durable cursor.
      {:noreply, reconnect(state, {:silent_for, silent_for})}
    else
      {:noreply, schedule_liveness(state)}
    end
  end

  @impl GenServer
  def handle_info(message, %{conn: conn} = state) when not is_nil(conn) do
    case Mint.HTTP.stream(conn, message) do
      :unknown ->
        {:noreply, state}

      {:ok, conn, responses} ->
        handle_responses(responses, %{state | conn: conn})

      {:error, conn, reason, _responses} ->
        {:noreply, reconnect(%{state | conn: conn}, reason)}
    end
  end

  @impl GenServer
  def handle_info(_message, state), do: {:noreply, state}

  # A clean stop — an `unsubscribe/2`, a supervisor shutdown — should not throw
  # away skips already made. Rescued because the repo's pool may already be on
  # its way down, and re-skipping is the ordinary cost of a pending run rather
  # than something worth a noisy shutdown crash.
  @impl GenServer
  def terminate(_reason, state) do
    try do
      flush(state)
    rescue
      _ -> :ok
    end

    if state.conn, do: Mint.HTTP.close(state.conn)

    :ok
  end

  ## Connecting

  defp connect(state) do
    with {:ok, cursor} <- durable_cursor(state),
         {:ok, conn} <- open(state),
         {:ok, conn, ref} <- request(conn, state, cursor) do
      {:noreply,
       %{
         state
         | conn: conn,
           ref: ref,
           cursor: cursor,
           # The durable cursor just read is the authority, so anything still
           # marked pending was either already written or is about to be
           # re-delivered and re-skipped. Either way it is not owed a write.
           pipeline: %{state.pipeline | pending: false},
           decoder: Decoder.new(state.config.max_frame_bytes),
           # The producer's first frame links back to the position it was asked
           # to resume after, so this also proves it honoured the request. With
           # `latest` there is nothing to compare against: the tip is resolved
           # producer-side and this end never learns the number.
           expected_previous: expected_previous(state, cursor),
           attempt: 0
       }
       |> touch()
       |> schedule_liveness()}
    else
      {:error, reason} -> {:noreply, reconnect(state, reason)}
      {:error, _conn, reason} -> {:noreply, reconnect(state, reason)}
    end
  end

  defp open(%{uri: uri}) do
    Mint.HTTP.connect(Request.scheme(uri), uri.host, uri.port, mode: :active)
  end

  defp request(conn, state, cursor) do
    headers = Request.headers(cursor, state.config)

    Mint.HTTP.request(conn, "GET", Request.path(state.uri, state.fingerprint), headers, nil)
  end

  # `:none` rather than 0 for "never committed anything" — that distinction is
  # what makes `start: :latest` meaningful exactly once.
  defp durable_cursor(state) do
    case state.storage.last_seen(state.storage_opts, state.subscription) do
      {:ok, nil} -> {:ok, :none}
      {:ok, cursor} -> {:ok, cursor}
      {:error, reason} -> {:error, reason}
    end
  end

  ## Responses

  defp handle_responses([], state), do: {:noreply, state}

  defp handle_responses([{:status, ref, status} | rest], %{ref: ref} = state) do
    cond do
      status in 200..299 ->
        handle_responses(rest, state)

      # The one 4xx that is not this client's fault, and the one that can become
      # valid without anything here changing: the selection matched none of the
      # producer's message types. A consumer deployed ahead of the producer that
      # will emit them is legitimately in this state, and the repair is the
      # producer's deploy, so it retries. Loud, because the other cause — a
      # subscription pointed at the wrong producer entirely — looks identical
      # and is a real bug.
      status == 404 ->
        Logger.warning(
          "DMEx #{state.subscription} (#{inspect(state.handler)}) got 404 from " <>
            "#{state.url}: its message types match nothing this producer knows. Either the " <>
            "producer has not deployed them yet, or this subscription is pointed at the " <>
            "wrong producer. Retrying."
        )

        {:noreply, reconnect(state, {:no_matching_types, state.url})}

      status in 400..499 ->
        # This client sent something the producer could not parse. Reconnecting
        # would send it again, so stop loudly instead.
        {:stop, {:bad_request, status, state.url}, state}

      true ->
        {:noreply, reconnect(state, {:http_status, status})}
    end
  end

  defp handle_responses([{:data, ref, chunk} | rest], %{ref: ref} = state) do
    # Any bytes at all, including a heartbeat comment, mean the connection is
    # alive. Silence is what this measures, not idleness.
    state = touch(state)
    previous = Decoder.retry(state.decoder)

    case Decoder.push(state.decoder, chunk) do
      {:ok, rows, decoder} ->
        state = adopt_interval(%{state | decoder: decoder}, previous)

        report_lag(rows, state)

        case verify_chain(rows, state) do
          {:ok, state} -> handle_responses(rest, Enum.reduce(rows, state, &run/2))
          {:error, reason} -> {:noreply, reconnect(state, reason)}
        end

      {:error, reason, _decoder} ->
        # Dropping and reconnecting from the durable cursor costs only a
        # re-read; a drop is not a delivery event.
        {:noreply, reconnect(state, {:decode_failed, reason})}
    end
  end

  defp handle_responses([{:done, ref} | _rest], %{ref: ref} = state) do
    {:noreply, reconnect(state, :closed)}
  end

  defp handle_responses([{:error, ref, reason} | _rest], %{ref: ref} = state) do
    {:noreply, reconnect(state, reason)}
  end

  defp handle_responses([_other | rest], state), do: handle_responses(rest, state)

  # Inline, one row at a time, right here on the socket path. That *is* the
  # backpressure: while this runs the process is not calling `Mint.HTTP.stream/2`,
  # so the socket is not re-armed, the receive window closes, and the producer's
  # `chunk/2` blocks. Nothing buffers, so there is nothing to bound.
  defp run(row, state) do
    {:ok, pipeline} = Pipeline.run(row, %{state.pipeline | cursor: cursor_of(state)})

    %{state | pipeline: pipeline, cursor: pipeline.cursor}
  end

  defp cursor_of(%{cursor: :none}), do: 0
  defp cursor_of(%{cursor: cursor}), do: cursor

  ## The id chain

  # `start: :latest` resolves to the log's tip on the *producer*, and the
  # resolved number never comes back over the wire, so the first frame's claim
  # has nothing to be checked against. Every other resume position is one this
  # end chose and can therefore verify.
  defp expected_previous(%{config: %{start: :latest}}, :none), do: :unknown
  defp expected_previous(_state, :none), do: 0
  defp expected_previous(_state, cursor), do: cursor

  # Walked before anything is processed, so a break is caught while the rows are
  # still just rows. Returns the state with `expected_previous` advanced to the
  # last id in the run.
  defp verify_chain(rows, state) do
    Enum.reduce_while(rows, {:ok, state}, fn row, {:ok, state} ->
      case check_link(row, state.expected_previous) do
        :ok ->
          {:cont, {:ok, %{state | expected_previous: Map.fetch!(row, "id")}}}

        {:error, reason} ->
          log_gap(state, reason)

          {:halt, {:error, reason}}
      end
    end)
  end

  # A producer that does not chain at all is refused rather than trusted: the
  # whole point is that an unverifiable stream and a verified one must not look
  # the same from here.
  defp check_link(row, expected) do
    case Map.get(row, "previous_id") do
      nil -> {:error, {:unchained_frame, Map.fetch!(row, "id")}}
      _previous when expected == :unknown -> :ok
      ^expected -> :ok
      previous -> {:error, {:sequence_gap, expected, previous}}
    end
  end

  defp log_gap(state, {:sequence_gap, expected, got}) do
    Logger.error(
      "DMEx #{state.subscription} (#{inspect(state.handler)}) saw a gap in #{state.url}: the " <>
        "next frame claims #{got} as its predecessor, but the last row this subscription " <>
        "received was #{expected}. A message went missing between them. Dropping the " <>
        "connection and re-reading from the durable cursor; if this repeats, the producer is " <>
        "skipping rows rather than the transport losing them."
    )
  end

  defp log_gap(state, {:unchained_frame, id}) do
    Logger.error(
      "DMEx #{state.subscription} (#{inspect(state.handler)}) received frame #{id} from " <>
        "#{state.url} with no predecessor in its `id:` field. A stream whose frames cannot be " <>
        "linked cannot be checked for gaps, so it is refused rather than trusted."
    )
  end

  # How far behind the tip this consumer is, in ms rather than in ids. The wire
  # carries no `max_id` to subtract from, and `inserted_at` answers the question
  # the id difference was a proxy for anyway — "how stale is what I am handling".
  #
  # Once per decoder push rather than once per row: a push is one socket read's
  # worth of rows, so this samples at the rate bytes actually arrive instead of
  # emitting an event per message.
  #
  # It compares the producer's clock against this one, so it is only as good as
  # the two machines' NTP agreement. That is fine for a seconds-scale signal and
  # would not be for anything finer.
  defp report_lag([], _state), do: :ok

  defp report_lag(rows, state) do
    case rows |> List.last() |> Map.get("inserted_at") |> parse_inserted_at() do
      nil ->
        :ok

      at ->
        :telemetry.execute(
          [:dmex, :consumer, :lag],
          %{lag: DateTime.diff(DateTime.utc_now(), at, :millisecond), rows: length(rows)},
          %{subscription: state.subscription, handler: state.handler, url: state.url}
        )
    end
  end

  defp parse_inserted_at(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, at, _offset} -> at
      {:error, _reason} -> nil
    end
  end

  defp parse_inserted_at(_other), do: nil

  ## Reconnecting

  defp reconnect(state, reason) do
    # Before anything else: the next connect resumes from the *durable* cursor,
    # so an unwritten run of skips would be re-read and re-skipped for nothing.
    state = flush(state)

    if state.conn, do: Mint.HTTP.close(state.conn)
    if state.liveness_timer, do: Process.cancel_timer(state.liveness_timer)

    attempt = state.attempt + 1

    delay =
      Backoff.delay(attempt, state.config.reconnect_backoff, state.config.reconnect_backoff_max)

    Logger.debug(fn ->
      "DMEx #{state.subscription} (#{inspect(state.handler)}) reconnecting to " <>
        "#{state.url} in #{delay}ms " <>
        "after #{inspect(reason)}"
    end)

    :telemetry.execute(
      [:dmex, :consumer, :reconnect],
      %{attempt: attempt, delay: delay},
      %{subscription: state.subscription, handler: state.handler, url: state.url, reason: reason}
    )

    Process.send_after(self(), :connect, delay)

    %{state | conn: nil, ref: nil, liveness_timer: nil, attempt: attempt}
  end

  ## Checkpointing

  # Settles a coalesced run of skips. A no-op unless one is outstanding, so the
  # timer costs nothing on a stream whose rows are all handled.
  defp flush(state) do
    {:ok, pipeline} = Pipeline.flush(state.pipeline)

    %{state | pipeline: pipeline}
  end

  defp schedule_checkpoint(state) do
    timer = Process.send_after(self(), :checkpoint, state.config.checkpoint_interval)

    %{state | checkpoint_timer: timer}
  end

  ## Liveness

  defp touch(state), do: %{state | last_activity: System.monotonic_time(:millisecond)}

  # Checked twice per deadline, so a dead connection is noticed within about
  # half an interval of becoming provably dead rather than a whole one. The
  # floor keeps a producer with a very short interval from turning this into a
  # busy loop.
  defp schedule_liveness(state) do
    interval = max(div(deadline(state), 2), 250)

    %{state | liveness_timer: Process.send_after(self(), :liveness, interval)}
  end

  # The first timer is necessarily armed against `@assumed_retry`, because the
  # producer has not spoken yet — and at a minute that is far too slack to
  # notice anything. As soon as `retry:` arrives, re-arm against the real
  # figure. Without this the advertised interval would not take effect until the
  # first tick, which is the very tick it was supposed to size.
  defp adopt_interval(state, previous) do
    if Decoder.retry(state.decoder) == previous do
      state
    else
      if state.liveness_timer, do: Process.cancel_timer(state.liveness_timer)

      schedule_liveness(state)
    end
  end

  # The producer advertises `heartbeat_ms` plus one batch interval as `retry:`,
  # which is the real upper bound on silence — the throttle runs after the
  # heartbeat branch. Until that arrives, assume a generous interval rather than
  # risk dropping a healthy connection from a producer that paces itself slowly.
  defp deadline(state) do
    interval = Decoder.retry(state.decoder) || @assumed_retry

    state.config.missed_heartbeats * interval
  end

  defp register(nil, _subscription), do: {:ok, self()}

  defp register(registry, subscription) do
    Registry.register(registry, {:subscription, subscription}, nil)
  end
end
