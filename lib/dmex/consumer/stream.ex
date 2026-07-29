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

  ## A 4xx is fatal; everything else is retried

  A `400` means this client sent something the producer could not parse, and
  reconnecting cannot fix it — so the process stops with a descriptive reason
  rather than hammering the producer with the same bad request. A `5xx`, a
  dropped socket, or a connect failure are all transient and get the backoff.

  ## Liveness

  A connection can die without either end noticing: the peer vanishes, a NAT
  rebinds, an intermediary drops the flow, and the socket simply never delivers
  anything again. Heartbeats are what turn that from "quiet" into "dead" — the
  producer advertises its effective interval as SSE's `retry:` field, and this
  process drops and reconnects after `missed_heartbeats` intervals of complete
  silence.

  Silence means *no bytes at all*, not no messages. A heartbeat comment is
  bytes, so a caught-up stream on an idle log stays healthy indefinitely.

  ## Backpressure in Stage 1

  Rows are processed inline as frames arrive. `Mint.HTTP.stream/2` re-arms
  `active: :once` itself, so at most one further chunk lands in the mailbox
  while a frame is draining — real, if coarse, backpressure. The bounded queue
  and explicit `set_mode(:passive)` gate that turn this into a measurable drain
  rate are a later stage.
  """

  use GenServer

  alias DMEx.Backoff
  alias DMEx.Consumer.Control
  alias DMEx.Consumer.Meter
  alias DMEx.Consumer.Pipeline
  alias DMEx.Consumer.Queue
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
          pipeline: Pipeline.build(opts),
          storage: Keyword.fetch!(opts, :storage),
          storage_opts: Keyword.fetch!(opts, :storage_opts),
          conn: nil,
          ref: nil,
          last_activity: nil,
          liveness_timer: nil,
          decoder: Decoder.new(config.max_frame_bytes),
          cursor: nil,
          # What the producer granted, from its echo. Decisions are made against
          # this rather than against what was asked for.
          params: %{batch_window: config.batch_window, max_batch_size: config.max_batch_size},
          # What the next connect asks for. Moves under the control law; the
          # producer may still clamp it.
          requested: %{
            batch_window: config.batch_window,
            max_batch_size: config.max_batch_size
          },
          queue: Queue.new(DMEx.Config.queue_limit(config), config.max_batch_size),
          meter: Meter.new(config),
          control: Control.new(),
          socket_mode: :active,
          draining?: false,
          reparameterizing?: false,
          attempt: 0
        }

        {:ok, state, {:continue, :connect}}

      {:error, {:already_registered, _pid}} ->
        {:stop, {:already_subscribed, subscription}}
    end
  end

  @impl GenServer
  def handle_continue(:connect, state), do: connect(state)

  @impl GenServer
  def handle_info(:connect, state), do: connect(state)

  @impl GenServer
  def handle_info(:drain, state), do: {:noreply, drain(state)}

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

  @impl GenServer
  def terminate(_reason, %{conn: conn}) when not is_nil(conn) do
    Mint.HTTP.close(conn)

    :ok
  end

  def terminate(_reason, _state), do: :ok

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
           decoder: Decoder.new(state.config.max_frame_bytes),
           socket_mode: :active,
           reparameterizing?: false,
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
    headers = Request.headers(state.requested, cursor, state.config)

    Mint.HTTP.request(conn, "GET", Request.path(state.uri), headers, nil)
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

      status in 400..499 ->
        # This client sent something the producer could not parse. Reconnecting
        # would send it again, so stop loudly instead.
        {:stop, {:bad_request, status, state.url}, state}

      true ->
        {:noreply, reconnect(state, {:http_status, status})}
    end
  end

  defp handle_responses([{:headers, ref, headers} | rest], %{ref: ref} = state) do
    params = Request.adopt(state.params, headers)

    # The queue reserves room for one whole frame, and the frame size is
    # whatever the producer actually granted — not what was asked for.
    queue = Queue.resize(state.queue, params.max_batch_size)

    handle_responses(rest, %{state | params: params, queue: queue})
  end

  defp handle_responses([{:data, ref, chunk} | rest], %{ref: ref} = state) do
    # Any bytes at all, including a heartbeat comment, mean the connection is
    # alive. Silence is what this measures, not idleness.
    state = touch(state)
    advertised = Decoder.retry(state.decoder)

    case Decoder.push(state.decoder, chunk) do
      {:ok, frames, decoder} ->
        state = adopt_interval(%{state | decoder: decoder}, advertised)
        state = Enum.reduce(frames, state, &process_frame/2)

        handle_responses(rest, state |> observe() |> gate_socket() |> advance())

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

  # Admitted whole, in order, and *not* processed here. Decoding a row into a
  # struct on the socket path would put per-row work above the queue's bound —
  # unbounded by it, and invisible to the drain measurement even though it is
  # real cost the consumer pays for every row.
  defp process_frame(frame, state) do
    report_lag(frame, state)

    %{state | queue: Queue.push(state.queue, frame.messages)}
  end

  defp cursor_of(%{cursor: :none}), do: 0
  defp cursor_of(%{cursor: cursor}), do: cursor

  # `max_id` minus the last id in the frame is the "am I falling behind" signal,
  # and it is the only thing that outlives the frame.
  defp report_lag(%{metadata: %{"max_id" => max_id}, messages: [_ | _] = messages}, state) do
    last = messages |> List.last() |> Map.fetch!("id")

    :telemetry.execute(
      [:dmex, :consumer, :lag],
      %{
        lag: max_id - last,
        rows: length(messages),
        # The one signal `lag` alone cannot give: a consumer keeping up with
        # room to spare and one keeping up with none both report zero lag, and
        # they are the difference between healthy and one traffic increment
        # from falling behind.
        utilization: Meter.utilization(state.meter, now())
      },
      %{subscription: state.subscription, handler: state.handler, url: state.url}
    )
  end

  defp report_lag(_frame, _state), do: :ok

  ## Reconnecting

  defp reconnect(state, reason) do
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

  ## Draining

  # One row per message, deliberately. Draining the whole queue in a single pass
  # would starve the receive loop, and depth could then never rise above what a
  # single frame put there — which is exactly the signal the queue exists to
  # produce. Returning between rows lets a frame land mid-drain, and *that* is
  # what "the consumer did not finish the last frame before the next arrived"
  # looks like.
  defp drain(state) do
    started = System.monotonic_time(:microsecond)

    case Queue.pop(state.queue) do
      :empty ->
        advance(%{state | draining?: false})

      {:ok, row, queue} ->
        cursor = cursor_of(state)
        {:ok, cursor} = Pipeline.run(row, %{state.pipeline | cursor: cursor})

        elapsed = (System.monotonic_time(:microsecond) - started) / 1000

        %{state | queue: queue, cursor: cursor, draining?: false}
        |> sample(elapsed)
        |> observe()
        |> gate_socket()
        |> reparameterize()
        |> advance()
    end
  end

  # Only a pass that committed is a sample. A raising handler is retried inside
  # the pipeline with backoff, and those sleeps are wall-clock the stream spends
  # not draining — a failure mode, not a capacity. One poison message under
  # exponential backoff would otherwise drag the estimate down by orders of
  # magnitude and pin the batches at the floor long after `on_failure` resolved
  # it.
  defp sample(state, elapsed) do
    %{state | meter: Meter.sample(state.meter, elapsed, now())}
  end

  # Everything that depends on "what is in the queue now" decides it here, once.
  #
  # It used to be two functions — one scheduling the next row, one finishing a
  # re-parameterization — and the second was unreachable: the drain loop stops
  # scheduling itself the moment the queue empties, so the branch that noticed
  # emptiness never ran again. A re-parameterization would close its connection,
  # drain locally, and then sit there. One `cond` is not just shorter, it makes
  # that class of bug unrepresentable.
  defp advance(%{draining?: true} = state), do: state

  defp advance(state) do
    cond do
      not Queue.empty?(state.queue) ->
        send(self(), :drain)

        %{state | draining?: true}

      # Step three of a re-parameterization: everything in hand is committed, so
      # the cursor is exactly current and the new connection can be opened.
      state.reparameterizing? ->
        finish_reparameterize(state)

      true ->
        state
    end
  end

  ## Backpressure

  defp observe(state) do
    %{state | control: Control.observe(state.control, Queue.pressure(state.queue), now())}
  end

  # `Mint.HTTP.stream/2` re-arms `active: :once` itself, so withholding the
  # re-arm means switching the socket to passive rather than declining to call
  # it. That is what closes the TCP receive window and makes the backpressure
  # structural rather than advisory.
  #
  # Mint documents that one socket message may already be in the mailbox at the
  # switch. The gate reserves a whole frame's worth of room, so that overshoot
  # is already accounted for.
  defp gate_socket(%{conn: nil} = state), do: state

  defp gate_socket(state) do
    case {state.socket_mode, Queue.room?(state.queue)} do
      {:active, false} -> set_mode(state, :passive)
      {:passive, true} -> set_mode(state, :active)
      _ -> state
    end
  end

  defp set_mode(state, mode) do
    case Mint.HTTP.set_mode(state.conn, mode) do
      {:ok, conn} ->
        :telemetry.execute(
          [:dmex, :consumer, :socket],
          %{depth: Queue.depth(state.queue)},
          %{subscription: state.subscription, handler: state.handler, url: state.url, mode: mode}
        )

        %{state | conn: conn, socket_mode: mode}

      {:error, _reason} ->
        state
    end
  end

  ## Re-parameterizing

  # Already on the way down, or draining what is left before reconnecting.
  defp reparameterize(%{reparameterizing?: true} = state), do: state

  defp reparameterize(state) do
    decision =
      Control.decide(state.control, %{
        meter: state.meter,
        granted: state.params,
        requested: state.requested,
        config: state.config,
        now: now()
      })

    case decision do
      {:hold, control} ->
        %{state | control: control}

      {:adapt, requested, control} ->
        start_reparameterize(%{state | control: control, requested: requested})
    end
  end

  # The order matters, and this is step one: **close the connection first.**
  # That is the immediate relief and it is instant — it stops the inflow and
  # hands the producer back its request process, rather than leaving a stalled
  # connection for the producer's watcher to reclaim.
  #
  # Step two is draining what is already in hand, which happens through the
  # ordinary `:drain` loop with no connection attached: every queued row goes
  # through its own transaction and its own cursor advance, so nothing already
  # transferred is re-fetched. The drain also refines the estimate under no
  # inbound load at all.
  #
  # Step three, reconnecting from the post-drain cursor with the new headers,
  # happens in `finish_reparameterize/1` once the queue is empty.
  defp start_reparameterize(state) do
    :telemetry.execute(
      [:dmex, :consumer, :reparameterize],
      %{
        batch_window: state.requested.batch_window,
        max_batch_size: state.requested.max_batch_size,
        depth: Queue.depth(state.queue)
      },
      %{
        subscription: state.subscription,
        handler: state.handler,
        url: state.url,
        mean_service_ms: Meter.mean(state.meter)
      }
    )

    if state.conn, do: Mint.HTTP.close(state.conn)
    if state.liveness_timer, do: Process.cancel_timer(state.liveness_timer)

    %{state | conn: nil, ref: nil, liveness_timer: nil, reparameterizing?: true}
  end

  # The cursor is exactly current by now, so the new connection starts with an
  # empty queue and no overlap to suppress. Jittered like every other reconnect,
  # because a producer restart re-pacing every consumer at once is a
  # synchronized storm with nothing to absorb it.
  defp finish_reparameterize(state) do
    delay = Backoff.delay(1, state.config.reconnect_backoff, state.config.reconnect_backoff_max)

    Process.send_after(self(), :connect, delay)

    %{state | reparameterizing?: false}
  end

  defp now, do: System.monotonic_time(:millisecond)

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
  defp adopt_interval(state, advertised) do
    if Decoder.retry(state.decoder) == advertised do
      state
    else
      if state.liveness_timer, do: Process.cancel_timer(state.liveness_timer)

      schedule_liveness(state)
    end
  end

  # The producer advertises `heartbeat_ms + batch_window` as `retry:`, which is
  # the real upper bound on silence — the throttle runs after the heartbeat
  # branch. Until that arrives, assume a generous interval rather than risk
  # dropping a healthy connection from a producer that paces itself slowly.
  defp deadline(state) do
    interval = Decoder.retry(state.decoder) || @assumed_retry

    state.config.missed_heartbeats * interval
  end

  defp register(nil, _subscription), do: {:ok, self()}

  defp register(registry, subscription) do
    Registry.register(registry, {:subscription, subscription}, nil)
  end
end
