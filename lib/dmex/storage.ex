defmodule DMEx.Storage do
  @moduledoc """
  The behaviour both halves of DMEx read and write through.

  One behaviour covers both sides deliberately. A log appends to and reads from
  `dmex_messages`; a consumer reads and writes its own `dmex_cursors` and
  `dmex_dead_messages`. Different tables serving different roles, but the same
  *kind* of thing — a few keyed statements against an ordinary SQL database — so
  a second abstraction for the consumer's half would double the surface an
  engine has to implement for no gain.

  Every callback takes an `t:opts/0` map, built once and threaded through. There
  is no adapter process and no adapter state, so the same adapter serves every
  log and every subscription in an application without any of them sharing
  anything.

  ## Three contracts that are correctness, not style

  Each is easy to implement *almost* right, and each has a test pinning it.

  **`c:append/4` must hold its lock until COMMIT.** Postgres allocates sequence
  values at INSERT time but makes rows visible at COMMIT time, so if T1 takes
  `id=100` and T2 takes `id=101` but T2 commits first, a reader scanning
  `id > 99 ORDER BY id` sees 101, advances past it, and **permanently skips
  message 100** — silently, because `last_seen` advanced over the gap. So it
  must be `pg_advisory_xact_lock`, never `pg_advisory_lock`, and never released
  explicitly.

  **`c:read/5` must return `data` and `metadata` as JSON *text*.**
  `DMEx.Transport.SSE.Frame.encode_row/1` splices those columns into the wire
  frame byte-for-byte, so the cast belongs in the query (`data::text`), not in
  post-processing. An adapter that lets its driver decode `jsonb` into a map
  hands `Frame` a map where it expects text. This is what makes per-row work a
  memcpy rather than a decode plus an encode — paid once per connection for the
  same rows, since every connection issues its own read.

  **`c:last_seen/2` must distinguish "no cursor" from "cursor at 0".** It
  returns `nil`, not `0`, when a subscription has never committed anything,
  which is what makes `start: :latest` meaningful exactly once.
  """

  @typedoc """
  Everything an adapter needs, built once by the log module or the subscription
  and passed to every call.

  `:repo` is the only universally-required key. `:lock` and `:channel` are
  log-side; `:prefix` is the optional schema both sides may live in.

  The map is open, because these are the keys *DMEx* knows about and an adapter
  is free to need others. One that does must still accept an `opts` without
  them, which is what reading its own keys rather than matching on them buys.
  """
  @type opts :: %{
          required(:repo) => module(),
          optional(:log) => module(),
          optional(:lock) => {integer(), integer()},
          optional(:channel) => String.t(),
          optional(:prefix) => String.t() | nil,
          optional(atom()) => term()
        }

  @typedoc """
  One row of a log, as `c:read/5` returns it.

  `:previous_id` is not a column — it is the id of the row before this one in
  the *delivered* sequence, which only the read knows. `nil` where there is no
  sequence to speak of, as when replaying a dead message.

  `:data` and `:metadata` are JSON text: a transport that puts them on a wire
  splices them verbatim, and one that hands them to a pipeline decodes them.
  """
  @type row :: %{
          required(:id) => pos_integer(),
          required(:previous_id) => non_neg_integer() | nil,
          required(:message_type) => String.t(),
          required(:data) => iodata(),
          required(:metadata) => iodata(),
          required(:inserted_at) => DateTime.t() | NaiveDateTime.t()
        }

  @doc """
  Appends one message and returns its assigned id.

  Runs in a transaction that takes the per-app advisory lock *first* and holds
  it to COMMIT — see the module documentation for why that is load-bearing.

  `data` and `metadata` arrive as already-encoded JSON binaries, handed to the
  database without decoding, and nothing DMEx owns is merged into `metadata`.

  A caller already inside a transaction extends that lock for the enclosing
  transaction's lifetime, serializing every other append in the app for its
  duration. A documented consequence, not a bug.
  """
  @callback append(opts(), type :: String.t(), data :: binary(), metadata :: binary()) ::
              {:ok, pos_integer()} | {:error, term()}

  @doc """
  Reads rows with `id > cursor` in ascending id order, bounded by **both** a row
  count and a byte budget, optionally narrowed to a set of message types.

  Four things an adapter must get right:

    * **Both bounds are applied inside the query.** `max_bytes` caps
      `octet_length(data) + octet_length(metadata) + octet_length(message_type)`
      summed across the returned rows. Reading `limit` rows and trimming the
      surplus afterwards has already spent the memory the bound exists to cap.
      `limit` is not advisory either: it bounds the fixed ~1 KB per-row cost of
      turning a row into terms, which no budget in database octets can see, and
      it is what makes the running sum cheap by giving it a bounded input.

    * **The byte bound is inclusive, with one exception.** The rows never total
      more than `max_bytes` — except that at least one row must always come
      back, even one larger than the whole budget on its own, since refusing it
      would wedge the subscription forever.

    * **Every row states its predecessor.** `:previous_id` is the id of the row
      before it *in this delivered sequence*, with the first row of a read
      carrying `cursor` itself. It cannot be inferred as `id - 1`, because a
      selective read returns a sparse subset of the id space and only the log
      knows which gaps it made on purpose. Truncating a batch cannot break the
      chain: both bounds cut the tail.

    * **`types` is two statements, not one.** `:all` for a whole-log
      subscription, or a list already expanded by `DMEx.Log.Plug`. A
      `($4 IS NULL OR message_type = ANY($4))` shape is a correct query and a
      ruinous plan, defeating the index on both branches.

  Nothing about the log's tip comes back: the wire carries no `max_id`, and a
  consumer measures how far behind it is from `inserted_at`.

  For another adapter: `octet_length/1` is Postgres's spelling, SQLite's is
  `length(CAST(x AS BLOB))`. Both have window functions, so the running-sum
  shape carries over; what does not is putting the window at the same query
  level as the `LIMIT`, which computes it over every matching row first.
  """
  @callback read(
              opts(),
              cursor :: non_neg_integer(),
              limit :: pos_integer(),
              max_bytes :: pos_integer(),
              types :: :all | [String.t()]
            ) :: {:ok, [row()]} | {:error, term()}

  @doc """
  The log's current highest id, or `0` when the log is empty.

  Used for resolving a `Last-Event-ID: latest` connect against the tip.
  """
  @callback max_id(opts()) :: {:ok, non_neg_integer()} | {:error, term()}

  @doc """
  Returns the new highest id since `cursor` and the distinct types among those
  rows, or `{nil, []}` if nothing was appended.

  For `DMEx.Log.Dispatcher`'s poll, which broadcasts only when the id moves and
  needs the types to route it. The empty case is the common one on an idle log
  and must stay cheap: it runs once per poll interval per app.
  """
  @callback appended_since(opts(), cursor :: non_neg_integer()) ::
              {:ok, {non_neg_integer() | nil, [String.t()]}} | {:error, term()}

  @doc """
  Returns the durable read position for a subscription, or `nil` if it has never
  committed one.

  Keyed by the subscription's declared identifier rather than the handler
  module, so renaming the module does not mint a new subscription. See the
  module documentation for why `nil` and `0` must not be conflated.
  """
  @callback last_seen(opts(), subscription :: String.t()) ::
              {:ok, non_neg_integer() | nil} | {:error, term()}

  @doc """
  Runs `fun` inside a transaction on the consumer's own storage.

  This is what makes a handler's side effects and the `last_seen` advance
  atomic: they share one transaction, so DMEx's bookkeeping cannot drift from
  the callback. A raise inside `fun` must roll back and propagate.
  """
  @callback transaction(opts(), (-> result)) :: {:ok, result} | {:error, term()} when result: var

  @doc """
  Moves a subscription's cursor to `id`.

  Always called inside `c:transaction/2`. It does not refuse to move backwards:
  duplicate suppression happens in the pipeline, which is the authority, and an
  operator resetting a cursor deliberately needs this to go both ways.
  """
  @callback advance(opts(), subscription :: String.t(), id :: pos_integer()) :: :ok

  @doc """
  Records a message its handler gave up on.

  Written in the same transaction as the `last_seen` advance past it, so the two
  commit together or not at all.

  `attrs` carries `:subscription` — the table is shared across the application —
  plus the log row itself: `:id`, `:message_type`, `:data`, `:metadata`,
  unchanged. Not `:inserted_at`, which here records when the row landed and is
  the adapter's to stamp.

  **Nothing about the failure is stored**, which is what lets
  `c:dead_messages/2` return the row as an ordinary `t:row/0`.
  """
  @callback dead_message(opts(), attrs :: map()) :: :ok | {:error, term()}

  @doc """
  Lists every message `subscription` gave up on, oldest first, as log rows.

  `t:row/0` shape — what `c:read/5` returns and the pipeline consumes — because
  the stored columns are the log's columns, so replay needs no reshaping.
  """
  @callback dead_messages(opts(), subscription :: String.t()) ::
              {:ok, [row()]} | {:error, term()}

  @doc """
  Removes one dead message, by subscription and log id.

  Called inside `c:transaction/2` by a replay, so a message is dropped exactly
  when its handler finally accepted it.
  """
  @callback delete_dead_message(opts(), subscription :: String.t(), id :: non_neg_integer()) ::
              :ok | {:error, term()}

  @doc """
  Returns a subscription's full cursor row, or `nil`.

  `c:last_seen/2` answers the read path's question — where am I — and returns
  only the id. This answers an operator's: when did this last move? Used by
  `DMEx.Handler.Cursors`, not by the stream.
  """
  @callback cursor(opts(), subscription :: String.t()) ::
              {:ok,
               %{required(:last_seen) => non_neg_integer(), optional(atom()) => term()} | nil}
              | {:error, term()}

  @doc """
  Removes a subscription's cursor entirely.

  Distinct from advancing it to 0: with no row at all the subscription's
  `start:` option applies again on its next start.
  """
  @callback delete_cursor(opts(), subscription :: String.t()) :: :ok | {:error, term()}

  @doc """
  Returns the processes this adapter needs running alongside a log.

  Started by the log's supervisor, before `DMEx.Log.Dispatcher`. Postgres puts a
  `Postgrex.Notifications` connection here, since a `LISTEN` connection must
  stay checked out for its whole life and cannot be pooled; an engine with no
  notification mechanism returns `[]`.

  This exists so **nothing outside an adapter knows how that adapter is wired**,
  which is what makes a second engine a new adapter rather than an edit here.
  """
  @callback child_specs(opts()) :: [Supervisor.child_spec() | {module(), term()} | module()]

  @doc """
  Subscribes the calling process to this log's append notifications.

  Returns the pid to monitor, so `DMEx.Log.Dispatcher` can re-establish the
  subscription if it dies. `:unsupported` is the honest answer for an engine
  with no notification mechanism at all — the dispatcher then runs polling-only
  without warning.

  Best-effort by contract: an adapter must not raise or exit here. Polling is
  what makes dispatch robust, so a failed subscription costs latency and never
  correctness.
  """
  @callback listen(opts()) :: {:ok, pid()} | :unsupported | {:error, term()}
end
