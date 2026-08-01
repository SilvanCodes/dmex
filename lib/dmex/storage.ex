defmodule DMEx.Storage do
  @moduledoc """
  The behaviour both halves of DMEx read and write through.

  One behaviour covers both sides deliberately. A log appends to and reads
  from `dmex_messages`; a consumer reads and writes its own `dmex_cursors` and
  `dmex_dead_messages`. Those are different tables serving different roles, but
  they are the same *kind* of thing — a few keyed statements against an ordinary
  SQL database — and inventing a second abstraction for the consumer's half
  would double the surface an engine has to implement for no gain.

  ## Options

  Every callback takes an `t:opts/0` map, built once and threaded through. There
  is no adapter process and no adapter state: an adapter is a module of
  functions over this map, so the same adapter serves every log and every
  subscription in an application without any of them sharing anything.

  ## Three contracts that are correctness, not style

  These are easy to implement *almost* right, and each has a test pinning it.

  ### `c:append/4` must hold its lock until COMMIT

  Postgres allocates sequence values at INSERT time but makes rows visible at
  COMMIT time. If T1 takes `id=100` and T2 takes `id=101` but T2 commits first,
  a reader scanning `id > 99 ORDER BY id` sees 101, advances its cursor past it,
  and **permanently skips message 100** once T1 finally commits — silently,
  because `last_seen` advanced right over the gap.

  Every read path in DMEx is a cursor over `id`, so appends are serialized per
  app with a *transaction-scoped* advisory lock. A lock released at the end of
  the INSERT statement lets a later-allocated id commit first, which is the
  exact failure it was meant to prevent — so it must be
  `pg_advisory_xact_lock`, never `pg_advisory_lock`, and it must never be
  released explicitly.

  ### `c:read/5` must return `data` and `metadata` as JSON *text*

  `DMEx.Transport.SSE.Frame.encode_row/1` splices those two columns into the wire frame
  byte-for-byte, without parsing them. An adapter that lets its driver decode
  `jsonb` into an Elixir map hands `Frame` a map where it expects text, and the
  frame it builds is wrong — so the cast belongs in the query (`data::text`),
  not in a post-processing step.

  This is what makes the log's per-row work a memcpy rather than a decode
  plus an encode. Since every connection issues its own read, that cost would
  otherwise be paid once per connection for the same rows.

  ### `c:last_seen/2` must distinguish "no cursor" from "cursor at 0"

  It returns `nil`, not `0`, when a subscription has never committed anything. That
  is what makes `start: :latest` meaningful exactly once: a subscription with no
  cursor resolves `:latest` against the log's tip, and one with a cursor —
  even at 0 — ignores `:start` entirely, because the durable position is always
  the authority.
  """

  @typedoc """
  Everything an adapter needs, built once by the log module or the
  subscription and passed to every call.

  `:repo` is the only universally-required key. `:lock` and `:channel` are
  log-side (they scope the append lock and the notification channel);
  `:prefix` is the optional schema both sides may live in.
  """
  @type opts :: %{
          required(:repo) => module(),
          optional(:log) => module(),
          optional(:lock) => {integer(), integer()},
          optional(:channel) => String.t(),
          optional(:prefix) => String.t() | nil
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
  it to COMMIT — see the module documentation for why that ordering is
  load-bearing rather than cautious.

  `data` and `metadata` arrive as already-encoded JSON binaries. The adapter
  hands them to the database as JSON without decoding them, and nothing DMEx
  owns is merged into `metadata`.

  A caller already inside a transaction extends that lock to the enclosing
  transaction's lifetime, which serializes every other append in the app for
  its duration. That is a documented consequence, not a bug.
  """
  @callback append(opts(), type :: String.t(), data :: binary(), metadata :: binary()) ::
              {:ok, pos_integer()} | {:error, term()}

  @doc """
  Reads rows with `id > cursor` in ascending id order, bounded by **both** a row
  count and a byte budget, optionally narrowed to a set of message types.

  ## The byte bound is the adapter's job, not the caller's

  `max_bytes` caps `octet_length(data) + octet_length(metadata) +
  octet_length(message_type)` summed across the returned rows, and it must be
  enforced **inside the query**. Reading `limit` rows and discarding the surplus
  afterwards satisfies the letter of this and defeats the purpose: `limit` rows
  of large payloads are fully materialised in the caller's memory before
  anything trims them, which is precisely the unbounded-growth failure the
  log's send loop is built to exclude. Only the database can bound that
  before it is spent.

  ### `limit` is not redundant, and an adapter must not treat it as advisory

  `max_bytes` bounds payload; `limit` bounds the fixed per-row cost of turning a
  row into terms, which is around a kilobyte and which no byte budget expressed
  in database octets can see. On small messages that fixed cost is two orders of
  magnitude larger than the payload, so a byte bound on its own is not a memory
  bound at all.

  `limit` is also what makes `max_bytes` *cheap*. A byte bound is a running sum
  over the batch, and a running sum needs a bounded input — without a `LIMIT`
  beneath it, the sum has to visit every row after the cursor to find out where
  to stop, which turns a bounded read into a scan of the log's tail. An adapter
  that drops the row limit has not simplified the query, it has made it
  unbounded.

  The bound is **inclusive**: the rows that come back never total more than
  `max_bytes`. A predicate that admits a row while the rows *before* it fit is
  off by one row's size and is not a bound at all — it caps at `max_bytes` plus
  whatever the last row happened to be, which on a log with one 40 MB payload is
  not a cap.

  **The single exception is that at least one row must always come back**, even
  one on its own larger than `max_bytes`. Refusing it would wedge the
  subscription on that row forever, and a stuck stream is a worse failure than
  one oversized read. So the guarantee is precisely: *at most `max_bytes`,
  unless that would mean returning nothing.*

  The bound is on payload bytes, not on encoded frame bytes: framing is
  `DMEx.Transport.SSE.Frame`'s concern and a storage adapter should not know about it. A
  caller wanting a wire figure adds its own per-row constant — roughly 130 bytes
  of SSE scaffolding and ISO 8601 timestamp per row, at the time of writing.

  ## Writing another adapter

  `octet_length/1` is Postgres's spelling. SQLite's is
  `length(CAST(x AS BLOB))` — `length/1` on a text value counts characters, so
  the cast is what makes it count bytes. Both support window functions, so the
  running-sum shape carries over directly; what does not carry over is putting
  the window in the same query level as the `LIMIT`, since that computes it
  over every matching row before the limit applies.

  ## Every row states its predecessor

  Each returned row carries `:previous_id` — the id of the row before it **in
  this delivered sequence**, with the first row of a read carrying `cursor`
  itself. That is what lets a consumer prove nothing went missing between two
  rows it received.

  It cannot be inferred as `id - 1`. A selective read returns a sparse subset of
  the id space, so consecutive delivered rows are routinely far apart, and only
  the log knows which of those gaps it created on purpose. Stating the link
  explicitly is what makes a *deliberate* gap and a *lost* row distinguishable
  at all.

  The chain is over the rows the read returns, so truncating a batch on
  `max_bytes` or `limit` cannot break it: both cut the tail, and the next read
  resumes with `cursor` set to the last id delivered.

  ## Everything else

  `types` is `:all` for a whole-log subscription — the primary key serves that
  directly — or an explicit list, already expanded from a consumer's filter by
  `DMEx.Log.Plug`. An adapter must issue these as **two different
  statements** rather than one with a nullable predicate: a
  `($4 IS NULL OR message_type = ANY($4))` shape is a correct query and a
  ruinous plan, since it defeats the index on both branches.

  Nothing about the log's tip comes back with the rows. The wire carries no
  `max_id`, so there is nothing for a read to stamp a frame with — a consumer
  measures how far behind it is from `inserted_at`, which every row carries
  anyway.
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
  What has been appended since `cursor`: the new highest id, and the distinct
  message types among those rows.

  `DMEx.Log.Dispatcher`'s poll, which broadcasts only when the id moves and
  needs the types to route that broadcast. Returns `{nil, []}` when nothing has
  been appended since — which is the common case on an idle log and must stay
  cheap, since it runs once per poll interval per application.

  The types are what make the poll route as precisely as the notification path
  does. Without them the poll would have to wake every connection on any log
  appending more than once per interval, which is exactly the amplification the
  routing exists to remove.
  """
  @callback appended_since(opts(), cursor :: non_neg_integer()) ::
              {:ok, {non_neg_integer() | nil, [String.t()]}} | {:error, term()}

  @doc """
  The durable read position for a subscription, or `nil` if it has never
  committed one.

  Keyed by the subscription's declared identifier — `"orders/order_projection"`,
  the `\#{log}/\#{id}` pair from `DMEx.Handler` — rather than by the
  handler module, so renaming the module does not mint a new subscription. See
  the module documentation for why `nil` and `0` must not be conflated.
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

  Written in the same transaction as the `last_seen` advance that moves past it,
  so a dead message and the cursor that skipped it commit together or not at all.

  `attrs` carries `:subscription` — the table is shared across every
  subscription in the application, so a row has to say which one owns it —
  followed by the log row itself: `:id`, `:message_type`, `:data` and
  `:metadata`, unchanged from what arrived. Not `:inserted_at`: on this table
  that column records when the row landed here, which is the adapter's to
  stamp.

  **Nothing about the failure is stored.** No exception, no message, no attempt
  count. A failure is an event rather than a property of the message, so it goes
  to logs and telemetry; keeping the stored row a pure message is what makes
  `c:dead_messages/2` able to return it as an ordinary `t:row/0`.
  """
  @callback dead_message(opts(), attrs :: map()) :: :ok | {:error, term()}

  @doc """
  Every message `subscription` has given up on, oldest first, as ordinary log
  rows.

  The rows come back in `t:row/0` shape — the same shape `c:read/5` returns and
  the same shape the consumer pipeline consumes — because the stored columns are
  the log's columns. Replaying a dead message is therefore feeding a row back to
  a handler, with no reshaping in between.
  """
  @callback dead_messages(opts(), subscription :: String.t()) ::
              {:ok, [row()]} | {:error, term()}

  @doc """
  Removes one dead message, identified the way it is keyed: by subscription and
  log id. A subscription reads exactly one log, so its own ids cannot
  collide.

  Called inside `c:transaction/2` by a replay, so that a message is dropped from
  this table exactly when its handler finally accepted it.
  """
  @callback delete_dead_message(opts(), subscription :: String.t(), id :: non_neg_integer()) ::
              :ok | {:error, term()}

  @doc """
  A subscription's full cursor row, or `nil`.

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
  `start:` option applies again on its next start, which is what makes
  "re-read this stream from wherever `start:` says" expressible.
  """
  @callback delete_cursor(opts(), subscription :: String.t()) :: :ok | {:error, term()}

  @doc """
  Processes this adapter needs running alongside a log.

  Started by the log's own supervisor, before `DMEx.Log.Dispatcher`. Postgres
  returns a `Postgrex.Notifications` connection here — a `LISTEN` connection
  cannot be shared with the pool, since it must stay checked out for its whole
  life — and an engine with no notification mechanism returns `[]`.

  This callback exists so that **nothing outside an adapter knows how that
  adapter is wired**. Before it, `DMEx.Log` compared the configured storage
  module against `DMEx.Storage.Postgres` by name and carried a list of
  Postgrex-specific connection options, which made adding a second engine an
  edit to the log rather than a new adapter.
  """
  @callback child_specs(opts()) :: [Supervisor.child_spec() | {module(), term()} | module()]

  @doc """
  Subscribes the calling process to this log's append notifications.

  Returns the pid to monitor, so `DMEx.Log.Dispatcher` can re-establish the
  subscription if it dies. `:unsupported` is the honest answer for an engine
  with no notification mechanism at all — the dispatcher then runs polling-only
  without warning, since nothing is wrong.

  Best-effort by contract: an adapter must not raise or exit here. Polling is
  what makes dispatch robust, so a failed subscription costs latency and never
  correctness.
  """
  @callback listen(opts()) :: {:ok, pid()} | :unsupported | {:error, term()}
end
