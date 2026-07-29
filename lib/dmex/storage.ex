defmodule DMEx.Storage do
  @moduledoc """
  The behaviour both halves of DMEx read and write through.

  One behaviour covers both sides deliberately. A producer appends to and reads
  from `dmex_messages`; a consumer reads and writes its own `dmex_cursors` and
  `dmex_dead_messages`. Those are different tables serving different roles, but
  they are the same *kind* of thing — a few keyed statements against an ordinary
  SQL database — and inventing a second abstraction for the consumer's half
  would double the surface an engine has to implement for no gain.

  ## Options

  Every callback takes an `t:opts/0` map, built once and threaded through. There
  is no adapter process and no adapter state: an adapter is a module of
  functions over this map, so the same adapter serves every producer and every
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

  ### `c:read/3` must return `data` and `metadata` as JSON *text*

  `DMEx.SSE.Frame.encode_batch/2` splices those two columns into the wire frame
  byte-for-byte, without parsing them. An adapter that lets its driver decode
  `jsonb` into an Elixir map hands `Frame` a map where it expects text, and the
  frame it builds is wrong — so the cast belongs in the query (`data::text`),
  not in a post-processing step.

  This is what makes the producer's per-row work a memcpy rather than a decode
  plus an encode. Since every connection issues its own read, that cost would
  otherwise be paid once per connection for the same rows.

  ### `c:last_seen/2` must distinguish "no cursor" from "cursor at 0"

  It returns `nil`, not `0`, when a subscription has never committed anything. That
  is what makes `start: :latest` meaningful exactly once: a subscription with no
  cursor resolves `:latest` against the producer's tip, and one with a cursor —
  even at 0 — ignores `:start` entirely, because the durable position is always
  the authority.
  """

  @typedoc """
  Everything an adapter needs, built once by the producer module or the
  subscription and passed to every call.

  `:repo` is the only universally-required key. `:app_key`, `:lock` and
  `:channel` are producer-side (they scope the append lock and the notification
  channel); `:prefix` is the optional schema both sides may live in.
  """
  @type opts :: %{
          required(:repo) => module(),
          optional(:producer) => module(),
          optional(:app_key) => String.t(),
          optional(:lock) => {integer(), integer()},
          optional(:channel) => String.t(),
          optional(:prefix) => String.t() | nil
        }

  @typedoc "A log row, exactly as `DMEx.SSE.Frame` expects one."
  @type row :: DMEx.SSE.Frame.row()

  @doc """
  Appends one message and returns its assigned id.

  Runs in a transaction that takes the per-app advisory lock *first* and holds
  it to COMMIT — see the module documentation for why that ordering is
  load-bearing rather than cautious.

  `data` and `metadata` arrive as already-encoded JSON binaries. The adapter
  hands them to the database as JSON without decoding them, and nothing DMEx
  owns is merged into `metadata`.

  A caller already inside a transaction extends that lock to the enclosing
  transaction's lifetime, which serializes every other publish in the app for
  its duration. That is a documented consequence, not a bug.
  """
  @callback append(opts(), type :: String.t(), data :: binary(), metadata :: binary()) ::
              {:ok, pos_integer()} | {:error, term()}

  @doc """
  Reads up to `limit` rows with `id > cursor`, in ascending id order.

  Returns the rows together with the log's current `max_id` as of the same
  round trip — a correlated scalar subquery, not a second query. On the primary
  key that is one descent to the rightmost entry, and folding it into a read the
  loop was issuing anyway is what avoids paying an extra query per connection
  per `batch_window`.

  `max_id` is `nil` when the read returns no rows, which is legal: a zero-row
  read sends no frame, so there is nothing for it to stamp.

  This is the only read shape the producer ever issues. It takes no predicate
  but the cursor, so the primary key serves it directly and v1 needs no
  secondary index at all.
  """
  @callback read(opts(), cursor :: non_neg_integer(), limit :: pos_integer()) ::
              {:ok, {[row()], non_neg_integer() | nil}} | {:error, term()}

  @doc """
  The log's current highest id, or `0` when the log is empty.

  Used for two things: `DMEx.Producer.Dispatcher`'s poll, which broadcasts only
  when this moves, and resolving a `Last-Event-ID: latest` connect against the tip.
  """
  @callback max_id(opts()) :: {:ok, non_neg_integer()} | {:error, term()}

  @doc """
  The durable read position for a subscription, or `nil` if it has never
  committed one.

  Keyed by the subscription's declared identifier — `"orders/order_projection"`,
  the `\#{producer}/\#{id}` pair from `DMEx.Consumer.Handler` — rather than by the
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

  The rows come back in `t:row/0` shape — the same shape `c:read/3` returns and
  the same shape the consumer pipeline consumes — because the stored columns are
  the log's columns. Replaying a dead message is therefore feeding a row back to
  a handler, with no reshaping in between.
  """
  @callback dead_messages(opts(), subscription :: String.t()) ::
              {:ok, [row()]} | {:error, term()}

  @doc """
  Removes one dead message, identified the way it is keyed: by subscription and
  log id. A subscription reads exactly one producer's log, so its own ids cannot
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
  `DMEx.Consumer.Cursors`, not by the stream.
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
  Processes this adapter needs running alongside a producer.

  Started by the producer's own supervisor, before `DMEx.Producer.Dispatcher`. Postgres
  returns a `Postgrex.Notifications` connection here — a `LISTEN` connection
  cannot be shared with the pool, since it must stay checked out for its whole
  life — and an engine with no notification mechanism returns `[]`.

  This callback exists so that **nothing outside an adapter knows how that
  adapter is wired**. Before it, `DMEx.Producer` compared the configured storage
  module against `DMEx.Storage.Postgres` by name and carried a list of
  Postgrex-specific connection options, which made adding a second engine an
  edit to the producer rather than a new adapter.
  """
  @callback child_specs(opts()) :: [Supervisor.child_spec() | {module(), term()} | module()]

  @doc """
  Subscribes the calling process to this producer's publish notifications.

  Returns the pid to monitor, so `DMEx.Producer.Dispatcher` can re-establish the
  subscription if it dies. `:unsupported` is the honest answer for an engine
  with no notification mechanism at all — the dispatcher then runs polling-only
  without warning, since nothing is wrong.

  Best-effort by contract: an adapter must not raise or exit here. Polling is
  what makes dispatch robust, so a failed subscription costs latency and never
  correctness.
  """
  @callback listen(opts()) :: {:ok, pid()} | :unsupported | {:error, term()}
end
