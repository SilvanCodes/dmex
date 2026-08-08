defmodule DMEx.Storage do
  @moduledoc """
  The behaviour both halves of DMEx read and write through.

  One behaviour covers both sides deliberately. A log appends to and reads from
  `dmex_messages`; a consumer reads and writes its own `dmex_cursors` and
  `dmex_dead_messages`. Different tables serving different roles, but the same
  *kind* of thing — a few keyed statements against an ordinary SQL database — so
  a second abstraction for the consumer's half would double the surface an
  engine has to implement for no gain.

  Every callback takes an `t:opts/0` map **as its last argument**, built once and
  threaded through — the subject of the call comes first, the ambient config
  last, here and in `DMEx.Notifier`. There is no adapter process and no adapter
  state, so the same adapter serves every log and every subscription in an
  application without any of them sharing anything.

  ## Three contracts that are correctness, not style

  Each is easy to implement *almost* right, and each has a test pinning it.

  **`c:append/4` must make id assignment order equal commit order.** If T1 takes
  `id=100` and T2 takes `id=101` but T2 commits first, a reader scanning
  `id > 99 ORDER BY id` sees 101, advances past it, and **permanently skips
  message 100** — silently, because `last_seen` advanced over the gap. Postgres
  separates the two moments, so `DMEx.Storage.Postgres` has to serialize appends
  itself: `pg_advisory_xact_lock`, never `pg_advisory_lock`, and never released
  explicitly. An engine where the separation cannot arise satisfies this without
  a lock — SQLite has one writer database-wide, so the second cannot draw a
  rowid until the first has committed.

  **`c:read/5` must return `data` and `metadata` as JSON *text*.**
  `DMEx.Transport.SSE.Frame.encode_row/1` splices those columns into the wire
  frame byte-for-byte, so any cast belongs in the query (`data::text`), not in
  post-processing — and an engine storing them as text pays nothing at all. An
  adapter that lets its driver decode a JSON column into a map hands `Frame` a
  map where it expects text. This is what makes per-row work a
  memcpy rather than a decode plus an encode — paid once per connection for the
  same rows, since every connection issues its own read.

  **`c:last_seen/2` must distinguish "no cursor" from "cursor at 0".** It
  returns `nil`, not `0`, when a subscription has never committed anything,
  which is what makes the `last_seen:` option apply exactly once.

  ## The contract is executable

  `test/dmex/storage/conformance_test.exs` runs one suite against every shipped
  adapter and asserts nothing about how an engine keeps its promises. It is the
  single statement of everything above: an adapter is finished when that file
  passes, and a change to this behaviour is one edit there rather than one per
  engine. Engine-specific behaviour — query plans, driver error shapes, the two
  places the storage formats genuinely differ — lives in `postgres_test.exs` and
  `sqlite_test.exs` beside it.
  """

  @typedoc """
  Everything an adapter needs, built once by the log module or the subscription
  and passed to every call.

  `:repo` is the only universally-required key. `:lock` is log-side; `:prefix`
  is the optional schema both sides may live in. Nothing about notification is
  here — that is `t:DMEx.Notifier.opts/0`.

  The map is open, because these are the keys *DMEx* knows about and an adapter
  is free to need others. One that does must still accept an `opts` without
  them, which is what reading its own keys rather than matching on them buys.
  """
  @type opts :: %{
          required(:repo) => module(),
          optional(:log) => module(),
          optional(:lock) => {integer(), integer()},
          optional(:prefix) => String.t() | nil,
          optional(atom()) => term()
        }

  @typedoc """
  One row of a log, as `c:read/5` returns it.

  `:data` and `:metadata` are JSON text as **binaries**: a transport that puts
  them on a wire splices them verbatim into its own iodata, and one that hands
  them to a pipeline passes them straight to `JSON.decode!/1`. Iodata belongs to
  the frame, which is bytes on their way out; a row's fields are stored values, so
  they are one type and it is this one.

  `:inserted_at` is a `DateTime.t()` and never a `NaiveDateTime.t()`: a naive
  timestamp renders without the trailing `Z`, which would silently change every
  frame on the wire. `DMEx.Storage.Postgres.Migration` uses `timestamptz` for
  exactly this reason, `DMEx.Storage.SQLite` parses its text column back into a
  `DateTime`, and `conformance_test.exs` asserts the struct for every adapter.

  **`:inserted_at` means two different things, by table.** From `c:read/5` it is
  when the message was appended; from `c:list_dead_messages/2` it is when the row
  landed in `dmex_dead_messages`, since a dead row's append time is deliberately
  not carried over — see `DMEx.Storage.Migration`. The column keeps one name
  because that is what makes a dead row *structurally* a log row, which is what
  lets a replay go straight back through the pipeline with no reshaping and no
  second decode path. Nothing in DMEx reads the field either way; it is
  observability, and `DMEx.Handler.DeadMessages.list/2` is the one place a
  reader has to know which of the two it is holding.
  """
  @type row :: %{
          required(:id) => pos_integer(),
          required(:type) => String.t(),
          required(:data) => binary(),
          required(:metadata) => binary(),
          required(:inserted_at) => DateTime.t()
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
  @callback append(type :: String.t(), data :: binary(), metadata :: binary(), opts()) ::
              {:ok, pos_integer()} | {:error, term()}

  @doc """
  Reads rows with `id > cursor` in ascending id order, bounded by **both** a row
  count and a byte budget, optionally narrowed to a set of message types.

  Four things an adapter must get right:

    * **Both bounds are applied inside the query.** `max_bytes` caps
      `octet_length(data) + octet_length(metadata) + octet_length(type)`
      summed across the returned rows. Reading `max_rows` rows and trimming the
      surplus afterwards has already spent the memory the bound exists to cap.
      `max_rows` is not advisory either: it bounds the fixed ~1 KB per-row cost of
      turning a row into terms, which no budget in database octets can see, and
      it is what makes the running sum cheap by giving it a bounded input.

    * **The byte bound is inclusive, with one exception.** The rows never total
      more than `max_bytes` — except that at least one row must always come
      back, even one larger than the whole budget on its own, since refusing it
      would wedge the subscription forever.

    * **`types` is two statements, not one.** `:all` for a whole-log
      subscription, or a list already expanded by `DMEx.Log.Plug`. A
      `($4 IS NULL OR type = ANY($4))` shape is a correct query and a
      ruinous plan, defeating the index on both branches.

  Nothing about the log's tip comes back: the wire carries no `max_id`, so a
  consumer cannot tell how far behind it is.

  For another adapter: `octet_length/1` is Postgres's spelling, SQLite's is
  `length(CAST(x AS BLOB))`. Both have window functions, so the running-sum
  shape carries over; what does not is putting the window at the same query
  level as the `LIMIT`, which computes it over every matching row first.
  """
  @callback read(
              cursor :: non_neg_integer(),
              max_rows :: pos_integer(),
              max_bytes :: pos_integer(),
              types :: :all | [String.t()],
              opts()
            ) :: {:ok, [row()]} | {:error, term()}

  @doc """
  The log's current highest id, or `0` when the log is empty.

  Two callers, both wanting the tip as an exclusive cursor: `DMEx.Log.Dispatcher`
  seeds its poll from it, so a stream connecting to a busy log is not woken for
  everything already behind it; and the connect path resolves an absent
  `Last-Event-ID` with it, which is the whole of "from now on".
  """
  @callback max_id(opts()) :: {:ok, non_neg_integer()} | {:error, term()}

  @doc """
  Returns the new highest id since `cursor` and the distinct types among those
  rows, or `{nil, []}` if nothing was appended.

  For `DMEx.Log.Dispatcher`'s poll, which broadcasts only when the id moves and
  needs the types to route it. The empty case is the common one on an idle log
  and must stay cheap: it runs once per poll interval per app.
  """
  @callback appended_since(cursor :: non_neg_integer(), opts()) ::
              {:ok, {non_neg_integer() | nil, [String.t()]}} | {:error, term()}

  @doc """
  Returns the durable read position for a subscription, or `nil` if it has never
  committed one.

  Keyed by the subscription's declared identifier rather than the handler
  module, so renaming the module does not mint a new subscription. See the
  module documentation for why `nil` and `0` must not be conflated.
  """
  @callback last_seen(subscription :: String.t(), opts()) ::
              {:ok, non_neg_integer() | nil} | {:error, term()}

  @doc """
  Runs `fun` inside a transaction on the consumer's own storage.

  This is what makes a handler's side effects and the `last_seen` advance
  atomic: they share one transaction, so DMEx's bookkeeping cannot drift from
  the callback. A raise inside `fun` must roll back and propagate.
  """
  @callback transaction((-> result), opts()) :: {:ok, result} | {:error, term()} when result: var

  @doc """
  Moves a subscription's cursor to `last_seen` — the column it writes, and the
  same exclusive position the `last_seen:` option names.

  It does not refuse to move backwards: duplicate suppression happens in the
  pipeline, which is the authority, and an operator resetting a cursor
  deliberately needs this to go both ways.

  **`:ok` and nothing else**, where `c:record_dead_message/2`,
  `c:delete_dead_message/3` and `c:delete_cursor/2` also admit
  `{:error, term()}`. Deliberate, and the asymmetry is worth stating rather than
  smoothing over. Both shipped adapters implement this as a hard match on the
  driver's reply, so a failure raises — and on the path that matters this call
  is inside `c:transaction/2` beside a handler's side effects, where a raise is
  the only outcome that rolls both back together. An error tuple there would be
  one a caller has no useful way to act on: returning it leaves the transaction
  open and committing.

  The siblings' wider return type is not a caller-facing difference either.
  Every DMEx call site binds `:ok = ` on them, so the tuple becomes a raise one
  frame later; it survives because those three are also called outside a
  transaction, from `DMEx.Handler.Cursors` and `DMEx.Handler.DeadMessages`.
  A `c:transaction/2` that fails as *infrastructure* is neither — that is
  `DMEx.Storage.Error`, raised by `DMEx.Handler.Pipeline`, and nothing here
  reports it.
  """
  @callback advance_cursor(subscription :: String.t(), last_seen :: non_neg_integer(), opts()) :: :ok

  @typedoc """
  A message on its way into `dmex_dead_messages`.

  The `t:DMEx.Handler.Pipeline.decoded_row/0` the pipeline was holding, handed
  over whole — string-keyed `"id"`, `"type"`, `"data"`, `"metadata"` — plus the
  `"subscription"` that gave up on it, since the table is shared across the
  application. No `"inserted_at"`: here it records when the row landed, and that
  is the adapter's to stamp.

  So `"data"` and `"metadata"` are **terms, not column text** — the one place the
  two directions differ, and deliberately: a consumer holds decoded maps, and
  what a column wants them to look like is the adapter's business, not a
  caller's. Both are objects, at worst `%{}`.
  """
  @type dead_row :: DMEx.Handler.Pipeline.decoded_row()

  @doc """
  Records a message its handler gave up on.

  Written in the same transaction as the `last_seen` advance past it, so the two
  commit together or not at all.

  The adapter encodes `"data"` and `"metadata"` for its own columns — see
  `t:dead_row/0`. Both shipped ones match the whole shape in the function head, so
  a row short of a key raises rather than being written half-formed.

  **Nothing about the failure is stored**, which is what lets
  `c:list_dead_messages/2` return the row as an ordinary `t:row/0`.
  """
  @callback record_dead_message(dead_row(), opts()) :: :ok | {:error, term()}

  @doc """
  Lists every message `subscription` gave up on, oldest first, as log rows.

  `t:row/0` shape — what `c:read/5` returns and the pipeline consumes — because
  the stored columns are the log's columns, so replay needs no reshaping.
  """
  @callback list_dead_messages(subscription :: String.t(), opts()) ::
              {:ok, [row()]} | {:error, term()}

  @doc """
  Removes one dead message, by subscription and log id.

  Called inside `c:transaction/2` by a replay, so a message is dropped exactly
  when its handler finally accepted it.
  """
  @callback delete_dead_message(subscription :: String.t(), id :: non_neg_integer(), opts()) ::
              :ok | {:error, term()}

  @doc """
  Returns a subscription's full cursor row, or `nil`.

  `c:last_seen/2` answers the read path's question — where am I — and returns
  only the id. This answers an operator's: when did this last move? Used by
  `DMEx.Handler.Cursors`, not by the stream.
  """
  @callback cursor(subscription :: String.t(), opts()) ::
              {:ok, %{required(:last_seen) => non_neg_integer(), optional(atom()) => term()} | nil}
              | {:error, term()}

  @doc """
  Removes a subscription's cursor entirely.

  Distinct from advancing it to 0: with no row at all the subscription's
  `last_seen:` option applies again on its next start.
  """
  @callback delete_cursor(subscription :: String.t(), opts()) :: :ok | {:error, term()}

  @doc """
  The `DMEx.Notifier` to use when a log names none.

  Notification is a separate axis — see `DMEx.Notifier` — but which mechanism
  *fits* is an engine's own business, so the engine names it rather than a
  `case` somewhere central that a third-party adapter could never appear in.

  Optional. An adapter that does not implement it forces every log to pass
  `notifier:` explicitly.
  """
  @callback default_notifier() :: module()

  @doc """
  Refuses options this adapter cannot honour, before anything runs.

  Called once per log when its supervision tree starts, and once per
  subscription — at boot rather than at the first query, so an option that will
  never be honoured fails where it was written rather than somewhere downstream.
  `DMEx.Storage.SQLite` refuses a `:prefix` here, since SQLite has no schemas.

  Optional. An adapter that implements it may read `repo.config()`; the repo is
  started by then.
  """
  @callback validate!(opts()) :: :ok

  @optional_callbacks default_notifier: 0, validate!: 1

  @doc """
  Runs `adapter`'s `c:validate!/1` if it has one, and returns `:ok`.

  Here rather than on either caller because `c:validate!/1` is optional, so
  every call site would otherwise carry the same `function_exported?/3` — and
  both halves of DMEx have one: a log when its tree starts, a subscription when
  its storage is resolved. Neither of them is a log's business, which is why
  this is not on `DMEx.Log`.
  """
  @spec validate!(module(), opts()) :: :ok
  def validate!(adapter, opts) do
    if function_exported?(adapter, :validate!, 1) do
      adapter.validate!(opts)
    end

    :ok
  end

  ## Table names

  # The three table names, stated once. Both adapters' SQL and both migrations'
  # DDL render these rather than repeating the literals — the same move as
  # `DMEx.Storage.Postgres.__table__/2`, which is already the one renderer for a
  # prefix-qualified name so that DDL and queries cannot disagree about it.
  # Here rather than on an adapter because the names are the *behaviour's*: an
  # engine implements these tables, it does not get to pick what they are called.
  #
  # Atoms, because `Ecto.Migration.table/2` wants one and every other site
  # interpolates, where an atom renders as its own name.
  @doc false
  @spec __table__(:messages | :cursors | :dead_messages) :: atom()
  def __table__(:messages), do: :dmex_messages
  def __table__(:cursors), do: :dmex_cursors
  def __table__(:dead_messages), do: :dmex_dead_messages
end
