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

  Nothing in DMEx calls an adapter module directly. That map carries the adapter
  too, and the functions under *Calling an adapter* take it first and dispatch
  on it.

  ## Two ways to be an adapter

  A SQL engine writes `use DMEx.Storage.SQL` and declares a **dialect** — what
  Postgres and SQLite spell differently. That generates every statement
  callback, leaving the engine only what is genuinely its own.

  Anything else implements the callbacks directly. The suite's in-memory stub does,
  which is what keeps this a behaviour rather than a SQL template — and is why the
  dialect is `DMEx.Storage.SQL`'s callback and not one of these.

  ## Three contracts that are correctness, not style

  Each is easy to implement *almost* right, and each is stated on the callback
  it constrains: `c:append/4` must make id assignment order equal commit order,
  `c:read/5` must return `data` and `metadata` as JSON *text*, and
  `c:last_seen/2` must distinguish "no cursor" from "cursor at 0".

  The first is the one the conformance suite cannot pin — the Ecto sandbox is
  one connection, so nothing inside it can append concurrently — so it lives in
  `test/integration/append_ordering_test.exs`, which runs 8 writers against a
  real database. The other two are conformance tests.

  ## How a write fails

  **A read returns `{:error, reason}`; a write returns `:ok` or raises** — except
  `c:append/4`, which answers `{:ok, id}` or `{:error, reason}`, because a row can
  be refused on its own merits where a cursor advance can only fail with its
  connection. Reads have callers with somewhere to go — `DMEx.Log.SendLoop` closes
  the connection, `DMEx.Log.Plug` answers `503`,
  `DMEx.Handler.DeadMessages.list/2` hands the tuple back — while the four writes
  (`c:advance_cursor/3`, `c:record_dead_message/2`, `c:delete_dead_message/3`,
  `c:delete_cursor/2`) have none, and the path that matters is inside
  `c:transaction/2` beside a handler's side effects, where a raise is the only
  outcome that rolls both back together. An error tuple there leaves the
  transaction open and committing.

  So those four raise: `Ecto.Adapters.SQL.query!/4` rather than a match on
  `query/4`'s reply, which means the exception a crash log carries is the
  driver's own, naming the constraint or the connection that failed. Nothing in
  DMEx rescues it — `DMEx.Handler.Pipeline` tags what the *handler* raised and
  rescues only that, so a storage failure kills the subscription with the cursor
  still below the message it was processing.
  """

  @typedoc """
  An adapter and everything it needs, built once by the log module or the
  subscription and passed to every call.

  `:adapter` is the module implementing this behaviour, so this map is the whole
  handle rather than half of a `{module, opts}` pair — see *Calling an adapter*.
  `:repo` is the only other universally-required key, and `:adapter` is derived
  from it unless `storage:` names one — so it is `nil` only when `:repo` is.
  `:lock` is log-side and
  `DMEx.Log` is its only writer;
  `:prefix` is the optional schema both sides may live in. Nothing about
  notification is here — that is `t:DMEx.Notifier.opts/0`.

  The map is open, because these are the keys *DMEx* knows about and an adapter
  is free to need others. One that does must still accept an `opts` without
  them, which is what reading its own keys rather than matching on them buys.
  """
  @type opts :: %{
          required(:adapter) => module() | nil,
          required(:repo) => module(),
          optional(:lock) => {integer(), integer()},
          optional(:prefix) => String.t() | nil,
          optional(atom()) => term()
        }

  @typedoc """
  One row of a log, as `c:read/5` returns it.

  `:data` and `:metadata` are JSON text as **binaries**, never iodata: iodata
  belongs to the frame, which is bytes on their way out, where a row's fields are
  stored values. One type serves both readers — a transport splices it into its
  own iodata, a pipeline hands it to `JSON.decode!/1`.

  `:inserted_at` is a `DateTime.t()` and never a `NaiveDateTime.t()`: a naive
  timestamp renders without the trailing `Z`, which would silently change every
  frame on the wire.
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

  Must make id assignment order equal commit order. An engine that assigns ids
  at INSERT and makes rows visible at COMMIT can commit two appends out of
  allocation order, and a reader scanning `id > cursor ORDER BY id` then
  advances straight over the row that had not committed yet and **loses it
  permanently and silently** — `last_seen` has moved past the gap.
  `DMEx.Storage.Postgres` buys the order with `pg_advisory_xact_lock`, never
  `pg_advisory_lock` and never released explicitly; an engine with one writer
  database-wide, as SQLite has, satisfies the contract with no lock at all. See
  *Appends are serialized per app* in `ARCHITECTURE.md` for the trade.

  `data` and `metadata` arrive as already-encoded JSON binaries, handed to the
  database without decoding, and nothing DMEx owns is merged into `metadata`.

  A caller already inside a transaction holds whichever of those two an adapter
  uses — the advisory lock, the write transaction — for the enclosing
  transaction's lifetime, serializing every other append to that table for its
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
      `max_rows` is not advisory either — see `max_batch_rows` in `DMEx.Config`
      for what it buys that a budget in database octets cannot.

    * **The byte bound is inclusive, with one exception.** The rows never total
      more than `max_bytes` — except that at least one row must always come
      back, even one larger than the whole budget on its own, since refusing it
      would wedge the subscription forever.

    * **`types` is two statements, not one.** `:all` for a whole-log
      subscription, or a list already expanded by `DMEx.Log.Plug`. A
      `($4 IS NULL OR type = ANY($4))` shape is a correct query and a
      ruinous plan, defeating the index on both branches.

    * **`data` and `metadata` come back as JSON *text*.**
      `DMEx.Transport.SSE.Frame.encode_row/1` splices those columns into the
      wire frame byte-for-byte, so any cast belongs in the query (`data::text`)
      rather than in post-processing, and an engine storing them as text pays
      nothing at all. An adapter that lets its driver decode a JSON column into
      a map hands `Frame` a map where it expects text.

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
  module, so renaming the module does not mint a new subscription.

  It returns `nil`, not `0`, for a subscription that has never committed
  anything: conflating the two is what would stop the `last_seen:` option
  applying exactly once.
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

  **`:ok` or a raise**, like every other write here — see *how a write fails*
  above.
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
  `t:dead_row/0`. Both shipped ones share one statement whose head is the whole
  row shape, so a row short of a key raises rather than being written
  half-formed.

  **Nothing about the failure is stored**, which is what lets
  `c:list_dead_messages/2` return the row as an ordinary `t:row/0`.

  Recording is an **upsert on `{subscription, id}`**, because a cursor moves both
  ways: `DMEx.Handler.Cursors.reset!/2` below a dead-lettered id re-delivers that
  message, and a second failure has to be recordable or the subscription
  crash-loops on a row it already gave up on once. The later attempt wins and
  restamps `inserted_at`, which is when the row last landed here.
  """
  @callback record_dead_message(dead_row(), opts()) :: :ok

  @doc """
  Lists every message `subscription` gave up on, oldest first, as log rows.

  `t:row/0` shape — what `c:read/5` returns and the pipeline consumes — because
  the stored columns are the log's columns, so replay needs no reshaping.

  One column means something else here. `:inserted_at` is when the row landed in
  `dmex_dead_messages`, not when the message was appended; that append time is
  deliberately not carried over, because pruning this table needs an axis
  answering "how long has this been sitting here", which append time cannot.
  """
  @callback list_dead_messages(subscription :: String.t(), opts()) ::
              {:ok, [row()]} | {:error, term()}

  @doc """
  Removes one dead message, by subscription and log id.

  Called inside `c:transaction/2` by a replay, so a message is dropped exactly
  when its handler finally accepted it.
  """
  @callback delete_dead_message(subscription :: String.t(), id :: non_neg_integer(), opts()) :: :ok

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

  Distinct from advancing it to 0 — `DMEx.Handler.Cursors.reset!/2` covers the
  difference.
  """
  @callback delete_cursor(subscription :: String.t(), opts()) :: :ok

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
  The module holding this adapter's DDL, for `DMEx.Storage.Migration`.

  The three tables are the behaviour's — `DMEx.Storage.__table__/1` names them
  and an engine does not get to disagree — but the DDL that creates them is the
  engine's own, down to which spellings are re-runnable. So the engine names the
  module, for the reason `c:default_notifier/0` gives: otherwise the choice is a
  `case` somewhere central that a third-party adapter could never appear in.

  Optional. `DMEx.Storage.Migration` raises for an adapter without one, since
  there is nothing else it could run.
  """
  @callback migration() :: module()

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

  @optional_callbacks default_notifier: 0, migration: 0, validate!: 1

  # The shipped adapter for each Ecto adapter, and the only place that mapping
  # lives. A default, not the selection axis — that is `storage:`.
  @shipped %{
    Ecto.Adapters.Postgres => DMEx.Storage.Postgres,
    Ecto.Adapters.SQLite3 => DMEx.Storage.SQLite
  }

  @doc """
  The shipped adapter for `repo`'s engine, for a caller that names none.

  One owner, and one *source*: a log, a supervisor and a migration all derive it
  from the repo they already hold, so none of them can disagree with another.
  Two independent defaults could, silently — an owned log would serve from one
  adapter while its subscription's cursor was written through a second.
  """
  @spec default_adapter(module()) :: module()
  def default_adapter(repo) when is_atom(repo) do
    ecto = ecto_adapter!(repo)

    Map.get_lazy(@shipped, ecto, fn ->
      raise ArgumentError,
            "DMEx ships no storage adapter for #{inspect(ecto)}, so #{inspect(repo)} has to " <>
              "name the one to use, e.g. `storage: MyApp.Storage`."
    end)
  end

  # `Code.ensure_compiled/1` for the reason `optional/3` gives: `DMEx.Log`
  # resolves this at compile time, from inside a compilation that may still be
  # building the repo.
  defp ecto_adapter!(repo) do
    if match?({:module, _}, Code.ensure_compiled(repo)) and
         function_exported?(repo, :__adapter__, 0) do
      repo.__adapter__()
    else
      raise ArgumentError,
            "#{inspect(repo)} is not an Ecto repo, so DMEx cannot derive a storage adapter " <>
              "from it. Pass one built with `use Ecto.Repo`, or name the adapter with `storage:`."
    end
  end

  @doc """
  Builds a `t:opts/0` from `storage:`, `repo:` and `prefix:`.

  The one place those three become a handle, so the log side and the consumer side
  cannot disagree about the keys. `:repo` may be `nil` here — a supervisor started
  with no default is legal, and it is `DMEx.Supervisor.init/1` that seeds whatever
  it got as the registry metadata a later `subscribe/2` falls back to. There is
  nothing to derive an adapter from in that case, so `:adapter` is `nil` with it.

  `DMEx.Log` adds `:lock`, which is the one key only an appender needs.
  """
  @spec opts(keyword()) :: opts()
  def opts(opts) when is_list(opts) do
    repo = Keyword.get(opts, :repo)

    %{
      adapter: Keyword.get_lazy(opts, :storage, fn -> if repo, do: default_adapter(repo) end),
      repo: repo,
      prefix: Keyword.get(opts, :prefix)
    }
  end

  ## Calling an adapter

  # `c:default_notifier/0` and `c:migration/0` are the two callbacks with no
  # function here, both because their callers hold no `t:opts/0`: `DMEx.Log` asks
  # the module for a notifier while it is still building the map, and a migration
  # runs in the host's `Ecto.Migration` with only the repo to hand.

  @doc "Appends one message. See `c:append/4`."
  @spec append(opts(), String.t(), binary(), binary()) :: {:ok, pos_integer()} | {:error, term()}
  def append(%{adapter: adapter} = storage, type, data, metadata), do: adapter.append(type, data, metadata, storage)

  @doc "Reads a bounded batch of rows past `cursor`. See `c:read/5`."
  @spec read(opts(), non_neg_integer(), pos_integer(), pos_integer(), :all | [String.t()]) ::
          {:ok, [row()]} | {:error, term()}
  # An empty selection admits nothing, which is a property of the call rather than
  # of an engine: it needs no statement, and one adapter would otherwise build
  # `IN ()` out of it while the other round-tripped an empty array to learn the
  # same thing.
  def read(_storage, _cursor, _max_rows, _max_bytes, []), do: {:ok, []}

  def read(%{adapter: adapter} = storage, cursor, max_rows, max_bytes, types),
    do: adapter.read(cursor, max_rows, max_bytes, types, storage)

  @doc "The log's current highest id. See `c:max_id/1`."
  @spec max_id(opts()) :: {:ok, non_neg_integer()} | {:error, term()}
  def max_id(%{adapter: adapter} = storage), do: adapter.max_id(storage)

  @doc "What was appended past `cursor`. See `c:appended_since/2`."
  @spec appended_since(opts(), non_neg_integer()) ::
          {:ok, {non_neg_integer() | nil, [String.t()]}} | {:error, term()}
  def appended_since(%{adapter: adapter} = storage, cursor), do: adapter.appended_since(cursor, storage)

  @doc "A subscription's durable read position. See `c:last_seen/2`."
  @spec last_seen(opts(), String.t()) :: {:ok, non_neg_integer() | nil} | {:error, term()}
  def last_seen(%{adapter: adapter} = storage, subscription), do: adapter.last_seen(subscription, storage)

  @doc "Runs `fun` in a transaction. See `c:transaction/2`."
  @spec transaction(opts(), (-> result)) :: {:ok, result} | {:error, term()} when result: var
  def transaction(%{adapter: adapter} = storage, fun), do: adapter.transaction(fun, storage)

  @doc "Moves a subscription's cursor. See `c:advance_cursor/3`."
  @spec advance_cursor(opts(), String.t(), non_neg_integer()) :: :ok
  def advance_cursor(%{adapter: adapter} = storage, subscription, last_seen),
    do: adapter.advance_cursor(subscription, last_seen, storage)

  @doc "Records a message its handler gave up on. See `c:record_dead_message/2`."
  @spec record_dead_message(opts(), dead_row()) :: :ok
  def record_dead_message(%{adapter: adapter} = storage, dead), do: adapter.record_dead_message(dead, storage)

  @doc "Every message a subscription gave up on. See `c:list_dead_messages/2`."
  @spec list_dead_messages(opts(), String.t()) :: {:ok, [row()]} | {:error, term()}
  def list_dead_messages(%{adapter: adapter} = storage, subscription),
    do: adapter.list_dead_messages(subscription, storage)

  @doc "Removes one dead message. See `c:delete_dead_message/3`."
  @spec delete_dead_message(opts(), String.t(), non_neg_integer()) :: :ok
  def delete_dead_message(%{adapter: adapter} = storage, subscription, id),
    do: adapter.delete_dead_message(subscription, id, storage)

  @doc "A subscription's full cursor row. See `c:cursor/2`."
  @spec cursor(opts(), String.t()) ::
          {:ok, %{required(:last_seen) => non_neg_integer(), optional(atom()) => term()} | nil}
          | {:error, term()}
  def cursor(%{adapter: adapter} = storage, subscription), do: adapter.cursor(subscription, storage)

  @doc "Removes a subscription's cursor. See `c:delete_cursor/2`."
  @spec delete_cursor(opts(), String.t()) :: :ok
  def delete_cursor(%{adapter: adapter} = storage, subscription), do: adapter.delete_cursor(subscription, storage)

  @doc """
  Calls an optional callback on `adapter`, or answers `:none`.

  The three optional callbacks are asked in three places — a log building its
  config asks for `c:default_notifier/0`, a migration asks for `c:migration/0`,
  a starting tree asks for `c:validate!/1` — and every one of them needs the
  same two-step probe rather than a bare `function_exported?/3`.

  `function_exported?/3` alone answers `false` for a module that is merely not
  *loaded*, which every one of these callers can hit: a storage handle is built
  at compile time out of an atom, so in interactive mode nothing has referenced
  the adapter before the first of these asks. Unguarded, an implemented
  callback reads as absent — silently, since absent is the legal answer.
  `Code.ensure_compiled/1` rather than `ensure_loaded?/1` because a host may ask
  from inside the compilation still building the adapter, and only this form
  waits for it.

  What each caller does with `:none` differs — one skips, two raise — so that
  stays theirs.
  """
  @spec optional(module(), atom(), [term()]) :: {:ok, term()} | :none
  def optional(adapter, callback, args) when is_atom(callback) and is_list(args) do
    if is_atom(adapter) and match?({:module, _}, Code.ensure_compiled(adapter)) and
         function_exported?(adapter, callback, length(args)) do
      {:ok, apply(adapter, callback, args)}
    else
      :none
    end
  end

  @doc """
  Runs the adapter's `c:validate!/1` if it has one, and returns `:ok`.

  Here rather than on either caller because both halves of DMEx have one — a log
  when its tree starts, a subscription when its storage is resolved — and
  neither of them is a log's business, which is why this is not on `DMEx.Log`.
  """
  @spec validate!(opts()) :: :ok
  def validate!(%{adapter: adapter} = storage) do
    _ = optional(adapter, :validate!, [storage])

    :ok
  end

  ## Table names

  # The three table names, stated once: both adapters' SQL and both migrations'
  # DDL render these rather than repeating the literals. Here rather than on an
  # adapter because the names are the *behaviour's* — an engine implements these
  # tables, it does not get to pick what they are called.
  #
  # Atoms, because `Ecto.Migration.table/2` wants one and every other site
  # interpolates, where an atom renders as its own name.
  @doc false
  @spec __table__(:messages | :cursors | :dead_messages) :: atom()
  def __table__(:messages), do: :dmex_messages
  def __table__(:cursors), do: :dmex_cursors
  def __table__(:dead_messages), do: :dmex_dead_messages
end
