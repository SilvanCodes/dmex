defmodule DMEx.Message do
  @moduledoc """
  Defines a message type.

  One struct per message type, owned by the appending application:

      defmodule MyApp.Messages.OrderPlaced do
        use DMEx.Message,
          type: "order_placed",
          fields: [:order_id, :customer_id, :total]
      end

  `fields:` is exactly what `defstruct/1` takes; it generates the `defstruct`,
  the `@derive JSON.Encoder`, `c:type/0` and `c:cast/1`. Omit it entirely for a
  message carrying no data.

  No `t/0` is generated. A field list says what the fields are and never what
  they hold, so a generated one could only say `term()` per field — and since a
  second `@type t` is a redefinition, generating that would forbid the module
  from declaring the informative version itself. Write it where it is worth
  having: `@type t :: %__MODULE__{order_id: pos_integer()}`.

  Because `@derive` is generated with the struct, a message module must not
  repeat it: `@derive` only takes effect before `defstruct`. A module defined at
  *runtime* is the one place the derivation cannot land, since protocols are
  consolidated by then; such a module still casts, it just cannot be encoded.

  ## `c:type/0` — the wire type

  Declared, not derived: the string is the `type` column, the SSE `event:` name,
  and the key a consumer resolves a row back to a module with. It is
  deliberately independent of the module name, so a message module can be
  renamed or moved without that reaching the wire or the database, and a producer
  and a consumer can be refactored on their own schedules.

  **This is the one line in a message module that must not be edited in place.**
  Replacing it orphans every row already appended under the old string: a
  consumer stops resolving them and skips them silently. `retired:` is how a
  type is changed instead.

  The charset — `[A-Za-z0-9_]+` segments joined by single dots — is enforced
  here, because an SSE `event:` line has no escaping; the `type` column's CHECK
  constraint is a looser backstop for rows that never came through here, not a
  second definition. Namespacing with those dots is worth doing: two producers
  declaring a bare `"order_placed"` collide on a consumer that reads both, and
  nothing outside a single node can detect that.

  ## `retired:` — the types this module used to append under

  `retired: ["order_placed"]` on the module now declaring
  `type: "acme.orders.placed"` resolves the old string to this module everywhere
  the current one resolves — a key in the node's index, a member of `types/0`, a
  `cast/1` clause on every handler matching this struct, a member of that
  handler's selection fingerprint — so a subscription resuming from a cursor
  older than the rename reads its own history. What it is not is a second thing
  to append under: `type/0` is still one string, and it is the one that reaches
  the `type` column.

  Because the log is append-only, **a retirement is as permanent as the type it
  replaces**: dropping the entry later re-orphans exactly the rows it was added
  for. It does not migrate the payload either — a row older than the rename may
  also be older than the current `fields:`, and reshaping it is `c:cast/1`'s job.
  And it collides node-wide like any other type: two modules cannot both answer
  for one string, whether either claims it as current or retired.

  ## `entities:` — what the message is about

  `entities: [:order_id, :customer_id]` declares which of this message's fields
  hold entity identifiers. Each one is indexed at append time, which is what makes
  "every message concerning this order" a query — and a query across message
  types, since DMEx composes nothing into the identifier it stores.

  `c:entities/1` is generated and **not overridable**, which is the whole point of
  it: it is a projection of the struct's own fields and nothing else, so the entity
  set of any row is recomputable *from the row*. A body free to return a derived
  value, a clock or a service call would make the index a second source of truth
  rather than a materialized one.

  There is no `defoverridable`, and that is the enforcement: the generated clause
  matches this module's own struct and is defined first, so a clause written
  afterwards is unreachable for every message of this type. Elixir warns about
  the ungrouped clauses; nothing else has to.

  Which is what makes the index **rebuildable rather than migratable**:
  `DMEx.Storage.Migration.reindex_entities/1` throws the table away and recomputes
  it from the log. Changing an `entities:` declaration only reaches messages
  appended after it — the rows already in the log keep whatever was indexed at the
  time — so that migration is how the change is applied to history.

  Three rules, and they are the whole of it:

    * A **list** value contributes each element, so one message can concern many
      entities of one kind.
    * A `nil` is **skipped**. Several declared fields with some of them unset is
      ordinary, and the field is already known to exist — so this is the
      publisher's data rather than a typo.
    * Anything else that is not a non-empty string **raises**. An identifier is
      stored verbatim and never namespaced by DMEx: a UUID is already unique, and
      whatever namespacing a publisher wants lives *inside* the value. That is what
      lets two message types join on one identifier, and a non-string id cannot
      offer it.

  What the declaration cannot express is deliberate — a value nested below the
  struct's own fields, and a constant tag, which is not an entity at all. Both are
  data.

  Entities are an index on the log and never part of a message: none of this
  reaches the wire, and a handler is never given one.

  ## `c:cast/1` — the read-side inverse

  `cast_default/2` is the generated default, and it is **overridable**: that is
  where a field JSON does not model directly — a `DateTime`, a `Decimal`, a
  nested struct — is rebuilt.

  The write side is the stricter half, and this is the invariant it imposes:
  **a message field must hold something `JSON.Encoder` already encodes.** A
  nested struct that derives nothing is the case that bites: `JSON.encode!/1`
  raises `Protocol.UndefinedError` on it inside `MyApp.Log.append/2`, before the
  row is written and before the `[:dmex, :append, :start]` event. Enrichment past
  that line belongs on the read side, in `cast/1`. An explicit
  `defimpl JSON.Encoder, for: __MODULE__` is the only way to change what a
  message encodes to.

  **Atom safety.** The default `cast/1` walks the struct's *own* fields and
  looks each up in the incoming map, rather than converting the incoming map's
  keys and checking whether they are fields. The two read as equivalent and are
  not — only the first is incapable of growing the atom table from
  attacker-controlled input, with no `String.to_existing_atom/1` and no rescue
  clause needed.
  """

  alias DMEx.Config

  @message_opts [:type, :fields, :retired, :entities]
  # Dot-separated segments rather than "the dot is a legal character", so the
  # degenerate namespaces — `"."`, `".foo"`, `"foo."` — are illegal by
  # construction rather than merely never written.
  @type_format ~r/\A[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)*\z/

  # One key for the node, not one per application: see `types/0`.
  @index_key {__MODULE__, :index}

  @doc """
  The message's wire type — the string it declared.

  Generated at compile time by `use DMEx.Message`.
  """
  @callback type() :: String.t()

  @doc """
  Builds the struct from a row's JSON-decoded `data` map.

  Generated with an overridable default by `use DMEx.Message`.
  """
  @callback cast(data :: map()) :: struct()

  @doc """
  The wire types this module used to declare and still answers for.

  `[]` unless `use DMEx.Message` was given `retired:`. Generated at compile time.
  """
  @callback retired() :: [String.t()]

  @doc """
  The entity identifiers this message carries — the values of its `entities:` fields.

  Generated at compile time by `use DMEx.Message` and **not overridable**: see the
  moduledoc for why a projection of the struct's own fields is the only thing this
  may be.
  """
  @callback entities(message :: struct()) :: [String.t()]

  defmacro __using__(opts) do
    module = __CALLER__.module

    Config.keys!(
      opts,
      @message_opts,
      "`use DMEx.Message` in #{inspect(module)}",
      " A message module is `use DMEx.Message, type: \"order_placed\", fields: [:order_id]`. " <>
        "The type is the message's durable identity — the module name is deliberately not it, " <>
        "so that renaming or moving this module never reaches the wire.",
      [:type]
    )

    type = type!(module, Keyword.fetch!(opts, :type))
    fields = opts |> Keyword.get(:fields, []) |> validate_fields!()
    retired = retired!(module, type, Keyword.get(opts, :retired, []))
    entities = entities!(module, fields, Keyword.get(opts, :entities, []))

    quote do
      @behaviour DMEx.Message

      # Persisted, so `DMEx.Message.message?/1` can recognise a message module by
      # a mark only this macro writes rather than by the shape of its exports.
      # A mark and nothing more: the wire type has one home, `type/0`.
      Module.register_attribute(__MODULE__, :dmex_message, persist: true)
      @dmex_message true

      # Every appended message has to be JSON-encodable, and the derive has to
      # precede the `defstruct`.
      @derive JSON.Encoder
      defstruct unquote(fields)

      @impl true
      def type, do: unquote(type)

      @impl true
      def retired, do: unquote(retired)

      # No `defoverridable`, which is what makes this unoverridable rather than
      # merely undocumented: this clause matches `%__MODULE__{}` and comes first,
      # so a clause added below it never answers for this message. The moduledoc
      # has the argument for why nothing else may.
      @impl true
      def entities(%__MODULE__{} = message), do: DMEx.Message.__entities__(message, unquote(entities))

      @impl true
      def cast(data) when is_map(data), do: DMEx.Message.cast_default(__MODULE__, data)

      defoverridable cast: 1
    end
  end

  # Atoms and binaries must render identically — otherwise a style change at the
  # declaration site would silently mint a second wire type, orphaning every row
  # already appended under the first.
  defp type!(module, value) do
    type = if is_atom(value) and not is_nil(value), do: Atom.to_string(value), else: value

    if !(is_binary(type) and Regex.match?(@type_format, type)), do: raise_type(module, type)

    type
  end

  # A retirement is as permanent as the type it replaces: the rows appended under
  # it are in an append-only log forever, so the declaration that resolves them
  # has to outlive every one of them. Checked here so a list that would silently
  # resolve nothing — a typo, or the current type restated — is a compile error
  # in the module that wrote it.
  defp retired!(module, type, values) when is_list(values) do
    retired = Enum.map(values, &type!(module, &1))

    cond do
      type in retired ->
        raise ArgumentError,
              ":retired in #{inspect(module)} names #{inspect(type)}, which is its current " <>
                "`type:`. A retired type is one this module *used* to append under and now " <>
                "only reads."

      retired != Enum.uniq(retired) ->
        raise ArgumentError,
              ":retired in #{inspect(module)} repeats a type: #{inspect(retired)}. Each one " <>
                "resolves the same rows to the same module, so a repeat says nothing twice."

      true ->
        retired
    end
  end

  defp retired!(module, _type, other) do
    raise ArgumentError,
          ":retired in #{inspect(module)} takes the types this module used to append under, " <>
            "as a list of atoms or strings. Got: #{inspect(other)}"
  end

  # The fields are checked against `fields:` here so that `entities: [:odrer_id]` is
  # a compile error naming the typo, rather than a `KeyError` from the generated
  # body at the first append. It is also what makes a `nil` at append unambiguous:
  # the field certainly exists, so an absent value is data.
  defp entities!(module, fields, values) when is_list(values) do
    names =
      Enum.map(fields, fn
        {field, _default} -> field
        field -> field
      end)

    Enum.each(values, fn
      field when is_atom(field) ->
        if field not in names do
          raise ArgumentError,
                ":entities in #{inspect(module)} names #{inspect(field)}, which is not one of " <>
                  "its fields: #{inspect(names)}. An entity is the value of a field the message " <>
                  "declares, so `fields:` has to carry it."
        end

      other ->
        raise ArgumentError,
              ":entities in #{inspect(module)} takes the fields holding this message's entity " <>
                "identifiers, as a list of atoms. Got: #{inspect(other)}"
    end)

    if values != Enum.uniq(values) do
      raise ArgumentError,
            ":entities in #{inspect(module)} repeats a field: #{inspect(values)}. Each one is " <>
              "indexed once, so a repeat says nothing twice."
    end

    values
  end

  defp entities!(module, _fields, other) do
    raise ArgumentError,
          ":entities in #{inspect(module)} takes the fields holding this message's entity " <>
            "identifiers, as a list of atoms. Got: #{inspect(other)}"
  end

  defp raise_type(module, value) do
    raise ArgumentError,
          ":type in #{inspect(module)} must be an atom or a string matching " <>
            "#{inspect(@type_format)}, got: #{inspect(value)}. It goes onto an SSE `event:` " <>
            "line, which has no escaping — which rules out dashes, slashes and colons. " <>
            "Dots are available for namespacing: \"acme.orders.placed\"."
  end

  # The fields reach `defstruct` verbatim, so this only turns an error inside
  # the generated code into one that names `use DMEx.Message`. Anything beyond
  # the shape — duplicate keys, say — is left to `defstruct`.
  defp validate_fields!(fields) when is_list(fields) do
    Enum.each(fields, fn
      field when is_atom(field) -> :ok
      {field, _default} when is_atom(field) -> :ok
      other -> raise ArgumentError, bad_fields_message(other)
    end)

    fields
  end

  defp validate_fields!(other), do: raise(ArgumentError, bad_fields_message(other))

  defp bad_fields_message(got) do
    "`fields:` in `use DMEx.Message` takes the message's fields, in the shape defstruct " <>
      "takes them — a list of atoms, a keyword list of defaults, or a mix of the two. Got: " <>
      Macro.to_string(got)
  end

  @doc """
  Builds a struct from `data` — the default `c:cast/1` implementation.

  Walks the struct's declared fields and takes each value from `data` when
  present, leaving the struct's own default otherwise. Undeclared keys in `data`
  are ignored. Iterating the *fields* rather than the *data* is what keeps this
  incapable of creating atoms.
  """
  @spec cast_default(module(), map()) :: struct()
  def cast_default(module, data) when is_atom(module) and is_map(data) do
    base = module.__struct__()

    Enum.reduce(Map.from_struct(base), base, fn {field, _default}, acc ->
      # `Atom.to_string/1` per field per row is the one cost here.
      # `bench/micro/message_cast.exs`: precomputing the field-name strings at
      # `use` time is 1.1-2.1x faster, saving 120-570 ns per row. Not acted on,
      # because the consumer's per-row transaction costs orders of magnitude
      # more. Revisit if per-row commits are ever coalesced.
      case Map.fetch(data, Atom.to_string(field)) do
        {:ok, value} -> Map.put(acc, field, value)
        :error -> acc
      end
    end)
  end

  @doc false
  # The body of every generated `entities/1`. `List.wrap/1` is the whole of the
  # three rules at once: it drops a `nil`, passes a list through, and wraps a
  # scalar. The `reject` catches a `nil` *inside* a list, which the wrap cannot.
  @spec __entities__(struct(), [atom()]) :: [String.t()]
  def __entities__(%module{} = message, fields) when is_list(fields) do
    fields
    |> Enum.flat_map(fn field ->
      message
      |> Map.fetch!(field)
      |> List.wrap()
      |> Enum.reject(&is_nil/1)
      |> Enum.map(&entity!(module, field, &1))
    end)
    # Two fields can hold one identifier, which is data rather than a mistake —
    # unlike a repeat in the *declaration*, which `entities!/3` refuses.
    |> Enum.uniq()
  end

  defp entity!(_module, _field, value) when is_binary(value) and value != "", do: value

  defp entity!(module, field, value) do
    raise ArgumentError,
          "`entities:` in #{inspect(module)} names #{inspect(field)}, which holds " <>
            "#{inspect(value)}. An entity identifier must be a non-empty string: it is stored " <>
            "verbatim and never namespaced by DMEx, which is what lets two message types join " <>
            "on one. Hold it as a string, or leave the field out of `entities:` and let it be " <>
            "ordinary data."
  end

  @doc """
  What an append writes: the type, `data` and `metadata` as JSON text, and the
  entities to index the row under.

  The one place a message becomes a row, as `decode/1` is the one place a row
  becomes a message. `DMEx.Log` holds neither `JSON` call, which is what keeps
  the encoding and the `@derive JSON.Encoder` that makes it possible in one
  module.

  The type comes off `message`'s own module, never from a caller — see the
  moduledoc. `metadata` must be a map: anything else encodes to valid JSON and
  then reads as a malformed row at a consumer, where
  `DMEx.Transport.SSE.Frame.decode/1` requires an object.
  """
  @spec encode!(struct(), map()) :: {String.t(), binary(), binary(), [String.t()]}
  def encode!(message, metadata \\ %{})

  def encode!(%module{} = message, metadata) when is_map(metadata) do
    if !message?(module) do
      raise ArgumentError,
            "#{inspect(module)} is not a DMEx message module, so #{inspect(message)} cannot be " <>
              "appended. Add `use DMEx.Message` to #{inspect(module)}."
    end

    {module.type(), JSON.encode!(message), JSON.encode!(metadata), module.entities(message)}
  end

  def encode!(%_module{}, metadata) do
    raise ArgumentError, "metadata must be a map, got: #{inspect(metadata)}"
  end

  # A non-struct gets the same `ArgumentError` `MyApp.Log.append/2` promises,
  # rather than a `FunctionClauseError` from the head above.
  def encode!(other, _metadata) do
    raise ArgumentError,
          "#{inspect(other)} is not a struct built with `use DMEx.Message`, " <>
            "so it cannot be appended."
  end

  @doc """
  A row's message and its metadata, or `:unknown` for a type this node has no
  module for.

  The inverse of `encode!/2`, and the reason a row needs nothing but itself to be
  read: `type` resolves through `for_type/1`, so the caller supplies no module.
  An unresolvable type is an answer rather than an error — a consumer legitimately
  reads a log wider than its own message modules.

  Takes `data` and `metadata` as JSON text, the shape `t:DMEx.Storage.row/0`
  carries, or as already-decoded maps, the shape a transport yields.
  """
  @spec decode(map()) :: {:ok, struct(), map()} | :unknown
  def decode(row) do
    {type, data, metadata} = columns(row)

    case for_type(type) do
      nil -> :unknown
      module -> {:ok, module.cast(payload(data)), payload(metadata)}
    end
  end

  @doc """
  The same, for a caller that already holds the module.

  `DMEx.Query` does: it built its type filter out of `module` in the first place,
  so resolving the row's type through the node's index would be a second answer
  to a question it had already answered — and one that needs the index to be
  current, where this needs nothing. It cannot fail to resolve, so there is no
  `:unknown`.
  """
  @spec decode(map(), module()) :: {:ok, struct(), map()}
  def decode(row, module) when is_atom(module) do
    {_type, data, metadata} = columns(row)

    {:ok, module.cast(payload(data)), payload(metadata)}
  end

  defp columns(%{type: type, data: data, metadata: metadata}), do: {type, data, metadata}

  defp columns(%{"type" => type, "data" => data, "metadata" => metadata}), do: {type, data, metadata}

  defp payload(json) when is_binary(json), do: JSON.decode!(json)
  defp payload(map) when is_map(map), do: map

  @doc """
  Returns whether `module` was built with `use DMEx.Message`.

  The mirror of `DMEx.Handler.handler?/1`. Tested by the `@dmex_message`
  attribute `use DMEx.Message` persists into the BEAM, not by the presence of
  `type/0` and `cast/1` — a shape any struct can have by coincidence, and every
  module of every application depending on `:dmex` is offered to this predicate.

  `module_info/1` rather than `__info__/1`, since the scan reaches Erlang modules
  too, and `Code.ensure_loaded?/1` so this never blocks: `DMEx.Handler` calls it
  at compile time and does its own `Code.ensure_compiled!/1` first, since the
  parallel compiler resolves a struct pattern without loading the module.
  """
  @spec message?(module()) :: boolean()
  def message?(module) when is_atom(module) do
    Code.ensure_loaded?(module) and
      Keyword.has_key?(module.module_info(:attributes), :dmex_message)
  end

  @doc """
  Lists every message module compiled into the applications `DMEx.discovery/1` scans.

  The mirror of `DMEx.Handler.discover/1`. Sorted, so a collision is reported
  against the same pair of modules on every node.
  """
  @spec discover(atom() | nil) :: [module()]
  def discover(otp_app \\ nil) do
    otp_app
    |> DMEx.discovery()
    |> Enum.filter(&message?/1)
    |> Enum.sort()
  end

  @doc """
  `modules` keyed by the type each declares, raising if two of them declare one.

  What turns a `type` off the wire back into a module without `String.to_atom/1`.
  It raises rather than letting the second module win, because a swallowed
  collision is silent message loss: whichever module lost is unreachable, and
  every row of that type is skipped by every consumer.

  Checked once at boot rather than at compile time — a collision is a property of
  the module *set*, which no single module's compilation can see. This is the
  within-one-scan half of that check and `refresh_index/1`'s merge is the
  across-scans half; deliberately two, so a hand-built module list pins this one
  without going through application discovery.
  """
  @spec index!([module()]) :: %{String.t() => module()}
  def index!(modules) when is_list(modules) do
    Enum.reduce(modules, %{}, fn module, acc ->
      Enum.reduce(declared(module), acc, fn type, acc ->
        case acc do
          %{^type => already} -> raise_collision(type, already, module)
          _ -> Map.put(acc, type, module)
        end
      end)
    end)
  end

  @doc """
  Every type `module` answers for: the one it appends under, then its retired ones.

  The `type/0` first is load-bearing at exactly one call site — `DMEx.Log`
  appends under the head — and everywhere else this is a set.
  """
  @spec declared(module()) :: [String.t()]
  def declared(module) when is_atom(module), do: [module.type() | module.retired()]

  @doc """
  Every type known to the node, from the cached index.

  What a consumer's filter is resolved against: `DMEx.Log.Plug` tests every type
  here for membership once at connect, and the types that pass become the
  connection's `type = ANY($1)` predicate and its wakeup routing set.

  The keys of `index!/1`'s map, asked of the node rather than of a given list of
  modules, and answered from the cache.

  ## Why the *modules*, and not a log's contents

  Expanding over the types a log has actually seen — `SELECT DISTINCT type` —
  goes stale silently: a log's code can know a type it has not yet emitted, and
  first emission involves no restart and no reconnect, so an open connection
  would keep an expansion missing a type its consumer genuinely selected. That
  is a false negative, which is silent message loss.

  Compiled modules have no such gap: that set changes only on deploy, and a
  deploy takes every SSE connection with it, so every affected consumer
  reconnects and re-expands.

  ## Why one set for the whole node, and not one per application

  A type is unique node-wide — `refresh_index/1` raises on a second module
  claiming one — so there is nothing for a per-application key to separate. This
  is never per log either: the set only has to *contain* everything a log might
  append, since an extra type matches no filter and a missing one is silent
  loss.

  The lazy branch keeps every caller free of start order: a node whose
  `DMEx.Supervisor` has not run `refresh_index/1` — or that runs no supervisor
  at all — still answers correctly, just more slowly the first time. It scans
  the applications depending on `:dmex` alone, so an application that does not —
  an umbrella parent — is only ever reached by being named.
  """
  @spec types() :: MapSet.t(String.t())
  def types, do: index() |> Map.keys() |> MapSet.new()

  @doc """
  The module declaring `type`, or `nil`.

  How a handler decodes a type it never named: `DMEx.Handler` compiles a clause
  per type its callback did name, and — for a handler with a clause that names
  none, which asks for the whole log — a fallback that comes here. A **lookup,
  never `String.to_atom/1`** — the key is a string a log sent, so resolving it
  through anything that creates atoms would make an unbounded atom table out of
  attacker-controlled input.

  Same cache and same scan as `types/0`; see there for why the modules rather
  than the log's contents.
  """
  @spec for_type(String.t()) :: module() | nil
  def for_type(type) when is_binary(type), do: Map.get(index(), type)

  @doc """
  Rescans an application's message modules into the node's index, raising if two
  modules declare one type.

  Merges rather than replaces, because the scan an application sees is not the
  whole node: `DMEx.discovery/1` reaches every application depending on `:dmex`
  plus the one named, so an umbrella parent and its children each contribute
  modules the other's scan misses. Replacing would make the last writer at boot
  drop the rest.

  Which is also where the collision check earns its keep: a type already in the
  index under a *different* module raises here, even when no single scan sees
  both — the check is node-wide because the index is.

  Called from `DMEx.Supervisor`'s `init/1` with its `otp_app:` — so an
  application owning no log gets the check too — and once per application owning
  one of its logs.
  """
  @spec refresh_index(atom() | nil) :: %{String.t() => module()}
  def refresh_index(otp_app \\ nil) do
    scanned = otp_app |> discover() |> index!()

    # Read raw rather than through `index/0`, whose miss branch is this function.
    merged = Map.merge(:persistent_term.get(@index_key, %{}), scanned, &collision!/3)

    :persistent_term.put(@index_key, merged)

    merged
  end

  defp index do
    :persistent_term.get(@index_key, nil) || refresh_index()
  end

  # The merge step of `refresh_index/1`, where the two sides are separate scans
  # rather than two modules in one: a rescan of an application meets its own
  # modules again and must stay quiet, a second module claiming the type must not.
  defp collision!(_type, module, module), do: module
  defp collision!(type, already, module), do: raise_collision(type, already, module)

  # Names both modules: "duplicate message type" on its own is a riddle in an
  # application with thirty of them.
  defp raise_collision(type, already, module) do
    raise ArgumentError,
          "message type #{inspect(type)} is declared by both #{inspect(already)} and " <>
            "#{inspect(module)}. A type is how a consumer resolves a row back to a module, " <>
            "so two modules sharing one leaves the rows of whichever lost ambiguous — " <>
            "give one of them a different `type:`."
  end
end
