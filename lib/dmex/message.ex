defmodule DMEx.Message do
  @moduledoc """
  Defines a message type.

  Each message a producer emits is its own Elixir module — one struct per
  message type, owned by the appending application:

      defmodule MyApp.Messages.OrderPlaced do
        use DMEx.Message,
          type: "order_placed",
          fields: [:order_id, :customer_id, :total]
      end

  `use DMEx.Message` takes `fields:` in exactly the shape `defstruct/1` takes
  them and generates the `defstruct`, the `@derive JSON.Encoder`, `c:type/0` and
  `c:cast/1`. Omit `fields:` entirely for a message carrying no data.

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

  Declared, not derived. The string is the `type` column, the SSE
  `event:` name, and the key a consumer resolves a row back to a module with:

      MyApp.Messages.OrderPlaced.type()
      #=> "order_placed"

  It is deliberately independent of the module name, so a message module can be
  renamed, moved between namespaces or reorganised without any of that reaching
  the wire or the database — and so a producer and a consumer can be refactored
  on their own schedules.

  **This is the one line in a message module that must not be edited in place.**
  Replacing it orphans every row already appended under the old string: a
  consumer stops resolving them and skips them silently, exactly the way
  renaming the module used to. `retired:` is how a type is changed instead.

  Restricted to `[A-Za-z0-9_]+` segments joined by dots, because the type goes
  onto an SSE `event:` line and SSE field lines have no escaping. Dots are
  available for namespacing —
  `"acme.orders.placed"` — which is worth using: two producers declaring a bare
  `"order_placed"` collide on a consumer that reads both, and nothing outside a
  single node can detect that.

  ## `retired:` — the types this module used to append under

      use DMEx.Message,
        type: "acme.orders.placed",
        retired: ["order_placed"],
        fields: [:order_id]

  A retired type resolves to this module everywhere a current one does: it is a
  key in the node's index, a member of `types/0`, a `cast/1` clause on every
  handler matching this struct, and a member of that handler's selection
  fingerprint — so a subscription resuming from a cursor older than the rename
  reads its own history. What it is not is a second thing to append under:
  `type/0` is still one string, and it is the one that reaches the `type`
  column.

  Because the log is append-only, **a retirement is as permanent as the type it
  replaces**. The rows stay resolvable only while the declaration that resolves
  them is still compiled, so dropping the entry later re-orphans exactly the
  rows it was added for.

  It does not migrate the payload. A row older than the rename may also be older
  than the current `fields:`, and reshaping it is `c:cast/1`'s job — the same
  seam a field change already uses, now reached through a second type string.

  A retired type collides node-wide like any other: two modules cannot both
  answer for one string, whether either claims it as current or retired.

  ## `c:cast/1` — the read-side inverse

  Turns a row's JSON-decoded `data` map back into the struct. The generated
  default maps string keys onto the declared fields, keeps the struct's default
  for anything absent, and ignores anything undeclared.

  It is **overridable**, because the default can only be right for what JSON
  models directly. A `DateTime` or a `Decimal` field comes back as a string, a
  nested struct as a plain map, and each needs its own `cast/1`:

      defmodule MyApp.Messages.OrderPlaced do
        use DMEx.Message, type: "order_placed", fields: [:order_id, :placed_at]

        @impl true
        def cast(%{"placed_at" => placed_at} = data) do
          %__MODULE__{
            order_id: data["order_id"],
            placed_at: DateTime.from_iso8601(placed_at) |> elem(1)
          }
        end
      end

  The write side is the stricter half, and this is the invariant it imposes:
  **a message field must hold something `JSON.Encoder` already encodes.** A map,
  a list, a string, a number, a boolean, `nil`, or a struct implementing the
  protocol — `Date`, `DateTime`, `Time`, `NaiveDateTime`, `Duration`, and
  `Decimal` where that dependency is present. A nested struct that derives
  nothing is the case that bites: `JSON.encode!/1` raises
  `Protocol.UndefinedError` on it inside `MyApp.Log.append/2`, before the row is
  written and before the `[:dmex, :append, :start]` event. Enrichment past that
  line belongs on the read side, in `cast/1`.

  The derivation itself is replaceable, just not with a second `@derive`. An
  explicit `defimpl JSON.Encoder, for: __MODULE__` wins over it, whichever order
  the two compile in, and is the only way to change what a message encodes to.

  **Atom safety.** The default `cast/1` walks the struct's *own* fields and
  looks each up in the incoming map, rather than converting the incoming map's
  keys and checking whether they are fields. The two read as equivalent and are
  not — only the first is incapable of growing the atom table from
  attacker-controlled input, with no `String.to_existing_atom/1` and no rescue
  clause needed.
  """

  @message_opts [:type, :fields, :retired]
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

  defmacro __using__(opts) do
    module = __CALLER__.module

    DMEx.validate_keys!(
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

  @doc """
  Returns whether `module` was built with `use DMEx.Message`.

  The mirror of `DMEx.Handler.handler?/1`: a handler matching on a struct that
  is not a message should be rejected by name rather than blow up as an
  `UndefinedFunctionError` on `type/0`.

  Tested by the `@dmex_message` attribute `use DMEx.Message` persists into the
  BEAM, not by the presence of `type/0` and `cast/1`. Those two are a shape a
  module can have by coincidence, and every module of every application
  depending on `:dmex` is offered to this predicate — a stranger admitted here
  has its `type/0` called and the result folded into the node's type set, where
  at worst it collides with a real message and fails the boot in
  `index!/1` naming a module that has nothing to do with DMEx.

  `module_info/1` rather than `__info__/1`, since the scan reaches Erlang
  modules too.

  `Code.ensure_loaded?/1`, so this never blocks. `DMEx.Handler` calls it at
  compile time and does its own `Code.ensure_compiled!/1` first, since the
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

  What turns a `type` off the wire back into a module without
  `String.to_atom/1`.

  It raises rather than letting the second module win, because a swallowed
  collision is silent message loss: whichever module lost is unreachable, and
  every row of that type is skipped by every consumer.

  Checked once at boot rather than at compile time — a collision is a property of
  the module *set*, which no single module's compilation can see.
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

  @doc """
  `module`, or a raise when it is not the module already holding `type`.

  The merge step of `refresh_index/1`, where the two sides are separate scans
  rather than two modules in one: a rescan of an application meets its own
  modules again and must stay quiet, a second module claiming the type must not.
  """
  @spec collision!(String.t(), module(), module()) :: module()
  def collision!(_type, module, module), do: module
  def collision!(type, already, module), do: raise_collision(type, already, module)

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
