defmodule DMEx.Message do
  @moduledoc """
  Defines a message type.

  Each message a producer emits is its own Elixir module — one struct per
  message type, owned by the appending application:

      defmodule MyApp.Messages.OrderPlaced do
        use DMEx.Message, [:order_id, :customer_id, :total]
      end

  `use DMEx.Message` takes the fields in exactly the shape `defstruct/1` takes
  them and generates the `defstruct`, the `@derive JSON.Encoder`, `c:type/0`,
  `c:cast/1`, and a `t/0` type whose fields are all `term()` — a field list says
  what the fields are and never what they hold.
  Omit them entirely for a message carrying no data.

  Because `@derive` and `t/0` are generated with the struct, a message module
  must define neither itself: `@derive` only takes effect before `defstruct`,
  and a second `@type t` is a redefinition. A module defined at *runtime* is the
  one place the derivation cannot land, since protocols are consolidated by
  then; such a module still casts, it just cannot be encoded.

  ## `c:type/0` — the wire type

  Derived at compile time from the module's own fully-qualified name, so it
  cannot drift from what the module is called:

      MyApp.Messages.OrderPlaced.type()
      #=> "MyApp.Messages.OrderPlaced"

  The *full* name guarantees uniqueness across every producer in a network, and
  is why nothing records which producer appended a message.

  ## `c:cast/1` — the read-side inverse

  Turns a row's JSON-decoded `data` map back into the struct. The generated
  default maps string keys onto the declared fields, keeps the struct's default
  for anything absent, and ignores anything undeclared.

  It is **overridable**, because the default can only be right for what JSON
  models directly. A field holding a `DateTime`, a `Decimal`, or a nested struct
  round-trips as a string or a plain map and needs its own `cast/1`:

      defmodule MyApp.Messages.OrderPlaced do
        use DMEx.Message, [:order_id, :placed_at]

        @impl true
        def cast(%{"placed_at" => placed_at} = data) do
          %__MODULE__{
            order_id: data["order_id"],
            placed_at: DateTime.from_iso8601(placed_at) |> elem(1)
          }
        end
      end

  This mirrors `@derive JSON.Encoder` on the writing side: a default covering
  the common shape, replaced where the type demands it.

  **Atom safety.** The default `cast/1` walks the struct's *own* fields and
  looks each up in the incoming map, rather than converting the incoming map's
  keys and checking whether they are fields. The two read as equivalent and are
  not — only the first is incapable of growing the atom table from
  attacker-controlled input, with no `String.to_existing_atom/1` and no rescue
  clause needed.
  """

  @doc """
  The message's wire type — its own fully-qualified module name.

  Generated at compile time by `use DMEx.Message`.
  """
  @callback type() :: String.t()

  @doc """
  Builds the struct from a row's JSON-decoded `data` map.

  Generated with an overridable default by `use DMEx.Message`.
  """
  @callback cast(data :: map()) :: struct()

  defmacro __using__(fields) do
    fields = validate_fields!(fields)
    struct_type = struct_type(fields)

    quote do
      @behaviour DMEx.Message

      # At compile time, so `type/0` is a constant rather than an `inspect/1`
      # call on every append.
      @dmex_message_type inspect(__MODULE__)

      # Every appended message has to be JSON-encodable, and the derive has to
      # precede the `defstruct`.
      @derive JSON.Encoder
      defstruct unquote(fields)

      @typedoc "The #{@dmex_message_type} message."
      @type t :: unquote(struct_type)

      @impl true
      def type, do: @dmex_message_type

      @impl true
      def cast(data) when is_map(data), do: DMEx.Message.cast_default(__MODULE__, data)

      defoverridable cast: 1
    end
  end

  # `%__MODULE__{field: term(), ...}` — see the moduledoc for why `term()`.
  defp struct_type(fields) do
    pairs =
      Enum.map(fields, fn
        field when is_atom(field) -> {field, quote(do: term())}
        {field, _default} -> {field, quote(do: term())}
      end)

    quote do
      %__MODULE__{unquote_splicing(pairs)}
    end
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
    "use DMEx.Message takes the message's fields, in the shape defstruct takes them — " <>
      "a list of atoms, a keyword list of defaults, or a mix of the two. Got: " <>
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

    base
    |> Map.from_struct()
    |> Enum.reduce(base, fn {field, _default}, acc ->
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

  `Code.ensure_loaded?/1`, so this never blocks. `DMEx.Handler` calls it at
  compile time and does its own `Code.ensure_compiled!/1` first, since the
  parallel compiler resolves a struct pattern without loading the module.
  """
  @spec message?(module()) :: boolean()
  def message?(module) when is_atom(module) do
    Code.ensure_loaded?(module) and function_exported?(module, :type, 0) and
      function_exported?(module, :cast, 1)
  end

  @doc """
  Builds a `message_type => module` lookup from a set of message modules.

  How a consumer resolves the `message_type` on a row back to a module. Always
  built from the consumer's own declared set — never by atomizing whatever
  string the producer sent.

  Takes the `MapSet` a handler's `:messages` already is, so the only collision
  left to report is the real one: two *different* modules claiming one type,
  which can only happen if `c:type/0` was overridden.
  """
  @spec registry(MapSet.t(module())) :: %{String.t() => module()}
  def registry(modules) when is_struct(modules, MapSet) do
    Enum.reduce(modules, %{}, fn module, acc ->
      type = module.type()

      case acc do
        %{^type => already} ->
          raise ArgumentError,
                "message type #{inspect(type)} is claimed by both " <>
                  "#{inspect(already)} and #{inspect(module)}"

        _ ->
          Map.put(acc, type, module)
      end
    end)
  end
end
