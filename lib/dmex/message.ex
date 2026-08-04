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
  them and generates the `defstruct`, the `@derive JSON.Encoder`, `c:type/0`,
  `c:cast/1`, and a `t/0` type whose fields are all `term()` — a field list says
  what the fields are and never what they hold.
  Omit `fields:` entirely for a message carrying no data.

  Because `@derive` and `t/0` are generated with the struct, a message module
  must define neither itself: `@derive` only takes effect before `defstruct`,
  and a second `@type t` is a redefinition. A module defined at *runtime* is the
  one place the derivation cannot land, since protocols are consolidated by
  then; such a module still casts, it just cannot be encoded.

  ## `c:type/0` — the wire type

  Declared, not derived. The string is the `message_type` column, the SSE
  `event:` name, and the key a consumer resolves a row back to a module with:

      MyApp.Messages.OrderPlaced.type()
      #=> "order_placed"

  It is deliberately independent of the module name, so a message module can be
  renamed, moved between namespaces or reorganised without any of that reaching
  the wire or the database — and so a producer and a consumer can be refactored
  on their own schedules.

  **This is the one line in a message module that must not be edited casually.**
  Changing it orphans every row already appended under the old type: a consumer
  stops resolving them and skips them silently, exactly the way renaming the
  module used to. There is no rename tooling.

  Restricted to `[A-Za-z0-9_.]+`, because the type goes onto an SSE `event:` line
  and SSE field lines have no escaping. Dots are available for namespacing —
  `"acme.orders.placed"` — which is worth using: two producers declaring a bare
  `"order_placed"` collide on a consumer that reads both, and nothing outside a
  single node can detect that.

  ## `c:cast/1` — the read-side inverse

  Turns a row's JSON-decoded `data` map back into the struct. The generated
  default maps string keys onto the declared fields, keeps the struct's default
  for anything absent, and ignores anything undeclared.

  It is **overridable**, because the default can only be right for what JSON
  models directly. A field holding a `DateTime`, a `Decimal`, or a nested struct
  round-trips as a string or a plain map and needs its own `cast/1`:

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

  This mirrors `@derive JSON.Encoder` on the writing side: a default covering
  the common shape, replaced where the type demands it.

  **Atom safety.** The default `cast/1` walks the struct's *own* fields and
  looks each up in the incoming map, rather than converting the incoming map's
  keys and checking whether they are fields. The two read as equivalent and are
  not — only the first is incapable of growing the atom table from
  attacker-controlled input, with no `String.to_existing_atom/1` and no rescue
  clause needed.
  """

  @message_opts [:type, :fields]
  @type_format ~r/\A[A-Za-z0-9_.]+\z/

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

  defmacro __using__(opts) do
    module = __CALLER__.module

    validate_keys!(module, opts)

    type = fetch_type!(module, opts)
    fields = opts |> Keyword.get(:fields, []) |> validate_fields!()
    struct_type = struct_type(fields)

    quote do
      @behaviour DMEx.Message

      @dmex_message_type unquote(type)

      # Every appended message has to be JSON-encodable, and the derive has to
      # precede the `defstruct`.
      @derive JSON.Encoder
      defstruct unquote(fields)

      @typedoc "The #{inspect(__MODULE__)} message."
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

  defp validate_keys!(module, opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError,
            "`use DMEx.Message` takes a keyword list in #{inspect(module)}, " <>
              "got: #{Macro.to_string(opts)}"
    end

    case Enum.reject(Keyword.keys(opts), &(&1 in @message_opts)) do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "unknown option#{if length(unknown) > 1, do: "s"} " <>
                "#{inspect(unknown)} passed to `use DMEx.Message` in " <>
                "#{inspect(module)}. Known options: #{inspect(@message_opts)}"
    end
  end

  # Atoms and binaries must render identically — otherwise a style change at the
  # declaration site would silently mint a second wire type, orphaning every row
  # already appended under the first.
  defp fetch_type!(module, opts) do
    case Keyword.fetch(opts, :type) do
      {:ok, value} -> type!(module, value)
      :error -> raise_missing_type(module)
    end
  end

  defp type!(module, value) do
    value =
      case value do
        value when is_atom(value) and not is_nil(value) -> Atom.to_string(value)
        value when is_binary(value) -> value
        other -> raise_type(module, other)
      end

    unless Regex.match?(@type_format, value), do: raise_type(module, value)

    value
  end

  defp raise_missing_type(module) do
    raise ArgumentError,
          "`use DMEx.Message` requires :type in #{inspect(module)}, e.g. " <>
            "`use DMEx.Message, type: \"order_placed\", fields: [:order_id]`. The type is " <>
            "the message's durable identity — the module name is deliberately not it, so " <>
            "that renaming or moving this module never reaches the wire."
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
  The set of types `modules` declare, raising if two of them declare one type.

  A `MapSet` of the types alone would collapse a duplicate without a word, and a
  swallowed collision is silent message loss: whichever module lost the race is
  unreachable, and every row of that type is skipped by every consumer.

  Checked once at boot rather than at compile time — a collision is a property of
  the module *set*, which no single module's compilation can see.
  """
  @spec types!([module()]) :: MapSet.t(String.t())
  def types!(modules) when is_list(modules) do
    modules |> index!() |> Map.keys() |> MapSet.new()
  end

  defp index!(modules) do
    Enum.reduce(modules, %{}, fn module, acc ->
      type = module.type()

      case acc do
        %{^type => already} -> raise_collision(type, already, module)
        _ -> Map.put(acc, type, module)
      end
    end)
  end

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
