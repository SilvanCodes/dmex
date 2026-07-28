defmodule DMEx.Message do
  @moduledoc """
  Defines a message type.

  Each message a producer emits is its own Elixir module — one struct per
  message type, owned by the publishing application:

      defmodule MyApp.Messages.OrderPlaced do
        use DMEx.Message

        @derive JSON.Encoder
        defstruct [:order_id, :customer_id, :total]
      end

  `use DMEx.Message` takes no options. It gives the module two callbacks and
  nothing else.

  ## `c:type/0` — the wire type

  Derived automatically from the module's own full, fully-qualified name at
  compile time, so it cannot drift from what the module is actually called:

      MyApp.Messages.OrderPlaced.type()
      #=> "MyApp.Messages.OrderPlaced"

  The *full* name is what guarantees uniqueness across every producer in a
  network — two unrelated applications can each have their own
  `Messages.OrderPlaced` without their wire types colliding — and it is why
  nothing records "which producer published this": the type string already says.

  ## `c:cast/1` — the read-side inverse

  Turns a row's already-JSON-decoded `data` map back into the struct. The
  generated default maps string keys onto the module's declared fields, keeps
  the struct's default for anything absent, and ignores anything it does not
  declare.

  It is **overridable**, because the default can only be right for what JSON
  models directly. A field holding a `DateTime`, a `Decimal`, or a nested struct
  round-trips as a string or a plain map and needs a `cast/1` of its own:

      defmodule MyApp.Messages.OrderPlaced do
        use DMEx.Message

        @derive JSON.Encoder
        defstruct [:order_id, :placed_at]

        @impl true
        def cast(%{"placed_at" => placed_at} = data) do
          %__MODULE__{
            order_id: data["order_id"],
            placed_at: DateTime.from_iso8601(placed_at) |> elem(1)
          }
        end
      end

  This mirrors `@derive JSON.Encoder` on the writing side: a default that covers
  the common shape, replaced where the type demands it.

  ## Atom safety

  The default `cast/1` never converts a wire string to an atom. It walks the
  struct's *own* fields and looks each one up in the incoming map, rather than
  converting the incoming map's keys and checking whether they are fields. The
  two read as equivalent and are not: only the first is incapable of growing the
  atom table from attacker-controlled input, and it needs no
  `String.to_existing_atom/1` and no rescue clause to be safe.
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

  defmacro __using__(opts) do
    if opts != [] do
      raise ArgumentError,
            "use DMEx.Message takes no options, got: #{inspect(opts)}. " <>
              "A message module declares what the message is, and nothing about who receives it."
    end

    quote do
      @behaviour DMEx.Message

      # Evaluated at compile time, so `type/0` is a constant rather than an
      # `inspect/1` call on every publish.
      @dmex_message_type inspect(__MODULE__)

      @impl true
      def type, do: @dmex_message_type

      @impl true
      def cast(data) when is_map(data), do: DMEx.Message.cast_default(__MODULE__, data)

      defoverridable cast: 1
    end
  end

  @doc """
  The default `c:cast/1` implementation.

  Walks the struct's declared fields and takes each one's value from `data` when
  present, leaving the struct's own default in place otherwise. Keys in `data`
  that the struct does not declare are ignored.

  Iterating the *fields* rather than the *data* is what keeps this incapable of
  creating atoms — see the module documentation.
  """
  @spec cast_default(module(), map()) :: struct()
  def cast_default(module, data) when is_atom(module) and is_map(data) do
    base = module.__struct__()

    base
    |> Map.from_struct()
    |> Enum.reduce(base, fn {field, _default}, acc ->
      # `Atom.to_string/1` per field per row is the one cost here.
      #
      # Benchmark B3 (`bench/micro/message_cast.exs`) measured it: precomputing
      # the field-name strings at `use` time is 1.1x-2.1x faster, saving on the
      # order of 120-570 ns per row depending on struct width. Real, and not
      # acted on yet — the consumer runs a database transaction per row, which
      # `bench/e2e/catchup_drain.exs` shows costs orders of magnitude more, so
      # this is not the bottleneck it would need to be to justify moving `cast/1`
      # generation into `@before_compile`. Revisit if per-row commits are ever
      # coalesced, since that is what would make this visible.
      case Map.fetch(data, Atom.to_string(field)) do
        {:ok, value} -> Map.put(acc, field, value)
        :error -> acc
      end
    end)
  end

  @doc """
  Whether `module` was built with `use DMEx.Message`.

  The mirror of `DMEx.Consumer.Handler.handler?/1`, and used for the same
  reason: a handler matching on a struct that is not a message should be
  rejected by name rather than blow up as an `UndefinedFunctionError` on
  `type/0`.

  `Code.ensure_loaded?/1`, so this answers for a module that is already
  available and never blocks. `DMEx.Consumer.Handler` calls it at compile time
  on the structs a handler matches on, and does its own
  `Code.ensure_compiled!/1` first — the parallel compiler resolves a struct
  pattern without loading the module, so nothing else would have made `type/0`
  callable yet.
  """
  @spec message?(module()) :: boolean()
  def message?(module) when is_atom(module) do
    Code.ensure_loaded?(module) and function_exported?(module, :type, 0) and
      function_exported?(module, :cast, 1)
  end

  @doc """
  Builds a `message_type => module` lookup from a list of message modules.

  This is how a consumer resolves the `message_type` on a row back to a module.
  The mapping is always built from a consumer-declared list — never inferred by
  atomizing whatever string the producer sent.

  Raises if two modules claim the same type, which can only happen if `c:type/0`
  has been overridden, since the default derives from the module name.
  """
  @spec registry([module()]) :: %{String.t() => module()}
  def registry(modules) when is_list(modules) do
    Enum.reduce(modules, %{}, fn module, acc ->
      type = module.type()

      case acc do
        %{^type => already} when already != module ->
          raise ArgumentError,
                "message type #{inspect(type)} is claimed by both " <>
                  "#{inspect(already)} and #{inspect(module)}"

        _ ->
          Map.put(acc, type, module)
      end
    end)
  end
end
