defmodule DMEx.Log.Selection do
  @moduledoc """
  Which message types a connection asked for.

  `:all` is a whole-log subscription; a `MapSet` is a selective one, already
  resolved to concrete type names. Every question asked of that value is answered
  here — by the `DMEx.Log.Streams` entry it rides in, `DMEx.Log.Dispatcher`'s
  routing, the read's type predicate in `DMEx.Log.SendLoop` and the `matched:`
  measurement both transports emit — so the two transports cannot answer one
  differently.

  The two constructors do not build the same set: `expand/1`'s membership is a
  `DMEx.Handler.Fingerprint` test, so it admits a type the consumer never declared
  at 2⁻¹⁶ per type tested, while `declared/2` works from modules in this VM and is
  exact. A local subscription therefore reads a *subset* of what the same handler
  reads over SSE, which nothing observes: `DMEx.Handler.Pipeline` drops a row no
  clause matches, so a false positive costs one row read.
  """

  alias DMEx.Handler.Fingerprint

  @typedoc "`:all` for a whole-log subscription, or the selective one's expanded set."
  @type t :: :all | MapSet.t(String.t())

  @doc """
  Expands a parsed `DMEx.Log.Params` selection against the node's compiled message
  modules, not against what the log happens to contain — see
  `DMEx.Message.types/0` for why those are not interchangeable. An empty `MapSet`
  matched nothing, which `DMEx.Log.Plug` answers with a `404`.
  """
  @spec expand(:all | Fingerprint.t()) :: t()
  def expand(:all), do: :all

  def expand(%Fingerprint{} = filter) do
    MapSet.filter(DMEx.Message.types(), &Fingerprint.member?(filter, &1))
  end

  @doc """
  The selection a handler's own declared message modules make, exactly — see
  `expand/1` for why the wire's is not.

  Every type each module answers for, retired ones included, since that is what
  the fingerprint encodes: selecting less here than over the wire would read
  different rows depending on how the log happens to be bound. A `nil`
  fingerprint is a handler with a clause that named no type, and means the whole
  log here as it does on the wire.
  """
  @spec declared(MapSet.t(module()), String.t() | nil) :: t()
  def declared(_messages, nil), do: :all

  def declared(messages, _fingerprint), do: MapSet.new(Enum.flat_map(messages, &DMEx.Message.declared/1))

  @doc "Whether a selection is concerned with any of the types just appended."
  @spec concerns?(t(), [String.t()]) :: boolean()
  def concerns?(:all, _types), do: true
  def concerns?(selection, types), do: Enum.any?(types, &MapSet.member?(selection, &1))

  @doc """
  How many message types a selection admits, or `nil` for the whole log.

  For the `matched:` measurement on `[:dmex, :stream, :connect]` and the
  `dmex-matched-types` header, which must agree.
  """
  @spec size(t()) :: non_neg_integer() | nil
  def size(:all), do: nil
  def size(selection), do: MapSet.size(selection)

  @doc """
  The selection as the read's type predicate wants it: a list rather than a
  `MapSet`, because there it is a query parameter and not a membership test.
  """
  @spec to_list(t()) :: :all | [String.t()]
  def to_list(:all), do: :all
  def to_list(selection), do: MapSet.to_list(selection)

  @doc "Whether this is a selective subscription, for telemetry and headers."
  @spec selective?(t()) :: boolean()
  def selective?(selection), do: selection != :all
end
