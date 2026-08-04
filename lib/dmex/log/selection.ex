defmodule DMEx.Log.Selection do
  @moduledoc """
  Which message types a connection asked for.

  `:all` is a whole-log subscription; a `MapSet` is a selective one, already
  resolved to concrete type names. One value reaches four places — the
  `DMEx.Log.Registry` entry it rides in, `DMEx.Log.Dispatcher`'s routing
  decision, the read's type predicate in `DMEx.Log.Stream`, and the `matched:`
  measurement both transports emit — so every question asked of it is answered
  here, and the two transports cannot answer one differently.

  ## Two constructors, and they do not build the same set

  `expand/2` is the wire's: a `DMEx.Handler.Fingerprint` blob tested against the
  message types of the application owning the log. Membership there is
  probabilistic, so the set admits a type the consumer never declared at a rate
  of 2⁻¹⁶ per type tested.

  `declared/1` is `DMEx.Transport.Local`'s: the handler's declared modules are
  in this VM, so no blob is built and the set is exact.

  A local subscription therefore reads a *subset* of what the same handler reads
  over SSE. Deliberate, and not observable at either end: `DMEx.Handler.Pipeline`
  decodes by clause and drops a row no clause matches, so a false positive costs
  one row read. Making the local path encode a fingerprint and expand it back
  would buy nothing but those false positives.
  """

  alias DMEx.Handler.Fingerprint

  @typedoc """
  Which message types a connection asked for.

  `:all` is a whole-log subscription; a `MapSet` is a selective one, already
  expanded from its filter against the log's own message types.
  """
  @type t :: :all | MapSet.t(String.t())

  @doc """
  Expands a parsed `DMEx.Log.Params` selection against a log.

  Against the compiled message modules of the application owning the log, rather
  than against what the log happens to contain — see `DMEx.message_types/1` for
  why those are not interchangeable, and why the set is not per log.

  An empty `MapSet` is a filter that matched nothing, which `DMEx.Log.Plug`
  answers with a `404`.
  """
  @spec expand(module(), :all | Fingerprint.t()) :: t()
  def expand(_log, :all), do: :all

  def expand(log, %Fingerprint{} = filter) do
    log
    |> DMEx.message_types()
    |> Enum.filter(&Fingerprint.member?(filter, &1))
    |> MapSet.new()
  end

  @doc """
  The selection a handler's own declared message modules make. See `expand/2`
  for why this set is exact and that one is not.

  Which of the two a subscription gets is decided by the same value on both
  transports: a `nil` fingerprint *is* `selective: false` — a
  `t:DMEx.Handler.declaration/0` carries no separate field — and it means the
  same thing here as it does on the wire, the whole log.
  """
  @spec declared(MapSet.t(module()), String.t() | nil) :: t()
  def declared(_messages, nil), do: :all
  def declared(messages, _fingerprint), do: MapSet.new(messages, & &1.type())

  @doc "Whether a selection is concerned with any of the types just appended."
  @spec concerns?(t(), [String.t()]) :: boolean()
  def concerns?(:all, _types), do: true
  def concerns?(selection, types), do: Enum.any?(types, &MapSet.member?(selection, &1))

  @doc """
  How many message types a selection admits, or `nil` for the whole log.

  For the `matched:` measurement on `[:dmex, :stream, :connect]` and the
  `dmex-matched-messages` header, which must agree.
  """
  @spec size(t()) :: non_neg_integer() | nil
  def size(:all), do: nil
  def size(selection), do: MapSet.size(selection)

  @doc """
  The selection as the read's type predicate wants it.

  A list rather than a `MapSet`, because there this is a query parameter, not a
  membership test.
  """
  @spec to_list(t()) :: :all | [String.t()]
  def to_list(:all), do: :all
  def to_list(selection), do: MapSet.to_list(selection)

  @doc "Whether this is a selective subscription, for telemetry and headers."
  @spec selective?(t()) :: boolean()
  def selective?(selection), do: selection != :all
end
