defmodule DMEx do
  @moduledoc """
  A decentralized, broker-less message queue over plain HTTP and Server-Sent Events.

  Every application that appends messages owns and persists its own append-only
  message log — one ordinary SQL table — and streams it directly to its readers
  over HTTP. See [ARCHITECTURE](ARCHITECTURE.md) for the design.

  Three declarations: `DMEx.Log` for the log an application owns,
  `DMEx.Message` for one message type, `DMEx.Handler` for what a consumer reads
  and does with each row. `DMEx.Supervisor` is the one child spec, and holds the
  option vocabulary: what each log binding means, why it is also what chooses
  the transport, and why handler modules are not listed.
  [README](README.md) has the migration, the three declarations and the child
  spec end to end.

  DMEx owns the read position (`last_seen`), keyed by the declared `"log/id"`
  pair rather than the module name, which is what makes renaming a handler free.

  ## Three things worth knowing before you build on it

    * **A subscription receives the types its handler matches on, and there is
      no filter option.** The selection is derived from the `handle_message/2`
      clauses and resolved by the log against its own message modules.
      Inclusion is heuristic in one direction only: never a false negative, and
      an occasional extra type arrives and is skipped. See
      `DMEx.Handler.Fingerprint`.

    * **Delivery is at-least-once.** The handler call and the cursor advance
      share one transaction, so side effects through the reader's own repo are
      exactly-once for free. Effects that leave that transaction must be
      idempotent.

    * **Appending is serialized per log table** — what keeps every reader's
      `id > cursor` scan sound, at the price of append concurrency.
      `DMEx.Storage` covers how each adapter buys it and what it means for an
      `append/2` inside your own transaction.

  `DMEx.Telemetry` documents every event DMEx emits.
  """

  @doc """
  Every module of every application that could hold a DMEx declaration.

  That is every loaded application declaring a dependency on `:dmex`, plus
  `otp_app` when given. A module cannot `use DMEx.Message` or `use DMEx.Handler`
  unless its application depends on `:dmex`, so the condition is exact rather
  than a heuristic — and it keeps the scan off the thousands of OTP and Elixir
  modules that could never qualify. It also makes a **shared package** work: a
  library of message modules lists `:dmex` and is scanned like any other.

  `otp_app` covers the case the rule misses — an application that does not
  itself depend on `:dmex`, such as an umbrella parent or DMEx's own test suite.
  """
  @spec discovery(atom() | nil) :: [module()]
  def discovery(otp_app \\ nil) do
    for {app, _description, _vsn} <- Application.loaded_applications(),
        app == otp_app or :dmex in Application.spec(app, :applications),
        module <- Application.spec(app, :modules),
        do: module
  end

  @doc """
  The jittered exponential backoff delay in ms before attempt `attempt`,
  counting from 1. Shared by reconnects and local retries.

  A uniform draw from `[1, min(max, base * 2^(attempt - 1))]`.

  **Jitter is the point, not a refinement.** An event that fails many consumers
  at once — a producer restart, a network blip, a deploy — would otherwise bring
  them all back simultaneously, and DMEx has no admission control to absorb
  that. Full jitter decorrelates hardest.

  The same argument applies to a producer's advertised `retry:` value, which is
  why DMEx's own client keeps its own schedule rather than reconnecting at
  exactly the advertised interval.

  **Callers bound `attempt`** — `max_attempts` for a local retry, and
  `DMEx.Handler.Subscription`'s own cap for a reconnect, where the counter would
  otherwise climb for as long as an origin stays down. The power is evaluated
  before the cap applies, so an unbounded counter would mean a `2^n` bignum per
  reconnect, growing forever, to answer `max` every time.
  """
  @spec backoff(pos_integer(), pos_integer(), pos_integer()) :: pos_integer()
  def backoff(attempt, base, max) when is_integer(attempt) and attempt >= 1,
    do: :rand.uniform(min(max, base * 2 ** (attempt - 1)))
end
