defmodule DMEx.Backoff do
  @moduledoc """
  Jittered exponential backoff, shared by reconnects and local retries.

  **Jitter is the point, not a refinement.** An event that fails many consumers
  at once — a producer restart, a network blip, a deploy — would otherwise bring
  them all back simultaneously, and DMEx has no admission control to absorb
  that. Full jitter (a uniform draw from `[1, capped]` rather than a band around
  the ceiling) decorrelates hardest. The draw starts at 1 so a delay is always a
  delay.

  The same argument applies to a producer's advertised `retry:` value, which is
  why DMEx's own client keeps its own schedule rather than reconnecting at
  exactly the advertised interval.
  """

  @doc """
  The delay in ms before attempt `attempt`, counting from 1.

  A uniform draw from `[1, ceiling]`, where `ceiling` is `base * 2^(attempt - 1)`
  capped at `max`.
  """
  @spec delay(pos_integer(), pos_integer(), pos_integer()) :: non_neg_integer()
  def delay(attempt, base, max)
      when is_integer(attempt) and attempt >= 1 and is_integer(base) and is_integer(max) do
    ceiling = min(max, base * Integer.pow(2, attempt - 1))

    :rand.uniform(ceiling)
  end
end
