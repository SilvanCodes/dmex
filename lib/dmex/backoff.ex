defmodule DMEx.Backoff do
  @moduledoc """
  Jittered exponential backoff, shared by reconnects and local retries.

  **Jitter is not a refinement here, it is the point.** An event that fails many
  consumers at once — a producer restart, a network blip, a deploy — would
  otherwise bring them all back simultaneously, and DMEx has deliberately no
  admission control to absorb that. The same argument applies to a producer's
  advertised `retry:` value: one number applied literally by every consumer is a
  synchronized reconnect storm, which is why DMEx's own client keeps its own
  jittered schedule rather than reconnecting at exactly the advertised interval.

  Full jitter — a uniform draw from `[0, capped)` rather than a band around it —
  is used because it decorrelates hardest, which is what a thundering herd needs.
  """

  @doc """
  The delay in ms before attempt `attempt`, counting from 1.

  Doubles per attempt up to `max`, then draws uniformly below that.
  """
  @spec delay(pos_integer(), pos_integer(), pos_integer()) :: non_neg_integer()
  def delay(attempt, base, max)
      when is_integer(attempt) and attempt >= 1 and is_integer(base) and is_integer(max) do
    ceiling = min(max, base * Integer.pow(2, attempt - 1))

    :rand.uniform(ceiling)
  end
end
