defmodule DMEx.Migration do
  @moduledoc """
  Creates the tables DMEx needs, from a migration in the host application.

  DMEx lives in your database, alongside your own tables, so its schema is
  yours to migrate:

      defmodule MyApp.Repo.Migrations.AddDMEx do
        use Ecto.Migration

        def up, do: DMEx.Migration.up()
        def down, do: DMEx.Migration.down()
      end

  ## Which tables

  An application is usually a producer, a consumer, or both, and it only needs
  the tables for the roles it plays:

      DMEx.Migration.up(tables: [:producer])   # dmex_messages
      DMEx.Migration.up(tables: [:consumer])   # dmex_cursors, dmex_dead_messages
      DMEx.Migration.up()                      # both, the default

  ## Options

    * `:tables` — `[:producer, :consumer]` (default both)
    * `:prefix` — a Postgres schema to create the tables in. Also creates the
      schema itself if it does not exist.

  ## The two indexes, and why each exists

  **The primary key on `id`** serves every whole-log read directly: those are
  `id > cursor` range scans and nothing else.

  **`(message_type, id)`** serves a selective subscription's
  `WHERE id > $1 AND message_type = ANY($2) ORDER BY id`, and the planner takes
  it **when the selection is rare**. Measured on a 500k-row skewed log: a type
  making up 0.1% of rows uses the index; at 1% and above Postgres prefers
  walking the primary key in id order and filtering, which is genuinely the
  cheaper plan — that scan stops as soon as it has a batch, so the more of a log
  a subscription selects the less it examines per row returned.

  That is the behaviour you want rather than a shortfall. The index earns its
  keep exactly where a scan would walk thousands of rows to return one, and
  stays out of the way where it would be the slower plan.

  It is not free. Appends are serialized per app, so index maintenance sits on
  the one write path that cannot be parallelized away. The cost is one index
  entry per publish, paid by every producer whether or not anyone selects
  against it; it is accepted because the alternative is that server-side
  selection saves bandwidth and no database work at all.
  """

  alias DMEx.Migration.Postgres

  @tables [:producer, :consumer]

  @doc "Creates the DMEx tables. See the module documentation for options."
  @spec up(keyword()) :: :ok
  def up(opts \\ []) when is_list(opts) do
    opts |> validate!() |> Postgres.up()
  end

  @doc """
  Drops the DMEx tables.

  Note what this destroys: `dmex_messages` is an append-only log that nothing
  else keeps a copy of, and dropping it is not recoverable from any consumer.
  """
  @spec down(keyword()) :: :ok
  def down(opts \\ []) when is_list(opts) do
    opts |> validate!() |> Postgres.down()
  end

  defp validate!(opts) do
    tables = Keyword.get(opts, :tables, @tables)

    unless is_list(tables) and tables != [] and Enum.all?(tables, &(&1 in @tables)) do
      raise ArgumentError,
            ":tables must be a non-empty list drawn from #{inspect(@tables)}, " <>
              "got: #{inspect(tables)}"
    end

    %{tables: tables, prefix: Keyword.get(opts, :prefix)}
  end
end
