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

  ## What is deliberately absent

  **There is no index on `message_type`.** Every read DMEx issues is an
  `id > cursor` range scan, which the primary key serves directly, so a
  `message_type` index would exist purely for operators querying the log by
  hand — and it is not free: appends are serialized per app, so index
  maintenance sits on the one write path that cannot be parallelized away. Add
  one yourself if you query the log that way:

      create index(:dmex_messages, [:message_type])

  That calculus changes if server-side message-type selection ever lands, at
  which point `(message_type, id)` becomes required rather than optional.
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
