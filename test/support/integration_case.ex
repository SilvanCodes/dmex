defmodule DMEx.IntegrationCase do
  @moduledoc """
  Case template for the integration suite.

  Runs against `DMEx.Test.IntegrationRepo` with **no sandbox**, so writes really
  commit — see `config/test.exs` for why. The cost is that cleanup is ours to
  do, and that these tests cannot run `async: true` against the same tables.

  Tagged `:integration`, so `mix test` skips them; run them with
  `mix test.integration` or `mix test.all`.
  """

  use ExUnit.CaseTemplate

  alias DMEx.Storage.Postgres
  alias DMEx.Test.Handlers
  alias DMEx.Test.IntegrationRepo
  alias DMEx.Test.Server
  alias Ecto.Adapters.SQL

  using do
    quote do
      import DMEx.Case, only: [eventually: 1, eventually: 2, eventually: 3]

      import DMEx.IntegrationCase,
        only: [
          cursor: 1,
          start_stack!: 0,
          start_stack!: 1,
          start_supervisor!: 1,
          start_supervisor!: 2,
          subscribe!: 1,
          subscribe!: 2
        ]

      alias DMEx.Test.IntegrationRepo, as: Repo

      @moduletag :integration
      @moduletag :database

      # Here rather than per file: every integration test drives real
      # reconnects, reclaims and dead messages, so DMEx's own warnings are the
      # behaviour under test and not output worth reading.
      @moduletag :capture_log
    end
  end

  setup do
    truncate_all!()
    on_exit(&truncate_all!/0)
    :ok
  end

  @doc """
  Starts what an integration setup starts: handler reports addressed to this
  process, the log, an HTTP server in front of it, and a `DMEx.Supervisor`
  bound to that server.

  The server binds port 0, so the log map is necessarily built at runtime —
  which is exactly why it lives on the child spec rather than in application
  config, and why this returns the URL rather than taking one.

  Options:

    * `:log` — the log to `start_supervised!`, or `nil` for a file that serves a
      hand-written plug or lets the supervisor start the log. Defaults to
      `DMEx.Test.IntegrationLog`.
    * `:plug` — the router the server mounts. Defaults to the one
      `DMEx.Test.Server` mounts `DMEx.Log.Plug` on.
    * `:otp_app` — `nil`, the default, keeps handler auto-discovery off so each
      test subscribes explicitly.
    * `:bindings` — the bindings, given directly. No server starts at all, for a
      slice that binds a name to a module rather than to a URL.
    * `:supervisor` — `false` leaves the `DMEx.Supervisor` unstarted, for a file
      that starts it per describe.

  Returns the context: `:base`, `:url`, `:host` and `:port` whenever a server ran.
  """
  @spec start_stack!(keyword()) :: map()
  def start_stack!(opts \\ []) do
    Handlers.report_to(self())

    if log = Keyword.get(opts, :log, DMEx.Test.IntegrationLog), do: start_supervised!(log)

    {bindings, context} =
      case Keyword.fetch(opts, :bindings) do
        {:ok, bindings} ->
          {bindings, %{}}

        :error ->
          base = Server.start!(Keyword.take(opts, [:plug]))
          url = base <> "/stream"
          uri = URI.parse(base)

          {Server.bindings(url), %{base: base, url: url, host: uri.host, port: uri.port}}
      end

    if Keyword.get(opts, :supervisor, true) do
      start_supervisor!(bindings, Keyword.take(opts, [:otp_app]))
    end

    context
  end

  @doc """
  Starts the `DMEx.Supervisor` the whole suite subscribes through, bound to `bindings`.

  Split out of `start_stack!/1` for the file that needs the server up before the
  supervisor is, so only some of its describes have a consumer connected.
  """
  @spec start_supervisor!(keyword(), keyword()) :: pid()
  def start_supervisor!(bindings, opts \\ []) do
    start_supervised!(
      {DMEx.Supervisor,
       name: TestSupervisor, otp_app: Keyword.get(opts, :otp_app), repo: IntegrationRepo, bindings: bindings}
    )
  end

  @doc """
  Subscribes `handler` through the suite's supervisor, from the start of the log.

  `last_seen: 0` rather than the default: an integration test either appends
  before it subscribes, or subscribes and then appends. The default — unset —
  resolves to the log's tip at *connect*, which skips the backlog in the first
  case and races the appends in the second. Nothing here predates its own
  subscription, so 0 names exactly the rows the test wrote.

  Caller options merge last, so a call site can still override `last_seen:`.
  """
  @spec subscribe!(module(), keyword()) :: {:ok, [pid()]} | {:error, term()}
  def subscribe!(handler, opts \\ []) do
    defaults = [supervisor: TestSupervisor, last_seen: 0]

    DMEx.Supervisor.subscribe(handler, Keyword.merge(defaults, opts))
  end

  @doc "The durable cursor for `subscription`, read straight from the database."
  @spec cursor(String.t()) :: {:ok, non_neg_integer() | nil} | {:error, term()}
  def cursor(subscription) do
    Postgres.last_seen(subscription, %{repo: IntegrationRepo, prefix: nil})
  end

  @doc """
  Empties every DMEx table.

  `RESTART IDENTITY` matters here: ids are cursors, and a test that assumed a
  fresh log would otherwise see ids continuing from the previous test's tail.
  """
  @spec truncate_all!() :: :ok
  def truncate_all! do
    tables = ~w(dmex_messages dmex_cursors dmex_dead_messages)

    existing =
      Enum.filter(tables, fn table ->
        %{rows: [[exists?]]} =
          SQL.query!(IntegrationRepo, "SELECT to_regclass($1) IS NOT NULL", [table])

        exists?
      end)

    if existing != [] do
      SQL.query!(
        IntegrationRepo,
        "TRUNCATE #{Enum.join(existing, ", ")} RESTART IDENTITY CASCADE",
        []
      )
    end

    :ok
  end
end
