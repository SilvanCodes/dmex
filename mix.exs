defmodule DMEx.MixProject do
  use Mix.Project

  @version "0.1.0-dev"
  @source_url "https://github.com/SilvanCodes/dmex"

  def project do
    [
      app: :dmex,
      version: @version,
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      dialyzer: [
        plt_add_apps: [:ex_unit, :mix],
        plt_file: {:no_warn, "priv/plts/dmex.plt"}
      ],
      name: "DMEx",
      description: "A decentralized, broker-less message queue over plain HTTP and SSE.",
      docs: docs()
    ]
  end

  # `:crypto` for `DMEx.Handler.Fingerprint`, which hashes message-type strings with
  # SHA-256. Both are OTP applications, so neither is a dependency.
  def application do
    [extra_applications: [:logger, :crypto]]
  end

  # Without this, `mix test.integration` runs in :dev — the alias expands to
  # `mix test`, and only the literal task named "test" gets :test inferred.
  def cli do
    [
      preferred_envs: [
        "test.integration": :test,
        "test.all": :test,
        "bench.micro": :test,
        "bench.e2e": :test,
        "bench.fingerprint": :test
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      # Telemetry is a hard dependency: it is dependency-free and tiny, and
      # making it optional would mean guarding every emit site for no benefit.
      {:telemetry, "~> 1.2"},

      # Everything else is optional. A read-only application has no reason
      # to carry Postgrex, an append-only one has no reason to carry Mint, and
      # neither has a reason to carry exqlite's NIF.
      {:ecto_sql, "~> 3.12", optional: true},
      {:postgrex, "~> 0.19", optional: true},
      {:ecto_sqlite3, "~> 0.24", optional: true},
      {:plug, "~> 1.16", optional: true},
      {:mint, "~> 1.6", optional: true},

      # Development and test only.
      {:bandit, "~> 1.5", only: [:dev, :test]},
      {:benchee, "~> 1.3", only: [:dev, :test]},
      {:stream_data, "~> 1.1", only: [:dev, :test]},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:styler, "~> 1.12", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      test: ["test --exclude integration"],
      "test.integration": ["test --only integration"],
      "test.all": ["test --include integration"],
      # In :test so test/support — repos, message modules, the storage stub —
      # is on the path.
      "bench.micro": ["run bench/micro.exs"],
      "bench.e2e": ["run bench/e2e.exs"],
      "bench.fingerprint": ["run bench/fingerprint_sweep.exs"]
    ]
  end

  defp docs do
    [
      # The README, since the `DMEx` moduledoc no longer repeats its quickstart.
      main: "readme",
      source_url: @source_url,
      # No tags yet, so a `v#{@version}` ref 404s on every "Source" link.
      source_ref: "trunk",
      extras: [
        "README.md",
        "guides/deployment.md",
        "guides/fingerprint-spec.md",
        "ARCHITECTURE.md",
        "ROADMAP.md"
      ],
      groups_for_modules: [
        Declaring: [DMEx.Log, DMEx.Message, DMEx.Handler, DMEx.Supervisor],
        Operating: [
          DMEx.Config,
          DMEx.Query,
          DMEx.Handler.Cursors,
          DMEx.Handler.DeadMessages,
          DMEx.Storage.Migration,
          DMEx.Telemetry
        ],
        Storage: [
          DMEx.Storage,
          DMEx.Storage.Postgres,
          DMEx.Storage.Postgres.Migration,
          DMEx.Storage.SQLite,
          DMEx.Storage.SQLite.Migration,
          DMEx.Storage.SQL
        ],
        Transports: [
          DMEx.Transport,
          DMEx.Transport.Local,
          DMEx.Transport.SSE,
          DMEx.Transport.SSE.Decoder,
          DMEx.Transport.SSE.Frame
        ],
        "Producer internals": [
          DMEx.Log.Plug,
          DMEx.Log.Dispatcher,
          DMEx.Log.Params,
          DMEx.Log.Selection,
          DMEx.Log.SendLoop,
          DMEx.Log.Status,
          DMEx.Log.Streams,
          DMEx.Log.Watcher,
          DMEx.Notifier,
          DMEx.Notifier.Beam,
          DMEx.Notifier.Postgres
        ],
        "Consumer internals": [
          DMEx.Handler.Fingerprint,
          DMEx.Handler.Request,
          DMEx.Handler.Subscription,
          DMEx.Handler.Subscriptions,
          DMEx.Supervisor.Bindings
        ]
      ]
    ]
  end
end
