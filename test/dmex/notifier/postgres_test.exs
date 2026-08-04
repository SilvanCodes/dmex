defmodule DMEx.Notifier.PostgresTest do
  use DMEx.Case, async: true, database: true

  alias DMEx.Notifier.Postgres

  setup do
    %{
      opts: %{
        repo: Repo,
        name: DMEx.Test.Log.Notifications,
        channel: "dmex_notifier_test"
      }
    }
  end

  # These are why nothing outside this module knows how Postgres delivers a
  # wakeup. Before them, DMEx.Log compared the storage module against this one
  # by name and carried a list of Postgrex connection options.
  describe "child_specs/1" do
    test "supplies a notification connection under the name the log derived", %{opts: opts} do
      assert [%{id: id, start: {Postgrex.Notifications, :start_link, [conn_opts]}}] =
               Postgres.child_specs(opts)

      assert id == opts.name
      assert conn_opts[:name] == opts.name
      assert conn_opts[:auto_reconnect] == true
      # From the repo's own config, not a literal: the suite's database name is
      # overridable via DMEX_POSTGRES_URL so concurrent runs can isolate.
      assert conn_opts[:database] == Repo.config()[:database]
    end

    test "passes Postgrex only keys it understands", %{opts: opts} do
      # repo.config/0 carries Ecto's own settings — a pool module, a telemetry
      # prefix — and handing those to Postgrex would be a connection that fails
      # to start rather than one that runs without notifications.
      [%{start: {_, _, [conn_opts]}}] = Postgres.child_specs(opts)

      for key <- [:pool, :pool_size, :telemetry_prefix, :otp_app, :log, :scheme, :url] do
        refute Keyword.has_key?(conn_opts, key), "#{inspect(key)} leaked into Postgrex opts"
      end
    end
  end

  describe "listen/1" do
    # The behaviour promises best-effort: polling is what makes dispatch robust,
    # so a missing notifier must be an error value rather than an exit that
    # takes the dispatcher down with it.
    test "reports a notifier that is not running instead of exiting", %{opts: opts} do
      assert {:error, :not_running} = Postgres.listen(opts)
    end
  end

  describe "notify/2" do
    # Delivery cannot be asserted here: Postgres delivers at COMMIT and the
    # sandbox never commits, so the round trip belongs in the integration suite
    # (`test/integration/`, where a real dispatcher wakes a real stream). What
    # this pins is the contract the append path depends on — the call is `:ok`
    # and it does not raise, because by the time it runs the row is written.
    test "returns :ok", %{opts: opts} do
      assert :ok = Postgres.notify(opts, "MyApp.Messages.OrderPlaced")
    end
  end
end
