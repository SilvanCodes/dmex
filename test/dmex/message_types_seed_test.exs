defmodule DMEx.MessageTypesSeedTest do
  @moduledoc """
  The boot-time seed of `DMEx.Message.types/0`, when the supervisor's `otp_app:`
  and the application owning a log module are not the same one.

  Which is the case `otp_app:` exists for: an umbrella parent starting DMEx for
  a log that lives in a child application. The index is one node-wide entry, but
  a scan reaches only the applications depending on `:dmex` plus the one it was
  given, so an application outside that rule contributes nothing unless `init/1`
  names it — hence one refresh per application owning a log, merged.
  """

  # Not async: the owned log starts real processes against the sandbox.
  use DMEx.Case, async: false, database: true

  alias DMEx.Test.Repo

  @moduletag :capture_log

  @url "http://127.0.0.1:1/stream"

  # The applications below are synthesised at runtime rather than compiled:
  # `:application.get_application/1` answers from an application's `modules`
  # list, so loading a spec is all it takes to own a module. None of them
  # declares `:dmex` in `applications:`, so `DMEx.discovery/1` reaches them only
  # when they are named — no other suite's scan can see them.
  defmodule Dep.Thing do
    @moduledoc false
    use DMEx.Message, type: "fake_dep_thing", fields: [:id]
  end

  defmodule Dep.Log do
    @moduledoc false
    use DMEx.Log, repo: Repo, app: :dmex_fake_dep
  end

  defmodule Collide.First do
    @moduledoc false
    use DMEx.Message, type: "fake_collision", fields: [:id]
  end

  defmodule Collide.Second do
    @moduledoc false
    use DMEx.Message, type: "fake_collision", fields: [:id]
  end

  defmodule Collide.Log do
    @moduledoc false
    use DMEx.Log, repo: Repo, app: :dmex_fake_collide
  end

  # The same type as `Dep.Thing`, in an application whose scan cannot see it.
  defmodule Across.Thing do
    @moduledoc false
    use DMEx.Message, type: "fake_dep_thing", fields: [:id]
  end

  @apps [:dmex_fake_host, :dmex_fake_dep, :dmex_fake_collide, :dmex_fake_across]

  setup_all do
    load!(:dmex_fake_host, [])
    load!(:dmex_fake_dep, [Dep.Thing, Dep.Log])
    load!(:dmex_fake_collide, [Collide.First, Collide.Second, Collide.Log])
    load!(:dmex_fake_across, [Across.Thing])

    on_exit(fn ->
      :persistent_term.erase({DMEx.Message, :index})
      for app <- @apps, do: :application.unload(app)
    end)

    :ok
  end

  setup do
    # One node-wide entry, and these fixtures merge into it: cleared per test so
    # a leftover type cannot make the next one's merge collide, and put back the
    # way `test_helper.exs` leaves it so the suites after this one still see the
    # suite's own message modules.
    reset = fn ->
      :persistent_term.erase({DMEx.Message, :index})
      DMEx.Message.refresh_index(:dmex)
    end

    reset.()
    on_exit(reset)

    :ok
  end

  test "seeds the types of an application the supervisor's own scan misses" do
    assert :application.get_application(Dep.Log) == {:ok, :dmex_fake_dep}

    start_supervisor(otp_app: :dmex_fake_host, logs: [fake: Dep.Log])

    # In the term, not merely reachable: `DMEx.Log.Plug` reads the cache at the
    # first selective connect, and a lazy scan there would not reach
    # `:dmex_fake_dep` at all, since it declares no `:dmex` dependency.
    assert {DMEx.Message, :index} |> :persistent_term.get() |> Map.has_key?("fake_dep_thing")
    assert MapSet.member?(DMEx.Message.types(), "fake_dep_thing")
  end

  test "runs the collision check at boot, not on the first request" do
    assert start_error(otp_app: :dmex_fake_host, logs: [fake: Collide.Log]) =~
             ~s(message type "fake_collision" is declared by both)
  end

  test "collides across two scans that never see each other" do
    # The reason the index is node-wide: neither application declares `:dmex`,
    # so no single scan holds both modules and `DMEx.Message.index!/1` alone
    # would let both through. The merge is what catches it.
    _ = DMEx.Message.refresh_index(:dmex_fake_dep)

    assert_raise ArgumentError, ~r/"fake_dep_thing" is declared by both/, fn ->
      DMEx.Message.refresh_index(:dmex_fake_across)
    end
  end

  test "a rescan of one application does not collide with itself" do
    index = DMEx.Message.refresh_index(:dmex_fake_dep)

    assert DMEx.Message.refresh_index(:dmex_fake_dep) == index
  end

  ## Helpers

  defp load!(app, modules) do
    spec =
      {:application, app,
       [
         description: ~c"dmex test fixture",
         vsn: ~c"0.0.0",
         modules: modules,
         registered: [],
         applications: [:kernel, :stdlib]
       ]}

    case :application.load(spec) do
      :ok -> :ok
      {:error, {:already_loaded, ^app}} -> :ok
    end
  end

  defp start_supervisor(opts) do
    start_supervised!({DMEx.Supervisor, options(opts)})
  end

  # A raise in `init/1` reaches the caller as an exit, not an exception.
  defp start_error(opts) do
    Process.flag(:trap_exit, true)

    {:error, {%ArgumentError{message: message}, _stacktrace}} =
      DMEx.Supervisor.start_link(options(opts))

    message
  after
    Process.flag(:trap_exit, false)
  end

  defp options(opts) do
    Keyword.merge(
      [name: TestSupervisor, otp_app: nil, repo: Repo, logs: [test: @url]],
      opts
    )
  end
end
