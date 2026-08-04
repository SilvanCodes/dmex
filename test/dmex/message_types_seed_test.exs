defmodule DMEx.MessageTypesSeedTest do
  @moduledoc """
  The boot-time seed of `DMEx.message_types/1`, when the supervisor's `otp_app:`
  and the application owning a log module are not the same one.

  Which is the case `otp_app:` exists for: an umbrella parent starting DMEx for
  a log that lives in a child application, or a log shipped in a dependency. The
  seed is keyed on what `init/1` was given; the read in `DMEx.Log.Plug` is keyed
  on the application owning the log module.
  """

  # Not async: the owned log starts real processes against the sandbox.
  use DMEx.Case, async: false, database: true

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
    use DMEx.Log, repo: DMEx.Test.Repo, app: :dmex_fake_dep
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
    use DMEx.Log, repo: DMEx.Test.Repo, app: :dmex_fake_collide
  end

  setup_all do
    load!(:dmex_fake_host, [])
    load!(:dmex_fake_dep, [Dep.Thing, Dep.Log])
    load!(:dmex_fake_collide, [Collide.First, Collide.Second, Collide.Log])

    on_exit(fn ->
      for app <- [:dmex_fake_host, :dmex_fake_dep, :dmex_fake_collide],
          do: :application.unload(app)
    end)

    :ok
  end

  setup do
    for app <- [:dmex_fake_host, :dmex_fake_dep, :dmex_fake_collide],
        do: :persistent_term.erase({DMEx, :message_types, app})

    :ok
  end

  test "seeds the cache under the key the read path uses" do
    assert :application.get_application(Dep.Log) == {:ok, :dmex_fake_dep}

    start_supervisor(otp_app: :dmex_fake_host, logs: [fake: Dep.Log])

    # What `DMEx.Log.Plug` looks up at the first selective connect. A miss here
    # is a full module scan inside the request instead of at boot.
    assert :persistent_term.get({DMEx, :message_types, :dmex_fake_dep}, :miss) != :miss
    assert MapSet.member?(DMEx.message_types(Dep.Log), "fake_dep_thing")
  end

  test "runs the collision check at boot, not on the first request" do
    assert start_error(otp_app: :dmex_fake_host, logs: [fake: Collide.Log]) =~
             ~s(message type "fake_collision" is declared by both)
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
      [name: TestSupervisor, otp_app: nil, repo: DMEx.Test.Repo, logs: [test: @url]],
      opts
    )
  end
end
