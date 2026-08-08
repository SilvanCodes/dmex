defmodule DMEx.Notifier.BeamTest do
  use DMEx.Case, async: true

  alias DMEx.Notifier.Beam

  setup context do
    # A scope per test, since `:pg.start_link/1` registers the scope name and
    # these run concurrently. `:name` is what the log derives from its own
    # module, so a unique one is the whole isolation mechanism.
    name = Module.concat(__MODULE__, context.test |> to_string() |> String.replace(" ", ""))

    %{opts: %{name: name, channel: "dmex_beam_test"}}
  end

  describe "child_specs/1" do
    test "supplies a :pg scope under the name the log derived", %{opts: opts} do
      assert [%{id: id, start: {:pg, :start_link, [scope]}}] = Beam.child_specs(opts)

      assert id == opts.name
      assert scope == id
    end
  end

  describe "listen/1" do
    # Best-effort by contract: polling is what makes dispatch robust, so a scope
    # that has not started must be an error value rather than an exit.
    test "reports a scope that is not running instead of exiting", %{opts: opts} do
      assert {:error, :not_running} = Beam.listen(opts)
    end

    test "returns the scope to monitor", %{opts: opts} do
      scope = start_scope!(opts)

      assert {:ok, ^scope} = Beam.listen(opts)
    end
  end

  describe "notify/2" do
    test "delivers the message type in Postgrex's shape", %{opts: opts} do
      start_scope!(opts)
      {:ok, _pid} = Beam.listen(opts)

      assert :ok = Beam.notify("MyApp.Messages.OrderPlaced", opts)

      channel = opts.channel

      assert_receive {:notification, _pid, ref, ^channel, "MyApp.Messages.OrderPlaced"}
      assert is_reference(ref)
    end

    test "reaches every listener, and only this channel's", %{opts: opts} do
      start_scope!(opts)

      test = self()

      listener = fn listen_opts ->
        fn ->
          {:ok, _scope} = Beam.listen(listen_opts)
          send(test, {:ready, self()})

          receive do
            {:notification, _pid, _ref, _channel, type} -> send(test, {:got, self(), type})
          after
            1_000 -> :timeout
          end
        end
      end

      for i <- 1..2, do: start_supervised!({Task, listener.(opts)}, id: {:mine, i})
      start_supervised!({Task, listener.(%{opts | channel: "dmex_other"})}, id: :other)

      for _ <- 1..3, do: assert_receive({:ready, _pid})

      assert :ok = Beam.notify("A", opts)

      assert_receive {:got, first, "A"}
      assert_receive {:got, second, "A"}
      refute first == second

      # The third joined a different channel in the same scope, so it gets
      # nothing — one scope per log, one group per channel.
      refute_receive {:got, _pid, _type}, 50
    end

    # It runs after the row is written, so it may not raise — and with nothing
    # started there is no scope to look a group up in.
    test "is a no-op when the scope is not running", %{opts: opts} do
      assert :ok = Beam.notify("A", opts)
    end
  end

  defp start_scope!(opts) do
    [spec] = Beam.child_specs(opts)

    start_supervised!(spec)
  end
end
