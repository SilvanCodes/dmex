defmodule DMEx.Discovery do
  @moduledoc """
  Which modules could hold a DMEx declaration.

  Every loaded application declaring a dependency on `:dmex`, plus `otp_app`
  when given. A module cannot `use DMEx.Message` or `use DMEx.Handler` unless
  its application depends on `:dmex`, so the condition is exact rather than a
  heuristic — and it keeps the scan off the thousands of OTP and Elixir modules
  that could never qualify. It also makes a **shared package** work: a library
  of message modules lists `:dmex` and is scanned like any other.

  `otp_app` covers the case the rule misses — an application that does not
  itself depend on `:dmex`, such as an umbrella parent or DMEx's own test suite.
  """

  @doc "Every module of every scanned application."
  @spec modules(atom() | nil) :: [module()]
  def modules(otp_app \\ nil) do
    otp_app
    |> applications()
    |> Enum.flat_map(&app_modules/1)
  end

  defp applications(otp_app) do
    dependents =
      for {app, _description, _vsn} <- Application.loaded_applications(),
          depends_on_dmex?(app),
          do: app

    Enum.uniq(List.wrap(otp_app) ++ dependents)
  end

  defp depends_on_dmex?(app) do
    case :application.get_key(app, :applications) do
      {:ok, applications} -> :dmex in applications
      _other -> false
    end
  end

  defp app_modules(app) do
    case :application.get_key(app, :modules) do
      {:ok, modules} -> modules
      _other -> []
    end
  end
end
