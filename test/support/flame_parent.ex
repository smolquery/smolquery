defmodule Smolquery.Test.FlameParent do
  @moduledoc """
  Makes the test VM look like a burst runner: `FLAME_PARENT` set the way a
  `FLAME.Backend` sets it on a child it boots, pointing at the test process.

  `put_env/0` sets the variable and removes it when the test exits, so a
  test module that uses it must be `async: false`.
  """

  @doc "Sets `FLAME_PARENT` for the rest of the test and clears it on exit."
  @spec put_env() :: :ok
  def put_env do
    System.put_env("FLAME_PARENT", encoded())
    ExUnit.Callbacks.on_exit(fn -> System.delete_env("FLAME_PARENT") end)
  end

  @doc """
  Makes the test VM a runner configured with `burst_config` under
  `config :smolquery, Smolquery.Burst`, and restores the previous
  configuration when the test exits.
  """
  @spec as_runner(keyword()) :: :ok
  def as_runner(burst_config) do
    previous = Application.get_env(:smolquery, Smolquery.Burst)

    ExUnit.Callbacks.on_exit(fn ->
      if previous,
        do: Application.put_env(:smolquery, Smolquery.Burst, previous),
        else: Application.delete_env(:smolquery, Smolquery.Burst)
    end)

    put_env()
    Application.put_env(:smolquery, Smolquery.Burst, burst_config)
  end

  @doc "The encoded parent a backend would hand a runner."
  @spec encoded() :: String.t()
  def encoded do
    make_ref()
    |> FLAME.Parent.new(self(), FLAME.LocalBackend, "runner", "SMOLQUERY_BURST_HOST")
    |> FLAME.Parent.encode()
  end
end
