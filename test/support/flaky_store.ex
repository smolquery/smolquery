defmodule Smolquery.Test.FlakyStore do
  @moduledoc """
  A `Smolquery.Segments.Store` over another store whose `put/3` refuses
  chosen keys a chosen number of times, for tests of what a spill the
  store refuses does to a job (T-703). `failures` maps a key suffix to
  how many puts of a key ending in it fail, `:infinity` for every one; a
  refused put is `{:error, {:put_failed, key, :flaky}}` and writes
  nothing. Everything else goes to the store underneath.
  """

  @behaviour Smolquery.Segments.Store

  alias Smolquery.Segments.Store

  @type failures :: %{String.t() => pos_integer() | :infinity}

  @doc """
  A store over `inner` that refuses the puts `failures` names, linked to
  the calling process.
  """
  @spec new(Store.t(), failures()) :: Store.t()
  def new(%Store{} = inner, failures) do
    {:ok, agent} = Agent.start_link(fn -> failures end)

    %Store{impl: __MODULE__, config: %{inner: inner, agent: agent}}
  end

  @impl Store
  def put(%{inner: inner, agent: agent}, key, encoder) do
    if Agent.get_and_update(agent, &refuse(&1, key)),
      do: {:error, {:put_failed, key, :flaky}},
      else: Store.put(inner, key, encoder)
  end

  @impl Store
  def location(%{inner: inner}, key), do: Store.location(inner, key)

  @impl Store
  def list(%{inner: inner}, prefix), do: Store.list(inner, prefix)

  @impl Store
  def delete(%{inner: inner}, key), do: Store.delete(inner, key)

  @impl Store
  def shared?(%{inner: inner}), do: Store.shared?(inner)

  @impl Store
  def sweep_staging(%{inner: inner}, age_ms), do: Store.sweep_staging(inner, age_ms)

  defp refuse(failures, key) do
    case Enum.find(failures, fn {suffix, _left} -> String.ends_with?(key, suffix) end) do
      nil -> {false, failures}
      {_suffix, :infinity} -> {true, failures}
      {suffix, 1} -> {true, Map.delete(failures, suffix)}
      {suffix, left} -> {true, Map.put(failures, suffix, left - 1)}
    end
  end
end
