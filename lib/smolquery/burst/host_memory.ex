defmodule Smolquery.Burst.HostMemory do
  @moduledoc """
  How much memory the host a runner runs on gives it (T-695), read from the
  host itself: `AWS_LAMBDA_FUNCTION_MEMORY_SIZE` (megabytes, set by Lambda on
  every invocation) when present, else the smaller of the cgroup memory
  limit (`Smolquery.CgroupMemory.limit_bytes/1`) and `MemTotal` in
  `/proc/meminfo`. `:error` when none of them is readable, so the caller
  keeps whatever limit it had.
  """

  alias Smolquery.CgroupMemory

  @typedoc """
  Where to read: `env` for the Lambda variable (default: the VM's
  environment) and `root` for the files (default: `/`).
  """
  @type opts :: [env: %{optional(String.t()) => String.t()}, root: Path.t()]

  @doc "The host's memory in bytes, or `:error` when nothing says."
  @spec bytes(opts()) :: {:ok, pos_integer()} | :error
  def bytes(opts \\ []) do
    env = Keyword.get_lazy(opts, :env, &System.get_env/0)
    root = Keyword.get(opts, :root, "/")

    case lambda(env) do
      {:ok, bytes} -> {:ok, bytes}
      :error -> smallest([cgroup(root), meminfo(root)])
    end
  end

  defp lambda(env) do
    case positive(Map.get(env, "AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "")) do
      {:ok, megabytes} -> {:ok, megabytes * 1024 * 1024}
      :error -> :error
    end
  end

  defp cgroup(root) do
    case CgroupMemory.limit_bytes(Path.join(root, "sys/fs/cgroup")) do
      {:ok, bytes} -> {:ok, bytes}
      :none -> :error
    end
  end

  defp meminfo(root) do
    with {:ok, text} <- File.read(Path.join(root, "proc/meminfo")),
         [_line, kilobytes] <- Regex.run(~r/^MemTotal:\s+(\d+)\s+kB/m, text),
         {:ok, kilobytes} <- positive(kilobytes) do
      {:ok, kilobytes * 1024}
    else
      _unreadable -> :error
    end
  end

  defp smallest(readings) do
    case for {:ok, bytes} <- readings, do: bytes do
      [] -> :error
      sizes -> {:ok, Enum.min(sizes)}
    end
  end

  defp positive(text) do
    case Integer.parse(String.trim(text)) do
      {value, ""} when value > 0 -> {:ok, value}
      _other -> :error
    end
  end
end
