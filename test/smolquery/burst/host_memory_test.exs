defmodule Smolquery.Burst.HostMemoryTest do
  use ExUnit.Case, async: true

  alias Smolquery.Burst.HostMemory

  @moduletag :tmp_dir
  @gib 1024 * 1024 * 1024

  defp write(root, path, text) do
    full = Path.join(root, path)
    File.mkdir_p!(Path.dirname(full))
    File.write!(full, text)
  end

  test "the Lambda variable wins, in megabytes", %{tmp_dir: root} do
    write(root, "proc/meminfo", "MemTotal:       32249696 kB\n")

    assert HostMemory.bytes(env: %{"AWS_LAMBDA_FUNCTION_MEMORY_SIZE" => "10240"}, root: root) ==
             {:ok, 10 * @gib}
  end

  test "a junk Lambda variable is ignored", %{tmp_dir: root} do
    write(root, "proc/meminfo", "MemTotal:       1048576 kB\n")

    for junk <- ["", "lots", "0", "-1", "10GB"] do
      assert HostMemory.bytes(env: %{"AWS_LAMBDA_FUNCTION_MEMORY_SIZE" => junk}, root: root) ==
               {:ok, @gib}
    end
  end

  test "the smaller of the cgroup v2 limit and MemTotal", %{tmp_dir: root} do
    write(root, "proc/meminfo", "MemFree: 1 kB\nMemTotal:       32249696 kB\n")
    write(root, "sys/fs/cgroup/memory.max", "#{4 * @gib}\n")
    assert HostMemory.bytes(env: %{}, root: root) == {:ok, 4 * @gib}

    write(root, "sys/fs/cgroup/memory.max", "#{64 * @gib}\n")
    assert HostMemory.bytes(env: %{}, root: root) == {:ok, 32_249_696 * 1024}
  end

  test "an unlimited cgroup does not count", %{tmp_dir: root} do
    write(root, "proc/meminfo", "MemTotal:       2097152 kB\n")
    write(root, "sys/fs/cgroup/memory.max", "max\n")
    write(root, "sys/fs/cgroup/memory/memory.limit_in_bytes", "9223372036854771712\n")
    assert HostMemory.bytes(env: %{}, root: root) == {:ok, 2 * @gib}
  end

  test "a cgroup v1 limit alone", %{tmp_dir: root} do
    write(root, "sys/fs/cgroup/memory/memory.limit_in_bytes", "#{3 * @gib}\n")
    assert HostMemory.bytes(env: %{}, root: root) == {:ok, 3 * @gib}
  end

  test "nothing readable is an error", %{tmp_dir: root} do
    assert HostMemory.bytes(env: %{}, root: root) == :error
    write(root, "proc/meminfo", "garbage\n")
    assert HostMemory.bytes(env: %{}, root: root) == :error
  end

  test "this host reports something" do
    assert {:ok, bytes} = HostMemory.bytes()
    assert bytes > 0
  end
end
