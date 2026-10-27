defmodule Smolquery.QueryService.WorkerTransportPeerTest do
  @moduledoc """
  The remote half of `Smolquery.QueryService.WorkerTransport` against a real
  peer node (T-364): the call crosses gen_rpc, lands on the peer's
  `PartialWorker`, and the peer's allowlist admits it. The peer runs no
  query service, so the answer is the worker's own
  `:query_service_unavailable` — proof the call executed there rather than
  failing in transit.
  """

  use ExUnit.Case, async: false

  alias Smolquery.QueryService.WorkerTransport
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.FixedCatalog

  @moduletag :integration

  @primary_port 15_371
  @peer_port 15_373
  @slow_sql "SELECT max(a.range * b.range) FROM range(100000) a, range(100000) b"
  @request %{
    table_ref: {"analytics", "events"},
    schema: Smolquery.Schema.new!([{"id", :int64}]),
    files: [],
    partial_sql: "SELECT 1",
    allowed_paths: []
  }

  setup do
    ensure_distributed()
    {:ok, _peer, node} = start_peer()

    :erpc.call(node, :code, :add_paths, [:code.get_path()])

    peer_gen_rpc = [
      tcp_server_port: @peer_port,
      tcp_client_port: @peer_port,
      rpc_module_control: :whitelist,
      rpc_module_list: [Smolquery.QueryService.PartialWorker],
      client_config_per_node: {:internal, %{node() => @primary_port}}
    ]

    for {key, value} <- peer_gen_rpc do
      :erpc.call(node, :application, :set_env, [:gen_rpc, key, value, [persistent: true]])
    end

    {:ok, _started} = :erpc.call(node, Application, :ensure_all_started, [:gen_rpc])

    previous_per_node = :application.get_env(:gen_rpc, :client_config_per_node)

    :application.set_env(:gen_rpc, :client_config_per_node, {:internal, %{node => @peer_port}},
      persistent: true
    )

    on_exit(fn -> restore_per_node(previous_per_node) end)

    %{node: node}
  end

  test "reaches the peer's PartialWorker over gen_rpc", %{node: node} do
    assert node in Node.list()

    assert WorkerTransport.call(node, :never_started, @request, "job", 5_000) ==
             {:error, :query_service_unavailable}

    assert node in :gen_rpc.nodes()
  end

  test "a module outside the peer's allowlist is refused", %{node: node} do
    destination = WorkerTransport.destination(node, "job")

    assert {:badrpc, :unauthorized} = :gen_rpc.call(destination, Map, :new, [], 5_000)
  end

  describe "a peer running the query service" do
    @describetag :tmp_dir

    setup %{node: node, tmp_dir: tmp_dir} do
      :ok = :erpc.call(node, :application, :set_env, [:smolquery, :spill_dir, tmp_dir])
      {:ok, _started} = :erpc.call(node, Application, :ensure_all_started, [:adbc])

      %{peer_pid: :erpc.call(node, System, :pid, []), tmp_dir: tmp_dir}
    end

    test "a shard whose caller gave up stops running on the peer", context do
      %{node: node, peer_pid: peer_pid, tmp_dir: tmp_dir} = context
      name = start_query_service(node, [])

      caller =
        spawn(fn -> WorkerTransport.call(node, name, slow_request(60_000), "job", 60_000) end)

      assert Eventually.until(fn -> shard_directories(tmp_dir, peer_pid) != [] end, 200, 25),
             "the shard never started on the peer"

      Process.exit(caller, :kill)

      assert Eventually.until(fn -> shard_directories(tmp_dir, peer_pid) == [] end, 100, 20),
             "the peer still runs the abandoned shard: #{inspect(shard_directories(tmp_dir, peer_pid))}"
    end

    test "a peer with max_concurrent_shards 1 refuses the shards past its bound", context do
      %{node: node, peer_pid: peer_pid, tmp_dir: tmp_dir} = context
      name = start_query_service(node, distributed: [max_concurrent_shards: 1])

      running =
        spawn(fn -> WorkerTransport.call(node, name, slow_request(60_000), "job-0", 60_000) end)

      assert Eventually.until(fn -> shard_directories(tmp_dir, peer_pid) != [] end, 200, 25),
             "no shard started on the peer"

      refused =
        1..3
        |> Task.async_stream(
          &WorkerTransport.call(node, name, slow_request(60_000), "job-#{&1}", 60_000),
          timeout: 10_000
        )
        |> Enum.map(fn {:ok, result} -> result end)

      shards = Enum.count(shard_directories(tmp_dir, peer_pid))
      Process.exit(running, :kill)

      assert refused == List.duplicate({:error, :too_many_shards}, 3)
      assert shards == 1, "peer ran #{shards} shards at once with max_concurrent_shards: 1"
    end
  end

  defp slow_request(timeout_ms),
    do: Map.merge(@request, %{partial_sql: @slow_sql, timeout_ms: timeout_ms})

  defp start_query_service(node, options) do
    name = :"peer_query_#{:erlang.unique_integer([:positive])}"

    options =
      Keyword.merge(
        [
          name: name,
          catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}}),
          engine_extensions: [],
          warm_engines: 0
        ],
        options
      )

    code = """
    {:ok, pid} =
      Supervisor.start_link([{Smolquery.QueryService.Supervisor, options}], strategy: :one_for_one)

    Process.unlink(pid)
    pid
    """

    {_pid, _binding} = :erpc.call(node, Code, :eval_string, [code, [options: options]])

    name
  end

  defp shard_directories(tmp_dir, peer_pid),
    do: Path.wildcard(Path.join(tmp_dir, "smolquery-partial-#{peer_pid}-*"))

  defp ensure_distributed do
    case Node.start(:"smolquery_primary@127.0.0.1", :longnames) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    Node.set_cookie(:smolquery_test_cookie)
  end

  defp start_peer do
    {:ok, peer, node} =
      :peer.start_link(%{
        name: :"worker_peer_#{:erlang.unique_integer([:positive])}",
        host: ~c"127.0.0.1",
        longnames: true,
        args: [~c"-setcookie", ~c"smolquery_test_cookie"]
      })

    on_exit(fn -> safely_stop(peer) end)

    {:ok, peer, node}
  end

  defp safely_stop(peer) do
    :peer.stop(peer)
  catch
    :exit, _reason -> :ok
  end

  defp restore_per_node({:ok, value}),
    do: :application.set_env(:gen_rpc, :client_config_per_node, value, persistent: true)

  defp restore_per_node(:undefined),
    do: :application.unset_env(:gen_rpc, :client_config_per_node, persistent: true)
end
