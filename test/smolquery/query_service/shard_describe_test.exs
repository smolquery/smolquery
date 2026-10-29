defmodule Smolquery.QueryService.ShardDescribeTest do
  use ExUnit.Case, async: false

  alias Smolquery.Engine.CallExited
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result
  alias Smolquery.QueryService.JobEngine
  alias Smolquery.QueryService.ShardDescribe
  alias Smolquery.Schema
  alias Smolquery.Segments.FieldIds
  alias Smolquery.Segments.Store.Local
  alias Smolquery.Test.SegmentFixture

  @moduletag :tmp_dir

  defmodule SlowServer do
    @moduledoc false
    @behaviour Plug

    import Plug.Conn

    @impl Plug
    def init(opts), do: opts

    @impl Plug
    def call(conn, %{dir: dir, counters: counters, delay_ms: delay_ms}) do
      in_flight = :atomics.add_get(counters, 1, 1)
      record_peak(counters, in_flight)

      try do
        Process.sleep(delay_ms)
        serve(conn, Path.join(dir, Path.basename(conn.request_path)))
      after
        :atomics.sub(counters, 1, 1)
      end
    end

    defp serve(conn, path) do
      case {File.read(path), conn.method, get_req_header(conn, "range")} do
        {{:error, _enoent}, _method, _range} ->
          send_resp(conn, 404, "not found")

        {{:ok, data}, "GET", ["bytes=" <> range]} ->
          {first, last} = bounds(range, byte_size(data))

          conn
          |> put_resp_header("content-range", "bytes #{first}-#{last}/#{byte_size(data)}")
          |> send_resp(206, binary_part(data, first, last - first + 1))

        {{:ok, _data}, _head_or_whole, _range} ->
          conn |> put_resp_header("accept-ranges", "bytes") |> send_file(200, path)
      end
    end

    defp bounds(range, size) do
      case String.split(range, "-") do
        ["", suffix] -> {size - String.to_integer(suffix), size - 1}
        [first, ""] -> {String.to_integer(first), size - 1}
        [first, last] -> {String.to_integer(first), min(String.to_integer(last), size - 1)}
      end
    end

    defp record_peak(counters, in_flight) do
      peak = :atomics.get(counters, 2)

      if in_flight > peak and :atomics.compare_exchange(counters, 2, peak, in_flight) != :ok,
        do: record_peak(counters, in_flight)
    end
  end

  defmodule FakeConnection do
    @moduledoc false
    use GenServer

    def start(test, script), do: GenServer.start(__MODULE__, {test, script}, name: __MODULE__)

    def stop do
      case Process.whereis(__MODULE__) do
        nil -> :ok
        pid -> GenServer.stop(pid)
      end
    end

    @impl GenServer
    def init(state), do: {:ok, state}

    @impl GenServer
    def handle_call({:query, sql, _params}, _from, {test, script} = state) do
      send(test, {:sql, sql})

      case script.(sql) do
        :exit -> {:stop, :read_exited, state}
        reply -> {:reply, reply, state}
      end
    end

    @impl GenServer
    def terminate(:read_exited, {test, script}) do
      Process.unregister(__MODULE__)
      {:ok, _successor} = start(test, script)
    end

    def terminate(_reason, _state), do: :ok
  end

  describe "files/2" do
    setup %{tmp_dir: dir} = context do
      schema = Schema.new!([{"id", :int64}, {"name", :string}])
      store = Local.new(dir: dir)

      paths =
        for n <- 1..16 do
          {:ok, segment} =
            SegmentFixture.write([%{"id" => n, "name" => "n#{n}"}], schema, store: store)

          segment.path
        end

      counters = :atomics.new(2, [])

      server =
        start_supervised!(
          {Bandit,
           plug:
             {SlowServer,
              %{
                dir: Path.dirname(hd(paths)),
                counters: counters,
                delay_ms: Map.get(context, :delay_ms, 20)
              }},
           port: 0,
           startup_log: false}
        )

      {:ok, {_address, port}} = ThousandIsland.listener_info(server)
      urls = Enum.map(paths, &"http://127.0.0.1:#{port}/#{Path.basename(&1)}")

      {:ok, engine} =
        JobEngine.start(
          extensions: [:httpfs],
          settings: [threads: Map.get(context, :threads, 8)],
          statements: ["SET http_retries = 0"]
        )

      on_exit(fn -> JobEngine.stop(engine) end)

      %{engine: engine, urls: urls, counters: counters}
    end

    test "reads several files at once and answers what one statement does (T-719, T-759)", %{
      engine: engine,
      urls: urls,
      counters: counters
    } do
      assert {:ok, described} = ShardDescribe.files(engine, urls)
      assert :atomics.get(counters, 2) > 1

      {:ok, one} = Connection.query(engine.connection, FieldIds.sql(length(urls)), urls)
      assert {:ok, described} == FieldIds.by_file(one.rows)
      assert map_size(described) == 16
    end

    @tag threads: 2
    test "keeps to the engine's thread count", %{engine: engine, urls: urls, counters: counters} do
      assert {:ok, %{} = described} = ShardDescribe.files(engine, urls)
      assert map_size(described) == 16
      assert :atomics.get(counters, 2) == 2
    end

    test "puts preserve_insertion_order back on after a describe", %{engine: engine, urls: urls} do
      assert {:ok, _described} = ShardDescribe.files(engine, urls)
      assert insertion_order(engine) == true
    end

    test "puts preserve_insertion_order back on after a describe that fails", %{
      engine: engine,
      urls: urls
    } do
      assert {:error, %Adbc.Error{}} = ShardDescribe.files(engine, [missing(urls) | urls])
      assert insertion_order(engine) == true
    end

    test "leaves preserve_insertion_order off when it was off", %{
      engine: engine,
      urls: urls,
      counters: counters
    } do
      {:ok, _result} = Connection.query(engine.connection, "SET preserve_insertion_order = false")

      assert {:ok, %{} = described} = ShardDescribe.files(engine, urls)
      assert map_size(described) == 16
      assert :atomics.get(counters, 2) > 1
      assert insertion_order(engine) == false
    end

    test "one file reads, and no files is nothing", %{engine: engine, urls: [url | _rest]} do
      assert {:ok, %{^url => %{columns: ["id", "name"]}}} = ShardDescribe.files(engine, [url])
      assert ShardDescribe.files(engine, []) == {:ok, %{}}
    end

    test "a file the engine cannot read is the batch's error, as an Adbc.Error", %{
      engine: engine,
      urls: urls
    } do
      assert {:error, %Adbc.Error{}} = ShardDescribe.files(engine, urls ++ [missing(urls)])
    end
  end

  test "a connection that is gone answers a CallExited" do
    gone = spawn(fn -> :ok end)
    ref = Process.monitor(gone)
    assert_receive {:DOWN, ^ref, :process, ^gone, _reason}

    assert {:error, %CallExited{}} =
             ShardDescribe.files(%{connection: gone}, ["http://x/a.parquet"])
  end

  describe "files/2 against a scripted connection" do
    @tag :capture_log
    test "a read that exits still puts the setting back, and answers the read's exit" do
      fake(fn
        "SELECT current_setting" <> _rest -> {:ok, %Result{rows: [[true]]}}
        "SET " <> _rest -> {:ok, %Result{}}
        "SELECT file_name" <> _rest -> :exit
      end)

      assert {:error, %CallExited{reason: :read_exited}} =
               ShardDescribe.files(%{connection: FakeConnection}, ["a.parquet"])

      assert_received {:sql, "SET preserve_insertion_order = false"}
      assert_receive {:sql, "SET preserve_insertion_order = true"}
    end

    test "a setting of another shape is an error, and nothing is set" do
      fake(fn "SELECT current_setting" <> _rest -> {:ok, %Result{rows: [["true"]]}} end)

      assert {:error, {:unexpected_setting, :preserve_insertion_order, [["true"]]}} =
               ShardDescribe.files(%{connection: FakeConnection}, ["a.parquet"])

      refute_received {:sql, "SET " <> _rest}
    end

    test "a setting that cannot be put back fails a read that succeeded" do
      fake(fn
        "SELECT current_setting" <> _rest -> {:ok, %Result{rows: [[true]]}}
        "SET preserve_insertion_order = false" -> {:ok, %Result{}}
        "SET preserve_insertion_order = true" -> {:error, %Adbc.Error{message: "restore"}}
        "SELECT file_name" <> _rest -> {:ok, %Result{rows: [["a.parquet", "s", 0, nil]]}}
      end)

      assert {:error, %Adbc.Error{message: "restore"}} =
               ShardDescribe.files(%{connection: FakeConnection}, ["a.parquet"])
    end

    test "a read that fails keeps its own error when the setting cannot be put back either" do
      fake(fn
        "SELECT current_setting" <> _rest -> {:ok, %Result{rows: [[true]]}}
        "SET preserve_insertion_order = false" -> {:ok, %Result{}}
        "SET preserve_insertion_order = true" -> {:error, %Adbc.Error{message: "restore"}}
        "SELECT file_name" <> _rest -> {:error, %Adbc.Error{message: "read"}}
      end)

      assert {:error, %Adbc.Error{message: "read"}} =
               ShardDescribe.files(%{connection: FakeConnection}, ["a.parquet"])
    end
  end

  defp fake(script) do
    {:ok, _pid} = FakeConnection.start(self(), script)
    on_exit(fn -> FakeConnection.stop() end)
  end

  defp insertion_order(engine) do
    {:ok, %{rows: [[preserved]]}} =
      Connection.query(engine.connection, "SELECT current_setting('preserve_insertion_order')")

    preserved
  end

  defp missing(urls), do: String.replace(hd(urls), ~r/[^\/]+$/, "missing.parquet")
end
