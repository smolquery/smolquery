defmodule Smolquery.QueryService.ShardDescribeTest do
  use ExUnit.Case, async: false

  alias Smolquery.Engine.Connection
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

  describe "chunks/3 and max_chunks/1" do
    test "cuts the files into as many chunks as the engine has threads, at most the cap, in order" do
      urls = for n <- 1..20, do: "u#{n}"

      assert [[_, _, _, _, _], _, _, [_, _, _, _, _]] = ShardDescribe.chunks(urls, 8, 4)
      assert [[_, _, _, _, _], _, _, [_, _, _, _, _]] = ShardDescribe.chunks(urls, 64, 4)
      assert [[_ | _] = first, second] = ShardDescribe.chunks(urls, 2, 4)
      assert first ++ second == urls
      assert ShardDescribe.chunks(urls, 1, 4) == [urls]
      assert ShardDescribe.chunks(["u1"], 8, 4) == [["u1"]]
      assert ShardDescribe.chunks(["u1", "u2", "u3"], 8, 4) == [["u1"], ["u2"], ["u3"]]
      assert ShardDescribe.chunks([], 8, 4) == []
      assert [_, _, _, _, _, _, _] = ShardDescribe.chunks(urls, 8, 7)
    end

    test "max_chunks/1 is a fifth of the dirty I/O schedulers, at least 4 and at most 8 (T-723)" do
      assert ShardDescribe.max_chunks(10) == 4
      assert ShardDescribe.max_chunks(32) == 6
      assert ShardDescribe.max_chunks(64) == 8
    end
  end

  describe "files/3" do
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

    test "reads several files at once and answers what one statement does (T-719)", %{
      engine: engine,
      urls: urls,
      counters: counters
    } do
      assert {:ok, described} = ShardDescribe.files(engine, urls, [])
      assert :atomics.get(counters, 2) > 1

      {:ok, one} = Connection.query(engine.connection, FieldIds.sql(length(urls)), urls)
      assert described == FieldIds.by_file(one.rows)
      assert map_size(described) == 16
    end

    @tag threads: 2
    test "keeps to the engine's thread count", %{engine: engine, urls: urls, counters: counters} do
      assert {:ok, %{} = described} = ShardDescribe.files(engine, urls, [])
      assert map_size(described) == 16
      assert :atomics.get(counters, 2) == 2
    end

    test "every helper starts with the engine's bootstrap and the job's statements", %{
      engine: engine,
      urls: urls
    } do
      assert {:error, reason} = ShardDescribe.files(engine, urls, ["SET no_such_setting = 1"])
      assert inspect(reason) =~ "no_such_setting"
    end

    test "one chunk reads on the engine's own connection, and no files is nothing", %{
      engine: engine,
      urls: [url | _rest]
    } do
      assert {:ok, %{^url => %{columns: ["id", "name"]}}} =
               ShardDescribe.files(engine, [url], ["SET no_such_setting = 1"])

      assert ShardDescribe.files(engine, [], []) == {:ok, %{}}
    end

    test "a file the engine's own chunk cannot read is the batch's error, as an Adbc.Error", %{
      engine: engine,
      urls: urls
    } do
      assert {:error, %Adbc.Error{}} = ShardDescribe.files(engine, [missing(urls) | urls], [])
    end

    test "a file a helper cannot read is the batch's error, as an Adbc.Error", %{
      engine: engine,
      urls: urls
    } do
      assert {:error, %Adbc.Error{}} = ShardDescribe.files(engine, urls ++ [missing(urls)], [])
    end

    @tag delay_ms: 300
    test "an error waits for every helper, so no read is left running when it returns", %{
      engine: engine,
      urls: urls
    } do
      assert {:error, %Adbc.Error{}} = ShardDescribe.files(engine, [missing(urls) | urls], [])
      assert nif_readers() == 0
    end
  end

  defp missing(urls), do: String.replace(hd(urls), ~r/[^\/]+$/, "missing.parquet")

  defp nif_readers do
    Enum.count(Process.list(), fn pid ->
      match?(
        {:current_function, {Adbc.Nif, _function, _arity}},
        Process.info(pid, :current_function)
      )
    end)
  end
end
