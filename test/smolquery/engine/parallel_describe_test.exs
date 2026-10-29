defmodule Smolquery.Engine.ParallelDescribeTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Smolquery.DuckDB
  alias Smolquery.Engine.CallExited
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.ParallelDescribe
  alias Smolquery.Schema
  alias Smolquery.Segments.FieldIds
  alias Smolquery.Segments.Store.Local
  alias Smolquery.Test.SegmentFixture

  describe "chunks/3 and max_chunks/1" do
    test "cuts the files into as many chunks as the engine has threads, at most the cap, in order" do
      urls = for n <- 1..20, do: "u#{n}"

      assert [[_, _, _, _, _], _, _, [_, _, _, _, _]] = ParallelDescribe.chunks(urls, 8, 4)
      assert [[_, _, _, _, _], _, _, [_, _, _, _, _]] = ParallelDescribe.chunks(urls, 64, 4)
      assert [[_ | _] = first, second] = ParallelDescribe.chunks(urls, 2, 4)
      assert first ++ second == urls
      assert ParallelDescribe.chunks(urls, 1, 4) == [urls]
      assert ParallelDescribe.chunks(["u1"], 8, 4) == [["u1"]]
      assert ParallelDescribe.chunks(["u1", "u2", "u3"], 8, 4) == [["u1"], ["u2"], ["u3"]]
      assert ParallelDescribe.chunks([], 8, 4) == []
      assert [_, _, _, _, _, _, _] = ParallelDescribe.chunks(urls, 8, 7)
    end

    test "lanes/4 cuts each chunk into reads of at most max_chunk_files, never adding a lane" do
      urls = for n <- 1..20, do: "u#{n}"

      assert ParallelDescribe.lanes(urls, 8, 4, nil) ==
               Enum.map(ParallelDescribe.chunks(urls, 8, 4), &[&1])

      lanes = ParallelDescribe.lanes(urls, 8, 4, 2)
      assert [_, _, _, _] = lanes
      assert lanes |> Enum.concat() |> Enum.all?(&(Enum.count_until(&1, 3) <= 2))
      assert lanes |> List.flatten() == urls
      assert [_, _] = ParallelDescribe.lanes(urls, 2, 4, 1)
    end

    test "max_chunks/1 is a fifth of the dirty I/O schedulers, at least 4 and at most 8 (T-723)" do
      assert ParallelDescribe.max_chunks(10) == 4
      assert ParallelDescribe.max_chunks(32) == 6
      assert ParallelDescribe.max_chunks(64) == 8
    end
  end

  describe "files/3" do
    @describetag :tmp_dir

    setup %{tmp_dir: dir} do
      schema = Schema.new!([{"id", :int64}, {"name", :string}])
      store = Local.new(dir: dir)

      paths =
        for n <- 1..8 do
          {:ok, segment} =
            SegmentFixture.write([%{"id" => n, "name" => "n#{n}"}], schema, store: store)

          segment.path
        end

      database = start_supervised!({DuckDB, []})
      connection = start_supervised!({Connection, database: database, settings: [threads: 4]})

      %{engine: %{connection: connection, database: database}, paths: paths}
    end

    test "answers what one statement does, over helpers and bounded chunks alike", %{
      engine: engine,
      paths: paths
    } do
      {:ok, one} = Connection.query(engine.connection, FieldIds.sql(length(paths)), paths)
      {:ok, expected} = FieldIds.by_file(one.rows)

      assert ParallelDescribe.files(engine, paths, []) == {:ok, expected}
      assert ParallelDescribe.files(engine, paths, max_chunk_files: 1) == {:ok, expected}
      assert ParallelDescribe.files(engine, [], []) == {:ok, %{}}
    end

    test "every helper runs the statements, the engine's own connection does not", %{
      engine: engine,
      paths: [path | _rest] = paths
    } do
      statements = ["SET no_such_setting = 1"]

      assert {:ok, %{^path => %{columns: ["id", "name"]}}} =
               ParallelDescribe.files(engine, [path], statements: statements)

      assert {:error, {:describe_helper_failed, reason}} =
               ParallelDescribe.files(engine, paths, statements: statements)

      assert inspect(reason) =~ "no_such_setting"
    end

    test "a file no chunk can read is the batch's error, as an Adbc.Error", %{
      engine: engine,
      paths: paths
    } do
      missing = Path.join(Path.dirname(hd(paths)), "missing.parquet")

      assert {:error, %Adbc.Error{}} = ParallelDescribe.files(engine, [missing | paths], [])
      assert {:error, %Adbc.Error{}} = ParallelDescribe.files(engine, paths ++ [missing], [])
    end

    test "a call to the engine's own connection that times out is a CallExited, not an exit", %{
      engine: engine,
      paths: paths
    } do
      capture_log(fn ->
        assert {:error, %CallExited{reason: :timeout}} =
                 ParallelDescribe.files(engine, paths, timeout: 0)
      end)
    end
  end
end
