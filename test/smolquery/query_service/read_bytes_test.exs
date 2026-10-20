defmodule Smolquery.QueryService.ReadBytesTest do
  use ExUnit.Case, async: true

  alias Smolquery.Engine
  alias Smolquery.QueryService.ReadBytes
  alias Smolquery.Schema
  alias Smolquery.Segments.Store.Local
  alias Smolquery.Test.SegmentFixture
  alias Smolquery.Test.SegmentServer

  @moduletag :tmp_dir

  @get_206 "{'request': {'type': GET, 'url': 'http://h/f.parquet', 'headers': {Range='bytes=0-16383'}, 'duration_ms': 3}, 'response': {'status': PartialContent_206, 'reason': Partial Content, 'headers': {content-length=16384, content-range='bytes 0-16383/72858'}}}"
  @get_200 "{'request': {'type': GET, 'url': 'http://h/f.parquet', 'headers': {}}, 'response': {'status': OK_200, 'reason': OK, 'headers': {content-length=72858}}}"
  @head "{'request': {'type': HEAD, 'url': 'http://h/f.parquet', 'headers': {}}, 'response': {'status': OK_200, 'reason': OK, 'headers': {content-length=72858}}}"
  @failed "{'request': {'type': GET, 'url': 'http://h/f.parquet', 'headers': {Range='bytes=0-16383'}, 'duration_ms': 1}, 'response': NULL}"
  @missing "{'request': {'type': GET, 'url': 'http://h/absent.parquet', 'headers': {}}, 'response': {'status': NotFound_404, 'reason': Not Found, 'headers': {content-length=9}}}"

  describe "sum/1" do
    test "adds the bodies of answered GETs and counts every answered request" do
      assert ReadBytes.sum([@get_206, @get_200, @head, @failed, @missing]) ==
               %{bytes: 16_384 + 72_858, requests: 4}

      assert ReadBytes.sum([]) == %{bytes: 0, requests: 0}
    end
  end

  describe "enable/1 and measure/1" do
    test "measure what a scan fetched over HTTP, and nothing for a local file", %{tmp_dir: dir} do
      schema = Schema.new!([{"id", :int64}, {"name", :string}])
      rows = for i <- 1..5_000, do: %{"id" => i, "name" => "n#{i}"}
      {:ok, segment} = SegmentFixture.write(rows, schema, store: Local.new(dir: dir))
      server = start_supervised!(SegmentServer.bandit_spec(dir))
      url = SegmentServer.base_url(server) <> "/" <> Path.basename(segment.path)
      engine = :"read_bytes_#{System.unique_integer([:positive])}"
      start_supervised!({Engine, name: engine, extensions: [:httpfs]})
      connection = Engine.connection_name(engine)

      assert :ok = ReadBytes.enable(connection)
      assert {:ok, %{bytes: 0, requests: 0}} = ReadBytes.measure(connection)

      Engine.query!(engine, "SELECT sum(id) FROM read_parquet('#{segment.path}')")
      assert {:ok, %{bytes: 0, requests: 0}} = ReadBytes.measure(connection)

      Engine.query!(engine, "SELECT sum(id) FROM read_parquet('#{url}')")
      assert {:ok, %{bytes: bytes, requests: requests}} = ReadBytes.measure(connection)
      assert bytes > 0 and requests >= 1
    end
  end
end
