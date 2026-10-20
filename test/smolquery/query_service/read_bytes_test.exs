defmodule Smolquery.QueryService.ReadBytesTest do
  use ExUnit.Case, async: true

  alias Smolquery.Engine
  alias Smolquery.QueryService.ReadBytes
  alias Smolquery.Schema
  alias Smolquery.Segments.Store.Local
  alias Smolquery.Test.SegmentFixture
  alias Smolquery.Test.SegmentServer

  @moduletag :tmp_dir

  test "measures what a scan fetched over HTTP, body bytes of answered GETs only, and nothing for a local file",
       %{tmp_dir: dir} do
    schema = Schema.new!([{"id", :int64}, {"name", :string}])
    rows = for i <- 1..5_000, do: %{"id" => i, "name" => "n#{i}"}
    {:ok, segment} = SegmentFixture.write(rows, schema, store: Local.new(dir: dir))
    server = start_supervised!(SegmentServer.bandit_spec(dir))
    url = SegmentServer.base_url(server) <> "/" <> Path.basename(segment.path)
    engine = :"read_bytes_#{System.unique_integer([:positive])}"
    start_supervised!({Engine, name: engine, extensions: [:httpfs]})
    connection = Engine.connection_name(engine)

    assert ReadBytes.measured(connection) == %{bytes: 0, requests: 0}
    assert ReadBytes.enabled?(connection)
    assert ReadBytes.measured(connection) == %{bytes: 0, requests: 0}

    Engine.query!(engine, "SELECT sum(id) FROM read_parquet('#{segment.path}')")
    assert ReadBytes.measured(connection) == %{bytes: 0, requests: 0}

    Engine.query!(engine, "SELECT sum(id) FROM read_parquet('#{url}')")
    assert %{bytes: bytes, requests: requests} = ReadBytes.measured(connection)
    assert requests >= 2
    assert bytes >= segment.byte_size
    assert bytes < requests * segment.byte_size
  end
end
