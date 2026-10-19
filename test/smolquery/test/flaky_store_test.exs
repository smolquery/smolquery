defmodule Smolquery.Test.FlakyStoreTest do
  use ExUnit.Case, async: true

  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Store.Local
  alias Smolquery.Test.FakeParquet
  alias Smolquery.Test.FlakyStore

  @moduletag :tmp_dir

  test "refuses the named puts as many times as asked, and passes everything else through",
       %{tmp_dir: tmp_dir} do
    inner = Local.new(dir: tmp_dir)
    store = FlakyStore.new(inner, %{"/x.parquet" => 2, "/y.parquet" => :infinity})
    write = fn staged -> File.write(staged, FakeParquet.bytes()) end

    assert {:error, {:put_failed, "a/x.parquet", :flaky}} = Store.put(store, "a/x.parquet", write)
    assert {:error, {:put_failed, "a/x.parquet", :flaky}} = Store.put(store, "a/x.parquet", write)
    assert {:ok, %{location: location}} = Store.put(store, "a/x.parquet", write)
    assert location == Store.location(inner, "a/x.parquet")

    assert {:error, {:put_failed, "a/y.parquet", :flaky}} = Store.put(store, "a/y.parquet", write)
    assert {:error, {:put_failed, "a/y.parquet", :flaky}} = Store.put(store, "a/y.parquet", write)
    assert {:ok, _put} = Store.put(store, "a/z.parquet", write)

    assert {:ok, ["a/x.parquet", "a/z.parquet"]} = Store.list(store, "a")
    assert :ok = Store.delete(store, "a/z.parquet")
    assert {:ok, ["a/x.parquet"]} = Store.list(inner, "a")
    assert Store.shared?(store) == Store.shared?(inner)
  end
end
