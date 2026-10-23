defmodule Smolquery.QueryService.DescriptionCacheTest do
  use ExUnit.Case, async: true

  alias Smolquery.QueryService.DescriptionCache
  alias Smolquery.QueryService.Runtime
  alias Smolquery.Test.FixedCatalog

  @a %{"url" => "s3://lake/a.parquet", "snapshot" => 1, "bytes" => 100}
  @b %{"url" => "s3://lake/b.parquet", "snapshot" => 1, "bytes" => 200}
  @described %{
    "s3://lake/a.parquet" => %{ids: %{"id" => 1}, columns: ["id"]},
    "s3://lake/b.parquet" => %{ids: nil, columns: ["id", "name"]}
  }

  setup do
    name = :"description_cache_#{System.unique_integer([:positive])}"

    runtime =
      Runtime.new(
        name: name,
        catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}})
      )

    start_supervised!({DescriptionCache, runtime})

    %{table: Runtime.description_cache(name)}
  end

  test "keeps what a shard read, and answers it for the next, by URL", %{table: table} do
    assert DescriptionCache.fetch(table, [@a]) == {%{}, ["s3://lake/a.parquet"]}

    assert DescriptionCache.put(table, [@a, @b], @described, 10) == :ok

    c = %{"url" => "s3://lake/c.parquet", "snapshot" => 1, "bytes" => 300}

    assert DescriptionCache.fetch(table, [@a, c]) ==
             {%{"s3://lake/a.parquet" => %{ids: %{"id" => 1}, columns: ["id"]}},
              ["s3://lake/c.parquet"]}
  end

  test "a file registered again at the same URL is a new file (T-723)", %{table: table} do
    :ok = DescriptionCache.put(table, [@a], @described, 10)

    assert DescriptionCache.fetch(table, [%{@a | "snapshot" => 2}]) ==
             {%{}, ["s3://lake/a.parquet"]}

    assert DescriptionCache.fetch(table, [%{@a | "bytes" => 101}]) ==
             {%{}, ["s3://lake/a.parquet"]}
  end

  test "a put that would pass the ceiling empties the table first, and keeps at most it", %{
    table: table
  } do
    :ok = DescriptionCache.put(table, [@a, @b], @described, 3)
    c = %{"url" => "s3://lake/c.parquet", "snapshot" => 1, "bytes" => 300}
    d = %{"url" => "s3://lake/d.parquet", "snapshot" => 1, "bytes" => 400}

    :ok = DescriptionCache.put(table, [c], %{c["url"] => %{ids: nil, columns: ["x"]}}, 3)
    assert :ets.info(table, :size) == 3

    :ok = DescriptionCache.put(table, [d], %{d["url"] => %{ids: nil, columns: ["y"]}}, 3)
    assert :ets.info(table, :size) == 1

    assert {%{"s3://lake/d.parquet" => _d}, ["s3://lake/a.parquet"]} =
             DescriptionCache.fetch(table, [d, @a])

    :ok = DescriptionCache.put(table, [@a, @b], @described, 1)
    assert :ets.info(table, :size) == 1
  end

  test "no table, or one not running, holds nothing and keeps nothing" do
    assert DescriptionCache.fetch(nil, [@a]) == {%{}, ["s3://lake/a.parquet"]}
    assert DescriptionCache.put(nil, [@a], @described, 10) == :ok
    assert DescriptionCache.fetch(:not_running_cache, [@a]) == {%{}, ["s3://lake/a.parquet"]}
    assert DescriptionCache.put(:not_running_cache, [@a], @described, 10) == :ok
  end

  test "max_entries/0 is 10,000 unless the application config says otherwise" do
    assert DescriptionCache.max_entries() == 10_000
  end
end
