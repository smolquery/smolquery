defmodule Smolquery.Segments.FieldIdsTest do
  use ExUnit.Case, async: true

  alias Smolquery.Segments.FieldIds

  test "sql/1 binds one placeholder per file" do
    assert FieldIds.sql(2) ==
             "SELECT file_name, name, num_children, field_id FROM parquet_schema([$1, $2])"
  end

  test "by_file/1 keeps the top-level columns, skips a MAP's children, and reads ids only when every column has one" do
    rows = [
      ["a.parquet", "duckdb_schema", 3, nil],
      ["a.parquet", "id", nil, 1],
      ["a.parquet", "attrs", 1, 3],
      ["a.parquet", "key_value", 2, nil],
      ["a.parquet", "key", nil, nil],
      ["a.parquet", "value", nil, nil],
      ["a.parquet", "ts", nil, 4],
      ["b.parquet", "duckdb_schema", 2, nil],
      ["b.parquet", "id", nil, nil],
      ["b.parquet", "ts", nil, 4]
    ]

    assert {:ok, described} = FieldIds.by_file(rows)

    assert described == %{
             "a.parquet" => %{
               ids: %{"id" => 1, "attrs" => 3, "ts" => 4},
               columns: ["id", "attrs", "ts"]
             },
             "b.parquet" => %{ids: nil, columns: ["id", "ts"]}
           }

    assert FieldIds.ids(described) == %{
             "a.parquet" => %{"id" => 1, "attrs" => 3, "ts" => 4},
             "b.parquet" => nil
           }
  end

  test "by_file/1 answers an error for a file whose rows do not walk as one tree (T-759)" do
    root = ["a.parquet", "duckdb_schema", 2, nil]
    struct = ["a.parquet", "s", 1, 2]
    child = ["a.parquet", "x", nil, nil]
    leaf = ["a.parquet", "id", nil, 1]

    assert {:ok, %{"a.parquet" => %{columns: ["id", "s"]}}} =
             FieldIds.by_file([root, leaf, struct, child])

    unreadable = {:error, {:unreadable_schema, "a.parquet"}}
    assert FieldIds.by_file([struct, child, root, leaf]) == unreadable
    assert FieldIds.by_file([root, leaf, struct]) == unreadable
    assert FieldIds.by_file([root, leaf, child, struct]) == unreadable
  end
end
