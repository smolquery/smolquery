defmodule Smolquery.QueryService.SealedBoundsTest do
  use ExUnit.Case, async: true

  alias Smolquery.QueryService.SealedBounds
  alias Smolquery.Schema

  @schema Schema.new!([
            {"id", :int64, id: 1},
            {"amount", :float64, id: 2},
            {"ts", :timestamp, id: 3},
            {"day", :date, id: 4},
            {"name", :string, id: 5},
            {"price", {:numeric, 10, 2}, id: 6}
          ])
  @ids Schema.field_ids(@schema)

  defp file(bounds), do: %{path: "f.parquet", rows: 1, bytes: 1, column_bytes: 1, bounds: bounds}

  defp bound(min, max, extra \\ %{}),
    do: Map.merge(%{min: min, max: max, null_count: 0, nan: false}, extra)

  describe "prunable/2" do
    test "names the conjunct columns whose catalog bounds are exact" do
      conjuncts = [
        {"id", :gt, 1},
        {"name", :eq, "x"},
        {"ts", :lt, ~N[2024-01-01 00:00:00]},
        {"price", :gt, 1},
        {"id", :lt, 9}
      ]

      assert SealedBounds.prunable(@schema, conjuncts) == ["id", "ts"]
    end
  end

  describe "keep?/4" do
    test "an integer bound outside the file's range drops it, inside keeps it" do
      file = file(%{1 => bound("1001", "1010")})

      refute SealedBounds.keep?(file, @schema, [{"id", :gt, 1010}], @ids)
      refute SealedBounds.keep?(file, @schema, [{"id", :lt, 1001}], @ids)
      assert SealedBounds.keep?(file, @schema, [{"id", :ge, 1010}], @ids)
      assert SealedBounds.keep?(file, @schema, [{"id", :eq, 1005}], @ids)
      refute SealedBounds.keep?(file, @schema, [{"id", :eq, 2000}], @ids)
    end

    test "timestamps and dates read as the catalog prints them" do
      file =
        file(%{
          3 => bound("2024-01-01 00:00:00", "2024-01-31 23:59:59.5"),
          4 => bound("2024-01-01", "2024-01-31")
        })

      refute SealedBounds.keep?(file, @schema, [{"ts", :gt, ~N[2024-02-01 00:00:00]}], @ids)
      assert SealedBounds.keep?(file, @schema, [{"ts", :gt, ~N[2024-01-31 23:59:59]}], @ids)
      refute SealedBounds.keep?(file, @schema, [{"day", :lt, ~D[2024-01-01]}], @ids)
      assert SealedBounds.keep?(file, @schema, [{"day", :le, ~D[2024-01-01]}], @ids)
    end

    test "a double column that holds a NaN, an unparsable bound, or no bound at all keeps the file" do
      assert SealedBounds.keep?(
               file(%{2 => bound("1.5", "2.5", %{nan: true})}),
               @schema,
               [{"amount", :gt, 3.0}],
               @ids
             )

      refute SealedBounds.keep?(
               file(%{2 => bound("1.5", "2.5")}),
               @schema,
               [{"amount", :gt, 3.0}],
               @ids
             )

      assert SealedBounds.keep?(
               file(%{1 => bound("one", "ten")}),
               @schema,
               [{"id", :gt, 10}],
               @ids
             )

      assert SealedBounds.keep?(file(%{1 => bound(nil, nil)}), @schema, [{"id", :gt, 10}], @ids)
      assert SealedBounds.keep?(file(%{}), @schema, [{"id", :gt, 10}], @ids)
    end

    test "a bound on a string or decimal column never prunes" do
      file = file(%{5 => bound("a", "b"), 6 => bound("1.00", "2.00")})

      assert SealedBounds.keep?(file, @schema, [{"name", :gt, "z"}], @ids)
      assert SealedBounds.keep?(file, @schema, [{"price", :gt, 5}], @ids)
    end

    test "every conjunct must leave a match possible" do
      file =
        file(%{1 => bound("1", "10"), 3 => bound("2024-01-01 00:00:00", "2024-01-02 00:00:00")})

      assert SealedBounds.keep?(
               file,
               @schema,
               [{"id", :gt, 5}, {"ts", :lt, ~N[2024-01-02 00:00:00]}],
               @ids
             )

      refute SealedBounds.keep?(
               file,
               @schema,
               [{"id", :gt, 5}, {"ts", :gt, ~N[2024-01-03 00:00:00]}],
               @ids
             )
    end
  end
end
