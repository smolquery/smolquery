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

  defp file(bounds), do: %{path: "f.parquet", rows: 1, bytes: 1, column_bytes: 1, bounds: bounds}

  defp bound(min, max, extra \\ %{}),
    do: Map.merge(%{min: min, max: max, null_count: 0, nan: false}, extra)

  defp keep?(file, conjuncts),
    do: SealedBounds.keep?(file, SealedBounds.columns(@schema, conjuncts), conjuncts)

  describe "columns/2" do
    test "names the conjunct columns whose catalog bounds are exact, with their ids and types" do
      conjuncts = [
        {"id", :gt, 1},
        {"name", :eq, "x"},
        {"ts", :lt, ~N[2024-01-01 00:00:00]},
        {"price", :gt, 1},
        {"id", :lt, 9}
      ]

      assert SealedBounds.columns(@schema, conjuncts) == [
               {"id", 1, :int64},
               {"ts", 3, :timestamp}
             ]

      assert SealedBounds.columns(@schema, []) == []
    end
  end

  describe "keep?/3" do
    test "an integer bound outside the file's range drops it, inside keeps it" do
      file = file(%{1 => bound("1001", "1010")})

      refute keep?(file, [{"id", :gt, 1010}])
      refute keep?(file, [{"id", :lt, 1001}])
      assert keep?(file, [{"id", :ge, 1010}])
      assert keep?(file, [{"id", :eq, 1005}])
      refute keep?(file, [{"id", :eq, 2000}])
    end

    test "timestamps and dates read as the catalog prints them" do
      file =
        file(%{
          3 => bound("2024-01-01 00:00:00", "2024-01-31 23:59:59.5"),
          4 => bound("2024-01-01", "2024-01-31")
        })

      refute keep?(file, [{"ts", :gt, ~N[2024-02-01 00:00:00]}])
      assert keep?(file, [{"ts", :gt, ~N[2024-01-31 23:59:59]}])
      refute keep?(file, [{"day", :lt, ~D[2024-01-01]}])
      assert keep?(file, [{"day", :le, ~D[2024-01-01]}])
    end

    test "a double column prunes only when the catalog says it holds no NaN" do
      refute keep?(file(%{2 => bound("1.5", "2.5")}), [{"amount", :gt, 3.0}])
      assert keep?(file(%{2 => bound("1.5", "2.5", %{nan: true})}), [{"amount", :gt, 3.0}])
      assert keep?(file(%{2 => bound("1.5", "2.5", %{nan: nil})}), [{"amount", :gt, 3.0}])
    end

    test "an unparsable bound, or no bound at all, keeps the file" do
      assert keep?(file(%{1 => bound("one", "ten")}), [{"id", :gt, 10}])

      assert keep?(file(%{3 => bound("yesterday", "today")}), [
               {"ts", :gt, ~N[2024-01-01 00:00:00]}
             ])

      assert keep?(file(%{1 => bound(nil, nil)}), [{"id", :gt, 10}])
      assert keep?(file(%{}), [{"id", :gt, 10}])
    end

    test "a bound on a string or decimal column never prunes" do
      file = file(%{5 => bound("a", "b"), 6 => bound("1.00", "2.00")})

      assert keep?(file, [{"name", :gt, "z"}])
      assert keep?(file, [{"price", :gt, 5}])
    end

    test "every conjunct must leave a match possible" do
      file =
        file(%{1 => bound("1", "10"), 3 => bound("2024-01-01 00:00:00", "2024-01-02 00:00:00")})

      assert keep?(file, [{"id", :gt, 5}, {"ts", :lt, ~N[2024-01-02 00:00:00]}])
      refute keep?(file, [{"id", :gt, 5}, {"ts", :gt, ~N[2024-01-03 00:00:00]}])
    end
  end
end
