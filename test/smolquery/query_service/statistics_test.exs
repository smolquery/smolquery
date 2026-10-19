defmodule Smolquery.QueryService.StatisticsTest do
  use ExUnit.Case, async: true

  alias Smolquery.QueryService.Statistics

  describe "totals" do
    test "sum the two tiers" do
      statistics =
        Statistics.new(
          Statistics.tier(4, 2, 100, 1_000),
          Statistics.tier(3, 3, 900, 2_048)
        )

      assert Statistics.files_total(statistics) == 7
      assert Statistics.files_scanned(statistics) == 5
      assert Statistics.rows_scanned(statistics) == 1_000
      assert Statistics.bytes_scanned(statistics) == 3_048
      assert Statistics.file_bytes(statistics) == 3_048
    end

    test "an empty plan weighs nothing" do
      statistics = Statistics.new(Statistics.tier(0, 0, 0, 0), Statistics.tier(0, 0, 0, 0))

      assert Statistics.files_scanned(statistics) == 0
      assert Statistics.bytes_scanned(statistics) == 0
      assert Statistics.mib_scanned(statistics) == 0.0
    end
  end

  describe "file_bytes/1 (T-704)" do
    test "is the whole files where bytes_scanned is the columns read" do
      statistics =
        Statistics.new(
          Statistics.tier(1, 1, 10, 100),
          Statistics.tier(2, 2, 900, 2_048, 1_048_576 + 2_048)
        )

      assert Statistics.bytes_scanned(statistics) == 2_148
      assert Statistics.file_bytes(statistics) == 1_050_724
      assert Statistics.mib_in_files(statistics) == 1.0
      assert statistics.hot.file_bytes == 100
    end
  end

  describe "mib_scanned/1" do
    test "converts bytes to MiB, rounded to two decimals" do
      statistics =
        Statistics.new(
          Statistics.tier(1, 1, 1, 1_048_576),
          Statistics.tier(1, 1, 1, 524_288)
        )

      assert Statistics.mib_scanned(statistics) == 1.5
    end
  end
end
