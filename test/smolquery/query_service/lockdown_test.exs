defmodule Smolquery.QueryService.LockdownTest do
  use ExUnit.Case, async: true

  alias Smolquery.Catalog.DuckLake
  alias Smolquery.QueryService.Lockdown

  describe "statements/2" do
    test "lists first, external access off, DuckLake's options exempted, then the lock" do
      assert Lockdown.statements(["/data", "s3://sealed/"], ["http://hot/a.parquet"]) == [
               "SET allowed_directories = ['/data', 's3://sealed/']",
               "SET allowed_paths = ['http://hot/a.parquet']",
               "SET enable_external_access = false",
               DuckLake.allowed_configs_statement(),
               "SET lock_configuration = true"
             ]
    end

    test "always closes external access: the lists bind nothing while it is open (T-690)" do
      assert "SET enable_external_access = false" in Lockdown.statements([], [])
    end
  end
end
