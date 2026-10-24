defmodule Smolquery.DuckDBTest do
  use ExUnit.Case, async: false

  alias Smolquery.DuckDB
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result
  alias Smolquery.Identifier
  alias Smolquery.QueryService.ClickHouseFunctions
  alias Smolquery.Test.Postgres

  describe "version/0" do
    test "returns the configured DuckDB driver version" do
      assert DuckDB.version() == Application.fetch_env!(:smolquery, :duckdb_driver_version)
    end
  end

  describe "ADBC configuration" do
    test "pins the packaged driver to an official asset of the same version" do
      assert [{:duckdb, [version: version, url: url]}] = Application.fetch_env!(:adbc, :drivers)

      assert version == DuckDB.version()
      assert url =~ "duckdb/releases/download/v#{DuckDB.version()}/duckdb-shared-libs-"
    end
  end

  test "starts a database with the configured driver" do
    database =
      start_supervised!({
        DuckDB,
        version: "1.5.1", process_options: [name: __MODULE__.Database]
      })

    {:ok, connection} = Connection.start_link(database: database)

    assert {:ok, result} = Connection.query(connection, "SELECT version()")
    assert Result.one!(result) == "v" <> DuckDB.version()
  end

  describe "the remote pushdown optimizer (T-738)" do
    setup do
      database = start_supervised!({DuckDB, process_options: [name: __MODULE__.Database]})
      %{connection: start_supervised!({Connection, database: database})}
    end

    test "is disabled on every database", %{connection: connection} do
      assert {:ok, result} =
               Connection.query(connection, "SELECT current_setting('disabled_optimizers')")

      assert Result.one!(result) == "remote_pushdown"
    end

    @tag :integration
    test "a macro given a column and an expression binds with Postgres attached",
         %{connection: connection} do
      postgres = Postgres.ensure_database!()

      attach =
        "ATTACH #{Identifier.sql_string(libpq(postgres))} AS t738_pg (TYPE postgres, READ_ONLY)"

      sql =
        "SELECT toStartOfInterval(ts, INTERVAL 1 MINUTE) AS bucket, " <>
          "generate_subscripts([1, 2], 1) AS i " <>
          "FROM (SELECT TIMESTAMP '2026-10-24 14:17:49' AS ts)"

      for statement <-
            [
              "INSTALL postgres",
              "LOAD postgres",
              attach | ClickHouseFunctions.statements_for(sql)
            ] do
        assert {:ok, _result} = Connection.query(connection, statement)
      end

      assert {:ok, result} = Connection.query(connection, sql)

      assert result.rows == [
               [~N[2026-10-24 14:17:00.000000], 1],
               [~N[2026-10-24 14:17:00.000000], 2]
             ]
    end
  end

  defp libpq(options) do
    "host=#{options[:hostname]} port=#{options[:port]} user=#{options[:username]} " <>
      "password=#{options[:password]} dbname=#{options[:database]}"
  end
end
