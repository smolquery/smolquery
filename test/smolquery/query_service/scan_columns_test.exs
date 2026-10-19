defmodule Smolquery.QueryService.ScanColumnsTest do
  use ExUnit.Case, async: true

  alias Smolquery.Engine
  alias Smolquery.Identifier
  alias Smolquery.QueryService.ScanColumns
  alias Smolquery.Schema

  @engine __MODULE__.Engine
  @events {"analytics", "events"}
  @users {"analytics", "users"}

  setup_all do
    start_supervised!({Engine, name: @engine})
    :ok
  end

  defp schemas do
    %{
      @events =>
        Schema.new!([{"id", :int64}, {"name", :string}, {"amount", :float64}, {"ts", :timestamp}]),
      @users => Schema.new!([{"id", :int64}, {"email", :string}, {"payload", :variant}])
    }
  end

  defp referenced(sql) do
    {:ok, %{rows: [[json]]}} =
      Engine.query(@engine, "SELECT json_serialize_sql(#{Identifier.sql_string(sql)})")

    ScanColumns.referenced(JSON.decode!(json), schemas())
  end

  test "names the columns a statement reads, wherever it reads them" do
    assert %{@events => ["id", "name", "amount"], @users => ["id"]} =
             referenced(
               "SELECT name, sum(amount) AS a FROM analytics.events " <>
                 "WHERE id > 3 GROUP BY name HAVING count(*) > 1 ORDER BY a"
             )
  end

  test "a bare count(*) reads no column" do
    assert %{@events => [], @users => []} = referenced("SELECT count(*) FROM analytics.events")
  end

  test "a star anywhere reads every column of every table" do
    assert %{@events => :all, @users => :all} = referenced("SELECT * FROM analytics.events")

    assert %{@events => :all} =
             referenced("SELECT count(*) FROM (SELECT e.* FROM analytics.events e) t")
  end

  test "a qualified reference counts by its last name, and an alias of an output does not count" do
    assert %{@events => ["ts"]} =
             referenced(
               "SELECT date_trunc('day', e.ts) AS day FROM analytics.events e ORDER BY day"
             )
  end

  test "a name shared by two tables counts for both" do
    assert %{@events => ["id"], @users => ["id", "email"]} =
             referenced(
               "SELECT u.email FROM analytics.events e JOIN analytics.users u ON u.id = e.id"
             )
  end

  test "the columns a JOIN USING names count, and a NATURAL JOIN reads everything" do
    assert %{@events => ["id"], @users => ["id"]} =
             referenced(
               "SELECT count(*) FROM analytics.events e JOIN analytics.users u USING (id)"
             )

    assert %{@events => :all, @users => :all} =
             referenced("SELECT count(*) FROM analytics.events NATURAL JOIN analytics.users")
  end

  test "a field read out of a struct column counts the column" do
    assert %{@users => ["payload"]} =
             referenced("SELECT payload.user_id FROM analytics.users WHERE payload.kind = 'x'")
  end

  test "a CTE's own names do not count, its reads do" do
    assert %{@events => ["amount"]} =
             referenced(
               "WITH t AS (SELECT amount AS money FROM analytics.events) SELECT money FROM t"
             )
  end
end
