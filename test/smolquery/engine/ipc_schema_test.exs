defmodule Smolquery.Engine.IpcSchemaTest do
  @moduledoc """
  `field_names/1` on IPC streams DuckDB writes, with and without rows, and on
  bytes that are not one.
  """

  use ExUnit.Case, async: true

  alias Smolquery.Engine.IpcSchema

  doctest IpcSchema

  setup_all do
    {:ok, database} = Smolquery.DuckDB.start_link()
    {:ok, conn} = Adbc.Connection.start_link(database: database)

    %{conn: conn}
  end

  defp ipc(conn, sql) do
    {:ok, ipc} = Adbc.Connection.query_pointer(conn, sql, [], &Adbc.StreamResult.to_ipc_stream/1)
    ipc
  end

  test "reads the top-level names in order", %{conn: conn} do
    sql = "SELECT 1 AS n, 'a' AS \"two words\", {'x': 1} AS s, [1] AS l, NULL AS \"ünï\""

    assert IpcSchema.field_names(ipc(conn, sql)) ==
             {:ok, ["n", "two words", "s", "l", "ünï"]}
  end

  test "keeps a repeated name, with rows and without", %{conn: conn} do
    assert IpcSchema.field_names(ipc(conn, "SELECT 1 AS a, 2 AS a")) == {:ok, ["a", "a"]}

    assert IpcSchema.field_names(ipc(conn, "SELECT 1 AS a, 2 AS a WHERE false")) ==
             {:ok, ["a", "a"]}
  end

  test "reads a wide schema", %{conn: conn} do
    columns = Enum.map_join(1..300, ", ", &"#{&1} AS c#{&1}")

    assert {:ok, names} = IpcSchema.field_names(ipc(conn, "SELECT " <> columns))
    assert names == Enum.map(1..300, &"c#{&1}")
  end

  test "answers :error for bytes that are not a schema message", %{conn: conn} do
    stream = ipc(conn, "SELECT 1 AS n")

    assert IpcSchema.field_names(binary_part(stream, 0, 12)) == :error
    assert IpcSchema.field_names(binary_part(stream, 4, byte_size(stream) - 4)) == :error
    assert IpcSchema.field_names(<<0xFFFFFFFF::32, 8::little-32, 0::64>>) == :error
  end
end
