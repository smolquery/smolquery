defmodule Smolquery.Engine.IpcSchemaTest do
  @moduledoc """
  `fields/1` on IPC streams DuckDB writes, with and without rows, and on
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

  defp names(conn, sql) do
    with {:ok, fields} <- IpcSchema.fields(ipc(conn, sql)),
         do: {:ok, Enum.map(fields, &elem(&1, 0))}
  end

  test "reads the top-level names in order", %{conn: conn} do
    sql = "SELECT 1 AS n, 'a' AS \"two words\", {'x': 1} AS s, [1] AS l, NULL AS \"ünï\""

    assert names(conn, sql) == {:ok, ["n", "two words", "s", "l", "ünï"]}
  end

  test "keeps a repeated name, with rows and without", %{conn: conn} do
    assert names(conn, "SELECT 1 AS a, 2 AS a") == {:ok, ["a", "a"]}
    assert names(conn, "SELECT 1 AS a, 2 AS a WHERE false") == {:ok, ["a", "a"]}
  end

  test "reads a wide schema", %{conn: conn} do
    columns = Enum.map_join(1..300, ", ", &"#{&1} AS c#{&1}")

    assert {:ok, names} = names(conn, "SELECT " <> columns)
    assert names == Enum.map(1..300, &"c#{&1}")
  end

  test "answers :error for bytes that are not a schema message", %{conn: conn} do
    stream = ipc(conn, "SELECT 1 AS n")

    assert IpcSchema.fields(binary_part(stream, 0, 12)) == :error
    assert IpcSchema.fields(binary_part(stream, 4, byte_size(stream) - 4)) == :error
    assert IpcSchema.fields(<<0xFFFFFFFF::32, 8::little-32, 0::64>>) == :error
  end

  test "marks a field that is an interval apart from one that only holds one (T-728, T-737)", %{
    conn: conn
  } do
    sql =
      "SELECT INTERVAL 3 DAY AS i, 1 AS n, [INTERVAL 1 HOUR] AS l, {'d': INTERVAL 1 DAY} AS s, " <>
        "TIMESTAMP '2026-10-24 01:00:00' - TIMESTAMP '2026-10-24 00:00:00' AS diff, 'x' AS t, " <>
        "[{'x': 1}] AS plain"

    assert IpcSchema.fields(ipc(conn, sql)) ==
             {:ok,
              [
                {"i", :interval},
                {"n", nil},
                {"l", :nested},
                {"s", :nested},
                {"diff", :interval},
                {"t", nil},
                {"plain", nil}
              ]}
  end
end
