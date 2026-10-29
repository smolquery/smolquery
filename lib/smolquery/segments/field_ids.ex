defmodule Smolquery.Segments.FieldIds do
  @moduledoc """
  What a Parquet file says about its own columns: their names, and the ids
  they were written under (PL-62).

  Every file smolquery writes stamps the catalog's column ids into the file
  (`Smolquery.Schema.parquet_field_ids/1`), and DuckDB's `parquet_schema()`
  reads them back. A reader that does not hold a file's ids from elsewhere —
  the compactor over sealed files, a distributed shard over the files it was
  handed — asks the file: one `parquet_schema([...])` over a batch of files,
  parsed here into a per-file map of the top-level columns and, when every
  top-level column carries one, their ids.

  `parquet_schema` lists a nested type's children as rows of their own, a
  MAP's key and value included; only the top-level columns are the table's
  columns, so the walk skips each subtree by its `num_children`. A file with
  ids on some columns and not others is read as having none: the writer
  stamps all or nothing, so a partial stamp is not a file this system wrote.

  The walk reads a file's rows in the order DuckDB answers them, root first,
  and nothing in the SQL pins that order: both readers run with
  `preserve_insertion_order` off, so the scan stays parallel (T-759).
  DuckDB keeps a file's rows in order within a chunk, and a file of 3,001
  rows split across chunks came back in order every time, but a file whose
  rows do not walk as one tree, using every row and no more, is an error
  rather than a guess at its columns.
  """

  alias Smolquery.Engine.Connection

  @typedoc """
  One file's columns: `ids` maps each top-level column name to its id, or is
  `nil` when the file carries none; `columns` is every top-level column name.
  """
  @type description :: %{ids: %{String.t() => pos_integer()} | nil, columns: [String.t()]}

  @doc """
  The query that describes `count` files, bound as `$1..$count`.
  """
  @spec sql(pos_integer()) :: String.t()
  def sql(count) do
    placeholders = Enum.map_join(1..count, ", ", &"$#{&1}")

    "SELECT file_name, name, num_children, field_id FROM parquet_schema([#{placeholders}])"
  end

  @doc """
  The per-file descriptions of `urls`, read with one `sql/1` on
  `connection`, each read bounded by `timeout`.
  """
  @spec read(GenServer.server(), [String.t()], timeout()) ::
          {:ok, %{String.t() => description()}} | {:error, term()}
  def read(connection, urls, timeout) do
    with {:ok, result} <- Connection.query(connection, sql(length(urls)), urls, timeout),
         do: by_file(result.rows)
  end

  @doc """
  The per-file description from the rows `sql/1` answers, or
  `{:error, {:unreadable_schema, file}}` for the first file whose rows do not
  walk as one schema tree.
  """
  @spec by_file([[term()]]) ::
          {:ok, %{String.t() => description()}} | {:error, {:unreadable_schema, String.t()}}
  def by_file(rows) do
    rows
    |> Enum.group_by(&hd/1, &tl/1)
    |> Enum.reduce_while({:ok, %{}}, fn {file, file_rows}, {:ok, described} ->
      case walk(file_rows) do
        {:ok, columns} -> {:cont, {:ok, Map.put(described, file, describe(columns))}}
        :error -> {:halt, {:error, {:unreadable_schema, file}}}
      end
    end)
  end

  @doc """
  The per-file ids alone of `by_file/1`'s descriptions, as
  `Smolquery.Schema.projection_by_id/2` takes them.
  """
  @spec ids(%{String.t() => description()}) :: %{
          String.t() => %{String.t() => pos_integer()} | nil
        }
  def ids(described), do: Map.new(described, fn {file, %{ids: ids}} -> {file, ids} end)

  defp walk([[_root, count, _id] | rows]) do
    case top_level(rows, count || 0, []) do
      {[], columns} -> {:ok, columns}
      _short_or_left_over -> :error
    end
  end

  defp describe(columns) do
    ids =
      if Enum.all?(columns, fn {_name, id} -> is_integer(id) end), do: Map.new(columns), else: nil

    %{ids: ids, columns: columns |> Enum.map(&elem(&1, 0)) |> Enum.reverse()}
  end

  defp top_level(rows, 0, columns), do: {rows, columns}
  defp top_level([], _remaining, _columns), do: :short

  defp top_level([[name, children, id] | rest], remaining, columns) do
    with rest when is_list(rest) <- skip_subtree(rest, children || 0),
         do: top_level(rest, remaining - 1, [{name, id} | columns])
  end

  defp skip_subtree(rows, 0), do: rows
  defp skip_subtree([], _remaining), do: :short

  defp skip_subtree([[_name, children, _id] | rest], remaining) do
    with rest when is_list(rest) <- skip_subtree(rest, children || 0),
         do: skip_subtree(rest, remaining - 1)
  end
end
