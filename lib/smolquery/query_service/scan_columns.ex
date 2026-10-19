defmodule Smolquery.QueryService.ScanColumns do
  @moduledoc """
  The columns a statement reads from each table it names (T-704).

  Parquet is columnar and DuckDB reads only the column chunks a query
  references, so what a query scans is the referenced columns' bytes, not
  the files' size: `SELECT count(*)` over a 92 GB table reads footers,
  `SELECT UserID` reads one column of a hundred. That is also what
  BigQuery bills as bytes processed. `referenced/2` walks the DuckDB AST
  the planner already holds and names, per table, the columns it finds.

  The walk is by name, not by binding, and errs toward counting: every
  part of a `COLUMN_REF` counts for every table whose schema has a column
  of that name, so `e.ts` counts `ts` and `payload.user_id` counts the
  `payload` column it reads; the names a `JOIN ... USING` lists count; a
  `STAR` anywhere (a bare `*`, `t.*`, `COLUMNS(*)`) or a `NATURAL JOIN`
  means every column of every table. A `count(*)` is `count_star` in the
  AST, not a star, so a bare count references nothing. The answer
  over-counts a join whose tables share a column name, an alias that is
  also a column's name, and a `SELECT *` under an `EXCLUDE`; it never
  under-counts, which is the side a bill should err on.
  """

  alias Smolquery.Catalog
  alias Smolquery.Schema

  @typedoc "Every column, or the named ones."
  @type referenced :: :all | [String.t()]

  @doc """
  The columns `statement` references in each of `schemas`' tables.
  """
  @spec referenced(map(), %{Catalog.table_ref() => Schema.t()}) ::
          %{Catalog.table_ref() => referenced()}
  def referenced(statement, schemas) do
    case collect(statement, {MapSet.new(), false}) do
      {_names, true} ->
        Map.new(schemas, fn {ref, _schema} -> {ref, :all} end)

      {names, false} ->
        Map.new(schemas, fn {ref, %Schema{fields: fields}} ->
          {ref, for(%{name: name} <- fields, MapSet.member?(names, name), do: name)}
        end)
    end
  end

  defp collect(%{"class" => "STAR"}, {names, _star}), do: {names, true}
  defp collect(%{"ref_type" => "NATURAL"}, {names, _star}), do: {names, true}

  defp collect(%{"class" => "COLUMN_REF", "column_names" => [_ | _] = parts}, acc),
    do: named(parts, acc)

  defp collect(%{"using_columns" => [_ | _] = using} = node, acc),
    do: node |> Map.delete("using_columns") |> collect(named(using, acc))

  defp collect(%{} = node, acc), do: node |> Map.values() |> Enum.reduce(acc, &collect/2)
  defp collect(list, acc) when is_list(list), do: Enum.reduce(list, acc, &collect/2)
  defp collect(_scalar, acc), do: acc

  defp named(parts, {names, star}),
    do: {Enum.reduce(parts, names, fn name, set -> MapSet.put(set, name) end), star}
end
