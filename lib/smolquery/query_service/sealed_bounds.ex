defmodule Smolquery.QueryService.SealedBounds do
  @moduledoc """
  Prunes sealed files by the bounds the catalog keeps for them (T-705).

  A table sorted by time or by id answers a bounded query from a fraction
  of its files. DuckLake prunes them itself when it plans the query, but
  the planner could not see that: the sealed tier counted every live file
  as scanned, and the scatter dealt every file to a shard. The catalog
  keeps the same per-file minimum and maximum DuckLake prunes by
  (`Smolquery.Catalog.file_column_stats/4`), so the planner can prune the
  same way it prunes hot entries, with `Smolquery.QueryService.Pruner`.

  The catalog prints a bound as text. A bound is trusted only where the
  text is known to be the exact value: integers, doubles (not a column
  that holds a NaN, whose order is not the text's), timestamps and dates,
  which `Smolquery.StatText` reads as the writer's own `RETURN_STATS`
  text. A string bound may be a prefix of the value and a decimal's text
  form is not checked, so conjuncts on those columns prune nothing here;
  DuckLake may still prune them inside the engine, and then the sealed
  count is an upper bound, as it always was. A file whose bound the text
  cannot parse is kept: nothing proves a match impossible.
  """

  alias Smolquery.Catalog
  alias Smolquery.QueryService.Pruner
  alias Smolquery.Schema
  alias Smolquery.Schema.Field

  @prunable [:int64, :float64, :timestamp, :timestamp_ns, :date]

  @doc """
  The columns `conjuncts` bound whose catalog bounds the pruner can trust,
  by `schema`'s type for them.
  """
  @spec prunable(Schema.t(), [Pruner.conjunct()]) :: [String.t()]
  def prunable(%Schema{} = schema, conjuncts) do
    for {column, _op, _value} <- conjuncts,
        {:ok, %Field{type: type}} <- [Schema.field(schema, column)],
        type in @prunable,
        uniq: true,
        do: column
  end

  @doc """
  Whether `file`'s bounds leave a row matching every conjunct possible.
  `ids` is the schema's `Smolquery.Schema.field_ids/1`, which keys the
  file's columns.
  """
  @spec keep?(
          Catalog.file_column_stats(),
          Schema.t(),
          [Pruner.conjunct()],
          %{String.t() => pos_integer()}
        ) :: boolean()
  def keep?(%{bounds: bounds}, %Schema{fields: fields}, conjuncts, ids) do
    stats =
      for %Field{name: name, id: id, type: type} <- fields,
          type in @prunable,
          bound = typed(Map.get(bounds, id), type),
          bound != nil,
          into: %{},
          do: {name, bound}

    Pruner.keep?(%{"stats" => stats, "field_ids" => ids}, conjuncts, ids)
  end

  defp typed(%{nan: true}, :float64), do: nil

  defp typed(%{min: min, max: max, null_count: nulls}, type)
       when is_binary(min) and is_binary(max) do
    with {:ok, low} <- parse(min, type),
         {:ok, high} <- parse(max, type) do
      %{"min" => low, "max" => high, "null_count" => nulls}
    else
      :error -> nil
    end
  end

  defp typed(_unbounded, _type), do: nil

  defp parse(text, :int64) do
    case Integer.parse(text) do
      {value, ""} -> {:ok, value}
      _not_an_integer -> :error
    end
  end

  defp parse(text, :float64) do
    case Float.parse(text) do
      {value, ""} -> {:ok, value}
      _not_a_double -> :error
    end
  end

  defp parse(text, :date), do: {:ok, %{"type" => "date", "value" => text}}
  defp parse(text, _timestamp), do: {:ok, %{"type" => "naive_datetime", "value" => text}}
end
