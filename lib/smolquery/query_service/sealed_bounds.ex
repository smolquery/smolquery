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
  text is known to be the exact value: integers, doubles, timestamps and
  dates, which `Smolquery.StatText` reads as the writer's own
  `RETURN_STATS` text. A double column prunes only when the catalog says
  it holds no NaN: a NaN compares outside any bound, and a file the
  catalog has not flagged either way may hold one. A string bound may be
  a prefix of the value and a decimal's text form is not checked, so
  conjuncts on those columns prune nothing here; DuckLake may still prune
  them inside the engine, and then the sealed count is an upper bound, as
  it always was. A file whose bound the text cannot parse is kept:
  nothing proves a match impossible.

  `columns/2` is computed once per table, `keep?/3` once per file.
  """

  alias Smolquery.Catalog
  alias Smolquery.QueryService.Pruner
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.StatText

  @prunable [:int64, :float64, :timestamp, :timestamp_ns, :date]

  @typedoc "A column the pruner can trust the catalog's bounds for: its name, id and type."
  @type column :: {String.t(), pos_integer(), Schema.logical_type()}

  @doc """
  The columns `conjuncts` bound whose catalog bounds the pruner can trust,
  by `schema`'s type for them, with the ids the catalog keys them by.
  """
  @spec columns(Schema.t(), [Pruner.conjunct()]) :: [column()]
  def columns(%Schema{} = schema, conjuncts) do
    for name <- Enum.uniq(for {column, _op, _value} <- conjuncts, do: column),
        {:ok, %Field{id: id, type: type}} <- [Schema.field(schema, name)],
        is_integer(id) and type in @prunable,
        do: {name, id, type}
  end

  @doc """
  Whether `file`'s bounds for `columns` leave a row matching every
  conjunct possible.
  """
  @spec keep?(Catalog.file_column_stats(), [column()], [Pruner.conjunct()]) :: boolean()
  def keep?(%{bounds: bounds}, columns, conjuncts) do
    stats =
      for {name, id, type} <- columns,
          stat = typed(Map.get(bounds, id), type),
          stat != nil,
          into: %{},
          do: {name, stat}

    Pruner.keep_stats?(stats, conjuncts)
  end

  defp typed(%{nan: nan}, :float64) when nan != false, do: nil

  defp typed(%{min: min, max: max}, type) when is_binary(min) and is_binary(max) do
    case {parse(min, type, :floor), parse(max, type, :ceil)} do
      {low, high} when low != nil and high != nil -> %{min: low, max: high}
      _unreadable -> nil
    end
  end

  defp typed(_unbounded, _type), do: nil

  defp parse(text, :int64, _direction) do
    case Integer.parse(text) do
      {value, ""} -> value
      _not_an_integer -> nil
    end
  end

  defp parse(text, :float64, _direction) do
    case Float.parse(text) do
      {value, ""} -> value
      _not_a_double -> nil
    end
  end

  defp parse(text, :date, _direction), do: StatText.date(text)
  defp parse(text, _timestamp, direction), do: StatText.naive_datetime(text, direction)
end
