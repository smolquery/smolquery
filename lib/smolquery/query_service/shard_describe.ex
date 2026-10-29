defmodule Smolquery.QueryService.ShardDescribe do
  @moduledoc """
  Reads what a shard's Parquet files say about their columns
  (`Smolquery.Segments.FieldIds`) in one `parquet_schema` on the job
  engine's own connection, with `preserve_insertion_order` off (T-759).

  A shard reads the field ids of every sealed file whose catalog entry
  carries none before it scans (`Smolquery.QueryService.PartialWorker`). That
  is one footer read per file, each an S3 round trip. DuckDB 2.0 reads a
  `parquet_schema` list one file at a time while `preserve_insertion_order`
  is on (duckdb/duckdb#24273): a shard of 577 files took 30 s to describe
  where it took 1.5 s. With it off the read runs on every thread of the
  engine, as it did on 1.5. 120 files at 25 ms a request took 6.3 s with it
  on, and 0.8 s with it off at 8 threads, 0.22 s at 32. The fan-out it
  replaces, `Smolquery.Engine.ParallelDescribe` (T-719), read on at most 4
  to 6 connections and took 1.6 s at any thread count. Cutting the list as
  well as turning the setting off bought at most 15%, and cost a helper
  connection and a dirty I/O scheduler a lane.

  The setting belongs to the database, not the connection, and the engine
  runs the job's own SQL next, whose unordered results it would reorder. So
  the describe reads the setting, turns it off only if it was on, and puts
  it back before it answers, whether the read succeeded, failed or exited.
  A shard locks its engine down after the describe, so none of the job's
  SQL runs in between. A setting that cannot be put back is the describe's
  error when the read succeeded, never a scan with it off; when the read
  failed, its own error is the answer. A call that exits answers a
  `Smolquery.Engine.CallExited`.
  """

  alias Smolquery.Engine.CallExited
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result
  alias Smolquery.Segments.FieldIds

  @type engine :: %{required(:connection) => GenServer.server(), optional(atom()) => term()}

  @doc """
  The per-file descriptions of `urls`, read through `engine`'s connection, as
  `FieldIds.read/3` answers them.
  """
  @spec files(engine(), [String.t()]) ::
          {:ok, %{String.t() => FieldIds.description()}} | {:error, term()}
  def files(_engine, []), do: {:ok, %{}}

  def files(%{connection: connection}, urls),
    do: CallExited.catching(fn -> described(connection, urls) end)

  defp described(connection, urls) do
    with {:ok, preserved} <- insertion_order(connection),
         do:
           unordered(connection, preserved, fn -> FieldIds.read(connection, urls, :infinity) end)
  end

  defp insertion_order(connection) do
    sql = "SELECT current_setting('preserve_insertion_order')"

    case Connection.query(connection, sql, [], :infinity) do
      {:ok, %Result{rows: [[preserved]]}} when is_boolean(preserved) ->
        {:ok, preserved}

      {:ok, %Result{rows: rows}} ->
        {:error, {:unexpected_setting, :preserve_insertion_order, rows}}

      {:error, _reason} = error ->
        error
    end
  end

  defp unordered(_connection, false, read), do: read.()

  defp unordered(connection, true, read) do
    with :ok <- insertion_order(connection, false) do
      described = CallExited.catching(read)

      restored(described, CallExited.catching(fn -> insertion_order(connection, true) end))
    end
  end

  defp restored({:ok, _described} = described, :ok), do: described
  defp restored({:ok, _described}, {:error, _reason} = error), do: error
  defp restored({:error, _reason} = error, _restore), do: error

  defp insertion_order(connection, preserved) do
    sql = "SET preserve_insertion_order = #{preserved}"

    case Connection.query(connection, sql, [], :infinity) do
      {:ok, _result} -> :ok
      {:error, _reason} = error -> error
    end
  end
end
