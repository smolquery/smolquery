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
  on, and 0.8 s with it off at 8 threads, 0.22 s at 32. That is as fast as
  cutting the list across helper connections, as
  `Smolquery.Engine.ParallelDescribe` does (T-719), without the helpers or
  the dirty I/O scheduler each one held.

  The setting belongs to the database, not the connection, and the engine
  runs the job's own SQL next, whose unordered results it would reorder. So
  the describe reads the setting, turns it off only if it was on, and puts
  it back before it answers, whether the read succeeded or not. A shard
  locks its engine down after the describe, so none of the job's SQL runs
  in between. A setting that cannot be put back is the describe's error,
  never a scan with it off. A call that exits answers a
  `Smolquery.Engine.CallExited`.
  """

  alias Smolquery.Engine.CallExited
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result
  alias Smolquery.Segments.FieldIds

  @type engine :: %{required(:connection) => GenServer.server(), optional(atom()) => term()}

  @doc """
  The per-file descriptions of `urls`, read through `engine`'s connection, as
  `FieldIds.by_file/1` answers them.
  """
  @spec files(engine(), [String.t()]) ::
          {:ok, %{String.t() => FieldIds.description()}} | {:error, term()}
  def files(_engine, []), do: {:ok, %{}}

  def files(%{connection: connection}, urls),
    do: catching_exit(fn -> described(connection, urls) end)

  defp described(connection, urls) do
    with {:ok, preserved} <- insertion_order(connection),
         do: unordered(connection, preserved, fn -> describe(connection, urls) end)
  end

  defp insertion_order(connection) do
    sql = "SELECT current_setting('preserve_insertion_order')"

    with {:ok, %Result{rows: [[preserved]]}} <- Connection.query(connection, sql, [], :infinity),
         do: {:ok, preserved}
  end

  defp unordered(_connection, false, read), do: read.()

  defp unordered(connection, true, read) do
    with :ok <- insertion_order(connection, false) do
      described = read.()

      with :ok <- insertion_order(connection, true), do: described
    end
  end

  defp insertion_order(connection, preserved) do
    sql = "SET preserve_insertion_order = #{preserved}"

    case Connection.query(connection, sql, [], :infinity) do
      {:ok, _result} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp describe(connection, urls) do
    with {:ok, result} <-
           Connection.query(connection, FieldIds.sql(length(urls)), urls, :infinity),
         do: {:ok, FieldIds.by_file(result.rows)}
  end

  defp catching_exit(call) do
    call.()
  catch
    :exit, reason -> {:error, CallExited.new(reason)}
  end
end
