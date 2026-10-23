defmodule Smolquery.QueryService.ShardDescribe do
  @moduledoc """
  Reads what a batch of Parquet files says about its columns
  (`Smolquery.Segments.FieldIds`), several files at a time (T-719).

  A shard reads the field ids of every sealed file whose catalog entry
  carries none before it scans (`Smolquery.QueryService.PartialWorker`). That
  is one footer read per file, each an S3 round trip. DuckDB 1.5 read a
  `parquet_schema([...])` list on every thread. DuckDB 2.0 reads it one file
  at a time, and so does a `UNION ALL` of several lists: a shard of 577 files
  took 30 s to describe where it took 1.5 s.

  So the files are cut into as many chunks as the engine has threads, at
  most 4, and every chunk is read by its own `parquet_schema` at once: the
  first on the engine's own connection, each other on a helper connection to
  the engine's database. Each read waits inside a NIF on one of the node's 10
  dirty I/O schedulers, which every `adbc` call shares, so 4 keeps two shards
  describing at once inside them.

  A helper is a `Smolquery.Engine.Connection` on the engine's database. It
  loads no extension and applies no setting, because both belong to the
  database and the engine set them already; a `SET` of an extension's option,
  though, holds for the connection that ran it. So a helper runs the `SET`s of
  the engine's `bootstrap` (`Smolquery.QueryService.JobEngine.start/1`) and
  the job's own, and `ca_cert_file` when the engine reads through `httpfs`. It
  starts on the engine's spill directory and leaves the spill limit alone, so
  it changes nothing of the database's. Helpers run before the engine locks
  down.

  The chunks run under a supervisor of the call's own, not linked to the
  caller, so a helper that dies answers an error rather than taking the
  shard's worker with it, and every one is awaited, the first error kept. A
  read stopped halfway left `httpfs` a pooled HTTP connection with an unread
  answer, which the next request on it waited 30 s for, so a failure costs
  what the slowest chunk does, as a success would. If the worker dies
  instead, the call's supervisor stops the helpers, and each helper's task is
  linked to its connection's `Smolquery.Engine.Canceller`, which cancels the
  read. A helper is killed once it has answered, its ADBC connection with it.
  """

  alias Smolquery.Engine.CaBundle
  alias Smolquery.Engine.Canceller
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result
  alias Smolquery.Identifier
  alias Smolquery.Segments.FieldIds

  @max_chunks 4

  @type engine :: %{connection: pid(), database: pid(), bootstrap: keyword()}

  @doc """
  The per-file descriptions of `urls`, read through `engine`, each helper
  connection running `statements` after the engine's bootstrap `SET`s, as
  `FieldIds.by_file/1` answers them.
  """
  @spec files(engine(), [String.t()], [String.t()]) ::
          {:ok, %{String.t() => FieldIds.description()}} | {:error, term()}
  def files(_engine, [], _statements), do: {:ok, %{}}

  def files(engine, urls, statements) do
    with {:ok, threads, temp_directory} <- settings(engine.connection) do
      options = helper_options(engine, temp_directory, statements)
      {:ok, supervisor} = Task.Supervisor.start_link()

      try do
        supervisor
        |> Task.Supervisor.async_stream_nolink(
          urls |> chunks(threads) |> Enum.with_index(),
          fn
            {chunk, 0} -> describe(engine.connection, chunk)
            {chunk, _helper} -> on_helper(options, chunk)
          end,
          max_concurrency: @max_chunks,
          ordered: false,
          timeout: :infinity
        )
        |> Enum.reduce({:ok, %{}}, &merged/2)
      after
        Supervisor.stop(supervisor)
      end
    end
  end

  @doc """
  `urls` cut into at most `min(threads, #{@max_chunks})` chunks of near-equal
  size, in order. No files is no chunks.
  """
  @spec chunks([String.t()], pos_integer()) :: [[String.t()]]
  def chunks([], _threads), do: []

  def chunks(urls, threads) do
    count = threads |> min(@max_chunks) |> max(1)
    Enum.chunk_every(urls, ceil(length(urls) / count))
  end

  defp settings(connection) do
    sql = "SELECT current_setting('threads'), current_setting('temp_directory')"

    with {:ok, %Result{rows: [[threads, directory]]}} <- Connection.query(connection, sql),
         do: {:ok, threads, directory}
  end

  defp helper_options(engine, temp_directory, statements) do
    [
      database: engine.database,
      temp_directory: temp_directory,
      max_temp_directory_size: nil,
      statements:
        ca_cert_file(engine.bootstrap) ++
          Keyword.get(engine.bootstrap, :statements, []) ++ statements
    ]
  end

  defp ca_cert_file(bootstrap) do
    httpfs? =
      bootstrap
      |> Keyword.get(:extensions, [])
      |> Enum.any?(&(extension_name(&1) == "httpfs"))

    case CaBundle.path() do
      bundle when httpfs? and is_binary(bundle) ->
        ["SET ca_cert_file = #{Identifier.sql_string(bundle)}"]

      _none ->
        []
    end
  end

  defp extension_name({name, _repository}), do: to_string(name)
  defp extension_name(name), do: to_string(name)

  defp on_helper(options, urls) do
    with {:ok, connection} <- start_helper(options) do
      try do
        with {:ok, canceller} <- Canceller.start(connection) do
          Process.link(canceller)
          describe(connection, urls)
        end
      after
        Process.unlink(connection)
        Process.exit(connection, :kill)
      end
    end
  end

  defp start_helper(options) do
    Process.flag(:trap_exit, true)
    started = Connection.start_link(options)
    Process.flag(:trap_exit, false)

    case started do
      {:ok, connection} ->
        {:ok, connection}

      {:error, reason} ->
        receive do
          {:EXIT, _connection, ^reason} -> :ok
        after
          0 -> :ok
        end

        {:error, reason}
    end
  end

  defp describe(connection, urls) do
    with {:ok, result} <-
           Connection.query(connection, FieldIds.sql(length(urls)), urls, :infinity),
         do: {:ok, FieldIds.by_file(result.rows)}
  end

  defp merged({:exit, reason}, result),
    do: merged({:ok, {:error, {:describe_helper_exited, reason}}}, result)

  defp merged({:ok, {:ok, more}}, {:ok, described}), do: {:ok, Map.merge(described, more)}
  defp merged({:ok, {:error, _reason} = error}, {:ok, _described}), do: error
  defp merged(_answer, {:error, _reason} = error), do: error
end
