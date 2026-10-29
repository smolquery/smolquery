defmodule Smolquery.Engine.ParallelDescribe do
  @moduledoc """
  Reads what a batch of Parquet files says about its columns
  (`Smolquery.Segments.FieldIds`), several files at a time (T-719, T-723).

  DuckDB 1.5 read a `parquet_schema([...])` list on every thread. DuckDB 2.0
  reads it one file at a time while `preserve_insertion_order` is on
  (duckdb/duckdb#24273), and so does a `UNION ALL` of several lists: one
  footer read per file, each an S3 round trip, so 577 files took 30 s to
  describe where they took 1.5 s. A shard turns the setting off for its
  describe instead, and reads on its own connection
  (`Smolquery.QueryService.ShardDescribe`, T-759).

  So the files are cut into as many chunks as the engine has threads, at
  most `max_chunks/1`, and every chunk is read by its own `parquet_schema` at
  once: the first on the engine's own connection, each other on a helper
  connection to the engine's database. Each read waits inside a NIF on one of
  the node's dirty I/O schedulers, which every `adbc` call shares: the BEAM
  starts 10, and a release starts 32 (`rel/env.sh.eex`, T-723). So the cap
  is a fifth of them, at least 4 and at most 8.

  A caller that bounds how many files one read may take (`:max_chunk_files`)
  gets each chunk read in turn, on the same connection, in reads of at most
  that many, so the bound never adds a connection (`lanes/4`).

  A helper is a `Smolquery.Engine.Connection` on the engine's database. It
  loads no extension and applies no setting, because both belong to the
  database and the engine set them already; a `SET` of an extension's option,
  though, holds for the connection that ran it. So a helper runs the
  caller's `:statements`, after `ca_cert_file` when the engine reads through
  `httpfs`. It starts on the engine's spill directory and leaves the spill
  limit alone, so it changes nothing of the database's.

  The chunks run under a supervisor of the call's own, not linked to the
  caller, so a connection that dies or a read that times out answers an
  error rather than taking the caller with it, and every one is awaited, the
  first error kept. A call to the engine's own connection that exits, its
  settings read or its chunk, answers a `Smolquery.Engine.CallExited`, as
  `Smolquery.Engine.try_query/4` does, so a caller that recycles a wedged
  engine on one still can. A helper that cannot start answers
  `{:describe_helper_failed, reason}`, never the `Adbc.Error` a file that
  cannot be read answers. A read stopped halfway left `httpfs` a pooled HTTP
  connection with an unread answer, which the next request on it waited 30 s
  for, so a failure costs what the slowest chunk does, as a success would.
  If the caller dies instead, the call's supervisor stops the helpers, and
  each helper's task is linked to its connection's
  `Smolquery.Engine.Canceller`, which cancels the read. A helper is killed
  once it has answered, its ADBC connection with it.
  """

  alias Smolquery.Engine.CaBundle
  alias Smolquery.Engine.CallExited
  alias Smolquery.Engine.Canceller
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result
  alias Smolquery.Identifier
  alias Smolquery.Segments.FieldIds

  @typedoc "An engine's own connection and the database its helpers connect to."
  @type engine :: %{
          required(:connection) => GenServer.server(),
          required(:database) => GenServer.server(),
          optional(atom()) => term()
        }

  @typedoc """
  `:statements` run on every helper, after `ca_cert_file` when the engine's
  `:extensions` include `httpfs`; `:timeout` bounds each chunk's read (`:infinity` by default), and
  `:max_chunk_files` cuts a chunk of more files than that into several, read
  in turn at the same concurrency, so no one read outgrows its timeout.
  """
  @type option ::
          {:statements, [String.t()]}
          | {:extensions, [atom() | String.t() | {atom(), String.t()}]}
          | {:timeout, timeout()}
          | {:max_chunk_files, pos_integer()}

  @doc """
  The per-file descriptions of `urls`, as `FieldIds.read/3` answers them.
  """
  @spec files(engine(), [String.t()], [option()]) ::
          {:ok, %{String.t() => FieldIds.description()}} | {:error, term()}
  def files(_engine, [], _opts), do: {:ok, %{}}

  def files(engine, urls, opts) do
    timeout = Keyword.get(opts, :timeout, :infinity)

    with {:ok, threads, temp_directory} <- settings(engine.connection, timeout) do
      helper = helper_options(engine, temp_directory, opts)
      cap = max_chunks(:erlang.system_info(:dirty_io_schedulers))
      lanes = lanes(urls, threads, cap, Keyword.get(opts, :max_chunk_files))
      {:ok, supervisor} = Task.Supervisor.start_link()

      try do
        supervisor
        |> Task.Supervisor.async_stream_nolink(
          Enum.with_index(lanes),
          fn
            {chunks, 0} -> on_engine(engine.connection, chunks, timeout)
            {chunks, _helper} -> on_helper(helper, chunks, timeout)
          end,
          max_concurrency: max(length(lanes), 1),
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
  `urls` as lanes, one per connection that reads at once: `chunks/3`'s
  chunks, each cut into reads of at most `max_chunk_files` files (`nil` for
  no bound), which its lane reads in turn. So the lanes never outnumber
  `min(threads, cap)`, however many reads the bound makes.
  """
  @spec lanes([String.t()], pos_integer(), pos_integer(), pos_integer() | nil) ::
          [[[String.t()]]]
  def lanes(urls, threads, cap, nil), do: urls |> chunks(threads, cap) |> Enum.map(&[&1])

  def lanes(urls, threads, cap, max_chunk_files),
    do: urls |> chunks(threads, cap) |> Enum.map(&Enum.chunk_every(&1, max_chunk_files))

  @doc """
  How many chunks a describe reads at once at most on a node with
  `dirty_io_schedulers`: a fifth of them, at least 4 and at most 8.
  """
  @spec max_chunks(pos_integer()) :: pos_integer()
  def max_chunks(dirty_io_schedulers), do: dirty_io_schedulers |> div(5) |> max(4) |> min(8)

  @doc """
  `urls` cut into at most `min(threads, cap)` chunks of near-equal size, in
  order. No files is no chunks.
  """
  @spec chunks([String.t()], pos_integer(), pos_integer()) :: [[String.t()]]
  def chunks([], _threads, _cap), do: []

  def chunks(urls, threads, cap) do
    count = threads |> min(cap) |> max(1)
    Enum.chunk_every(urls, ceil(length(urls) / count))
  end

  defp settings(connection, timeout) do
    sql = "SELECT current_setting('threads'), current_setting('temp_directory')"

    CallExited.catching(fn ->
      with {:ok, %Result{rows: [[threads, directory]]}} <-
             Connection.query(connection, sql, [], timeout),
           do: {:ok, threads, directory}
    end)
  end

  defp on_engine(connection, chunks, timeout),
    do: CallExited.catching(fn -> read(connection, chunks, timeout) end)

  defp helper_options(engine, temp_directory, opts) do
    [
      database: engine.database,
      temp_directory: temp_directory,
      max_temp_directory_size: nil,
      statements:
        ca_cert_file(httpfs?(Keyword.get(opts, :extensions, []))) ++
          Keyword.get(opts, :statements, [])
    ]
  end

  defp httpfs?(extensions), do: Enum.any?(extensions, &(extension_name(&1) == "httpfs"))

  defp extension_name({name, _repository}), do: to_string(name)
  defp extension_name(name), do: to_string(name)

  defp ca_cert_file(httpfs?) do
    case CaBundle.path() do
      bundle when httpfs? and is_binary(bundle) ->
        ["SET ca_cert_file = #{Identifier.sql_string(bundle)}"]

      _none ->
        []
    end
  end

  defp on_helper(options, chunks, timeout) do
    case start_helper(options) do
      {:ok, connection} ->
        try do
          with {:ok, canceller} <- Canceller.start(connection) do
            Process.link(canceller)
            read(connection, chunks, timeout)
          end
        after
          Process.unlink(connection)
          Process.exit(connection, :kill)
        end

      {:error, reason} ->
        {:error, {:describe_helper_failed, reason}}
    end
  end

  defp read(connection, chunks, timeout) do
    Enum.reduce_while(chunks, {:ok, %{}}, fn chunk, {:ok, described} ->
      case FieldIds.read(connection, chunk, timeout) do
        {:ok, more} -> {:cont, {:ok, Map.merge(described, more)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
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

  defp merged({:exit, reason}, result),
    do: merged({:ok, {:error, {:describe_helper_exited, reason}}}, result)

  defp merged({:ok, {:ok, more}}, {:ok, described}), do: {:ok, Map.merge(described, more)}
  defp merged({:ok, {:error, _reason} = error}, {:ok, _described}), do: error
  defp merged(_answer, {:error, _reason} = error), do: error
end
