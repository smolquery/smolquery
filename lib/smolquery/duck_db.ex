defmodule Smolquery.DuckDB do
  @moduledoc """
  The supervised DuckDB database process used by every database in smolquery.

  Every instance is opened here, which is where the one process-wide DuckDB
  option is applied (T-452): `allocator_background_threads` is a jemalloc
  flag for the whole OS process, and libduckdb re-applies each instance's own
  configured value on open — and forces it off on every instance close. So
  every open goes out with the node's configured value, and
  `Smolquery.Allocator` re-asserts it on a schedule for the gap a close
  leaves. See `Smolquery.Engine` for why the flag is on.

  ## The remote pushdown optimizer is off (T-738)

  DuckDB 2.0 runs a `remote_pushdown` optimizer whenever a catalog that can
  run queries remotely is attached. A Postgres database is one: a job
  engine's DuckLake keeps its metadata there, and a federated connection
  may be one too. The optimizer breaks the binding of a macro called with
  a column and an expression argument, so a ClickHouse client's
  `toStartOfInterval(Timestamp, INTERVAL 1 MINUTE)` answers `Binder Error:
  Macro ... does not support the supplied arguments`
  (duckdb/duckdb-postgres#630). DuckDB 1.5 had no such optimizer.

  So every database opens with it in `disabled_optimizers`. Drop it once a
  DuckDB release carries the fix, duckdb/duckdb#27793.
  """

  @version Application.compile_env!(:smolquery, :duckdb_driver_version)

  @spec version() :: String.t()
  def version, do: @version

  @doc """
  Whether the node runs DuckDB's allocator with jemalloc's background thread
  (`config :smolquery, Smolquery.Engine, allocator_background_threads:`,
  default on).
  """
  @spec allocator_background_threads?() :: boolean()
  def allocator_background_threads? do
    :smolquery
    |> Application.get_env(Smolquery.Engine, [])
    |> Keyword.get(:allocator_background_threads, true)
  end

  defp database_options do
    [
      driver: :duckdb,
      version: @version,
      allocator_background_threads: to_string(allocator_background_threads?()),
      disabled_optimizers: "remote_pushdown"
    ]
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    Adbc.Database.start_link(
      Keyword.merge(database_options(), Keyword.drop(opts, [:driver, :version]))
    )
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end
end
