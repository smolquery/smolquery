defmodule Smolquery.QueryService.ShardDescribe do
  @moduledoc """
  Reads what a shard's Parquet files say about their columns
  (`Smolquery.Segments.FieldIds`), several files at a time (T-719), through
  `Smolquery.Engine.ParallelDescribe`.

  A shard reads the field ids of every sealed file whose catalog entry
  carries none before it scans (`Smolquery.QueryService.PartialWorker`). That
  is one footer read per file, each an S3 round trip, which DuckDB 2.0 makes
  one at a time within a statement: a shard of 577 files took 30 s to
  describe where it took 1.5 s. The 4 shards a node runs at once by default
  (`local_workers`) hold 24 of a release's 32 dirty I/O schedulers at
  `ParallelDescribe.max_chunks/1`'s 6. The cap is per call, not per node: a
  node that also compacts (`Smolquery.StorageService.Merge`) holds up to 6
  more for each compaction describing at once.

  What is the job engine's own is which statements a helper connection
  runs: the `SET`s of the engine's `bootstrap`
  (`Smolquery.QueryService.JobEngine.start/1`) and the job's own, and
  `ca_cert_file` when the engine reads through `httpfs`. Helpers run before
  the engine locks down.
  """

  alias Smolquery.Engine.ParallelDescribe
  alias Smolquery.Segments.FieldIds

  @type engine :: %{connection: pid(), database: pid(), bootstrap: keyword()}

  @doc """
  The per-file descriptions of `urls`, read through `engine`, each helper
  connection running `statements` after the engine's bootstrap `SET`s, as
  `FieldIds.by_file/1` answers them.
  """
  @spec files(engine(), [String.t()], [String.t()]) ::
          {:ok, %{String.t() => FieldIds.description()}} | {:error, term()}
  def files(engine, urls, statements) do
    ParallelDescribe.files(engine, urls,
      extensions: Keyword.get(engine.bootstrap, :extensions, []),
      statements: Keyword.get(engine.bootstrap, :statements, []) ++ statements
    )
  end
end
