defmodule Smolquery.QueryService.Statistics do
  @moduledoc """
  What serving a query costs, counted where the plan is made.

  The planner already decides exactly which files a query reads — the hot
  micro-segments that survive the membership rule and the pruner, and the
  sealed segments listed at the pinned snapshot. This struct carries those
  decisions out to the caller as numbers, so pruning can be *counted* rather
  than inferred from wall time (T-267): a regression that silently stops
  pruning shows up as `files_scanned` jumping, not as "a bit slower".

  The two tiers are reported separately because they prune by different
  mechanisms. The hot tier is pruned by the planner — the WHERE pruner
  (`Smolquery.QueryService.Pruner`) and the Top-N bound
  (`Smolquery.QueryService.TopN`) — so `files_total` (entries that passed
  the membership rule) versus `files_scanned` (entries the plan kept)
  measures the two together. The sealed tier is pruned by the planner from
  the catalog's per-file bounds (`Smolquery.QueryService.SealedBounds`,
  T-705) on integer, double, timestamp and date columns: `files_total` is
  the segments listed at the snapshot, `files_scanned` the ones a match is
  possible in. DuckDB may prune further inside the engine, on a string
  bound or a row group, which the planner cannot observe, so the sealed
  count is an upper bound on what is read.

  Rows and bytes sum over the scanned files' manifest and catalog stats. They
  are plan-derived sizes, not engine-measured I/O — `EXPLAIN ANALYZE` is the
  tool for the latter.

  `bytes_scanned` is what the query reads of those files: the bytes of the
  columns it references, from the catalog's per-file column stats (T-704),
  since Parquet is columnar and the engine reads only those chunks. That
  is the number a bill is made of, as BigQuery's bytes processed is.
  `file_bytes` is the files' whole size, the upper bound `bytes_scanned`
  was cut from; the two are equal on a `SELECT *`, on the hot tier (whose
  micro-segments are small and served whole by their buffer pod), and on a
  catalog without per-column sizes.

  `bytes_read` is the one engine-measured number (T-706): what the scan
  fetched over HTTP, summed from the shards' own engines when the query
  scattered and from the job engine when it ran on one, as
  `Smolquery.QueryService.ReadBytes` measures it; `nil` until the query
  has run, and for a deployment that turned the measurement off.

  Both numbers are always reported, so the two accountings can be compared
  on every job; `accounting` says which one the deployment treats as the
  bytes the query is accounted for (`bytes_accounted/1`): `:plan`, the
  default, bills what the catalog says the query reads and is the same
  whatever the cache held; `:measured` bills what was fetched, and falls
  back to the plan on a job that measured nothing.
  """

  @enforce_keys [:hot, :sealed]
  defstruct [:hot, :sealed, bytes_read: nil, accounting: :plan]

  @typedoc """
  One tier's scan counts: files the planner considered, files the plan keeps,
  the kept files' total rows, the bytes the query reads of them, and their
  whole size.
  """
  @type tier :: %{
          files_total: non_neg_integer(),
          files_scanned: non_neg_integer(),
          rows_scanned: non_neg_integer(),
          bytes_scanned: non_neg_integer(),
          file_bytes: non_neg_integer()
        }

  @type t :: %__MODULE__{
          hot: tier(),
          sealed: tier(),
          bytes_read: non_neg_integer() | nil,
          accounting: :plan | :measured
        }

  @doc """
  Statistics from the two tiers' counts.
  """
  @spec new(tier(), tier()) :: t()
  def new(hot, sealed), do: %__MODULE__{hot: hot, sealed: sealed}

  @doc """
  One tier's counts as a `t:tier/0`; `file_bytes` is `bytes_scanned` when
  not given, for a tier read whole.
  """
  @spec tier(
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer() | nil
        ) :: tier()
  def tier(files_total, files_scanned, rows_scanned, bytes_scanned, file_bytes \\ nil) do
    %{
      files_total: files_total,
      files_scanned: files_scanned,
      rows_scanned: rows_scanned,
      bytes_scanned: bytes_scanned,
      file_bytes: file_bytes || bytes_scanned
    }
  end

  @doc """
  Files the planner considered, both tiers.
  """
  @spec files_total(t()) :: non_neg_integer()
  def files_total(%__MODULE__{hot: hot, sealed: sealed}),
    do: hot.files_total + sealed.files_total

  @doc """
  Files the plan reads, both tiers.
  """
  @spec files_scanned(t()) :: non_neg_integer()
  def files_scanned(%__MODULE__{hot: hot, sealed: sealed}),
    do: hot.files_scanned + sealed.files_scanned

  @doc """
  Rows in the files the plan reads, both tiers.
  """
  @spec rows_scanned(t()) :: non_neg_integer()
  def rows_scanned(%__MODULE__{hot: hot, sealed: sealed}),
    do: hot.rows_scanned + sealed.rows_scanned

  @doc """
  Bytes in the files the plan reads, both tiers.
  """
  @spec bytes_scanned(t()) :: non_neg_integer()
  def bytes_scanned(%__MODULE__{hot: hot, sealed: sealed}),
    do: hot.bytes_scanned + sealed.bytes_scanned

  @doc """
  `statistics` with what the query fetched over HTTP once it has run, and
  the accounting the deployment asked for, which stays `:plan` when
  nothing was measured; `nil` stays `nil`.
  """
  @spec read(t() | nil, non_neg_integer() | nil, :plan | :measured) :: t() | nil
  def read(nil, _bytes, _accounting), do: nil

  def read(%__MODULE__{} = statistics, bytes, accounting) do
    %{statistics | bytes_read: bytes, accounting: if(bytes == nil, do: :plan, else: accounting)}
  end

  @doc """
  The bytes the query is accounted for: `bytes_read/1` under `:measured`,
  `bytes_scanned/1` under `:plan`.
  """
  @spec bytes_accounted(t()) :: non_neg_integer()
  def bytes_accounted(%__MODULE__{accounting: :measured, bytes_read: bytes})
      when is_integer(bytes),
      do: bytes

  def bytes_accounted(%__MODULE__{} = statistics), do: bytes_scanned(statistics)

  @doc """
  The whole size of the files the plan reads, both tiers.
  """
  @spec file_bytes(t()) :: non_neg_integer()
  def file_bytes(%__MODULE__{hot: hot, sealed: sealed}), do: hot.file_bytes + sealed.file_bytes

  @doc """
  `file_bytes/1` in MiB, rounded to two decimals.
  """
  @spec mib_in_files(t()) :: float()
  def mib_in_files(%__MODULE__{} = statistics),
    do: Float.round(file_bytes(statistics) / 1_048_576, 2)

  @doc """
  `bytes_scanned/1` in MiB, rounded to two decimals.
  """
  @spec mib_scanned(t()) :: float()
  def mib_scanned(%__MODULE__{} = statistics),
    do: Float.round(bytes_scanned(statistics) / 1_048_576, 2)
end
