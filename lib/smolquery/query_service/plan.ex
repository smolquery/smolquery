defmodule Smolquery.QueryService.Plan do
  @moduledoc """
  An executable query plan: everything a job engine needs, resolved once.

  A plan is a value the planner produces and a runner consumes. `statements`
  are run in order on the job's engine — schema and view definitions that make
  each referenced `<dataset>.<table>` resolve to the union of its sealed tier
  (pinned at `snapshot`) and its surviving hot micro-segments — and then `sql`,
  the user's query, runs unmodified against them.

  `hot` carries the micro-segment entries that survived the membership rule
  and the pruner, keyed by table: their row counts and stats are what query
  statistics and pruning decisions are made of. `hot_members` is the ids of
  the entries that survived the membership rule alone, before pruning —
  the exact hot tier this plan read, keyed by table. A caller that wants a
  later plan to read the same hot tier passes it back as `hot_ids:`
  (PL-58 layer 8, T-418); it is the pruner's input, not its output, so a
  later query with a different `WHERE` still prunes from the whole set. A
  table the plan read by page — the preview's, whose unordered `LIMIT`
  fetched only the newest entries (T-449) — is absent: a page is not the
  tier, and a later query pinned to it would read short.
  `statistics` is those
  decisions counted (`Smolquery.QueryService.Statistics`) — what the plan
  reads, per tier, reported out with the finished job; `nil` when the
  catalog could not answer the sizes, because statistics are reporting and
  must not fail a query that would have run. `sealed_uncached_bytes` is the
  size of the live sealed files the node's file cache has never read
  (`Smolquery.QueryService.FileCache`, T-629); the runner weighs it to
  decide whether the job reads through the cache. It is `0` without a
  cache, for a job that forced the cache on or off, and for a scan too
  small to matter. `canonical_sql` is the
  statement's text as DuckDB's parser re-emits it — no comments, no
  trailing semicolon — which is what makes the runner's result-budget wrap
  safe to parenthesize.

  `non_null` says, per result column in order, whether it can never be
  `NULL` (`Smolquery.QueryService.Nullability`, T-510), or `:unknown`.

  `schemas` carries each referenced table's catalog schema, keyed by table:
  what the runner reads to decide whether a result can carry a `VARIANT`
  column that needs casting before it crosses Arrow
  (`Smolquery.QueryService.VariantResults`).

  `params` are the query's bind parameters, positional for its `$n`
  placeholders (T-410). They ride the plan so every consumer that prepares
  the statement — the runner's frame, a `DESCRIBE`, a scatter partial's
  `COPY` — binds the same values, and so the pruner and the Top-N bound
  can read them where the SQL says `$n`. They never enter the job's stored
  SQL; an engine error message can still quote a bound value.

  `federated` says whether `statements` opens with `ATTACH`es for registered
  connections (T-324). The runner reads `federated_extensions`, the DuckDB
  extensions those attaches need (`postgres`, and `ducklake` for a DuckLake
  connection, T-610), to load them before it locks the engine down —
  loading an extension is not free, and on a SQLite-metadata deployment
  nothing else would pull `postgres` in.
  `sealed_skipped` is the sealed files the planner pruned by their catalog
  bounds (`Smolquery.QueryService.SealedBounds`, T-705), per table, as the
  absolute paths the catalog lists them by. `Smolquery.QueryService.Scatter`
  deals every live file but these; the single-engine path needs nothing
  from it, since DuckLake prunes the same files itself. `sealed_scan_bytes`
  is, per table and kept file, what the query reads of it by the plan: the
  referenced columns' bytes, or the whole file under a star (T-706), with
  a `default` for a file the planner read no per-file stats for, the whole
  file or nothing for a query that references no column, so a shard's
  files add up to the sealed `bytes_scanned` either way. The scatter
  reports it per shard; it still deals and sizes shards by whole files.
  """

  alias Smolquery.BufferService.HotClient
  alias Smolquery.Catalog
  alias Smolquery.QueryService.Statistics

  @enforce_keys [:sql, :snapshot]
  defstruct [
    :sql,
    :canonical_sql,
    :snapshot,
    :statistics,
    sealed_uncached_bytes: 0,
    sealed_skipped: %{},
    sealed_scan_bytes: %{},
    tables: [],
    statements: [],
    hot: %{},
    hot_members: %{},
    schemas: %{},
    non_null: :unknown,
    federated: false,
    federated_extensions: [],
    params: []
  ]

  @type t :: %__MODULE__{
          sql: String.t(),
          canonical_sql: String.t() | nil,
          snapshot: Catalog.snapshot(),
          tables: [Catalog.table_ref()],
          statements: [String.t()],
          hot: %{Catalog.table_ref() => [HotClient.entry()]},
          hot_members: %{Catalog.table_ref() => [String.t()]},
          schemas: %{Catalog.table_ref() => Smolquery.Schema.t()},
          non_null: [boolean()] | :unknown,
          statistics: Statistics.t() | nil,
          sealed_uncached_bytes: non_neg_integer(),
          sealed_skipped: %{Catalog.table_ref() => MapSet.t(String.t())},
          sealed_scan_bytes: %{
            Catalog.table_ref() => %{
              files: %{String.t() => non_neg_integer()},
              default: :file | 0
            }
          },
          federated: boolean(),
          federated_extensions: [atom()],
          params: [term()]
        }
end
