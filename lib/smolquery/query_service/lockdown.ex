defmodule Smolquery.QueryService.Lockdown do
  @moduledoc """
  The statements that confine an engine about to run SQL derived from a
  user's statement — a job engine (`Smolquery.QueryService.Runner`) or a partial
  worker (`Smolquery.QueryService.PartialWorker`) — in the one order that
  works.

  The lists come first: `allowed_directories` and `allowed_paths` say what
  the engine may still reach once external access closes. External access
  then closes. DuckDB reads the two lists as the exceptions to a closed
  engine and nothing else: with access open they bind nothing, which is why
  there is no option to keep it open (T-690). A `COPY ... TO` into an allowed
  directory still works with access closed, so a partial worker writes its
  partial under the same lockdown a job engine runs under. Then
  `allowed_configs` names the options DuckLake sets on its metadata
  connection at every transaction
  (`Smolquery.Catalog.DuckLake.allowed_configs_statement/0`), and last
  `lock_configuration` seals everything, including that list. The order is
  the whole point: a `SET` after the lock is refused whatever its value, so a
  lockdown that locked before exempting DuckLake's options would fail every
  read of the lake with "Current transaction is aborted". DuckDB's own spill
  files are not subject to the lists; the engine spills with access closed
  wherever its `temp_directory` points.
  """

  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Identifier

  @doc """
  The lockdown statements for an engine allowed `directories` and `paths`.
  """
  @spec statements([String.t()], [String.t()]) :: [String.t()]
  def statements(directories, paths) do
    [
      "SET allowed_directories = #{Identifier.sql_list(directories)}",
      "SET allowed_paths = #{Identifier.sql_list(paths)}",
      "SET enable_external_access = false",
      DuckLake.allowed_configs_statement(),
      "SET lock_configuration = true"
    ]
  end
end
