defmodule Smolquery.Catalog.DuckLake.SqliteMetadata do
  @moduledoc """
  Puts a SQLite metadata database in write-ahead-log (WAL) mode (T-698).

  Every engine on a node attaches the same SQLite file: the catalog engine,
  each job engine, and the history writer. In SQLite's default `delete`
  journal mode, a reader's shared lock blocks a writer's commit, and DuckDB's
  SQLite connection gives up after a 5 s busy timeout. A job engine holds its
  read for the whole query, so any lake query longer than 5 s failed every
  catalog commit on the node with `database is locked`: seals, compaction,
  and table changes. Measured: a 7.7 s lake query failed `put_clustering/3`
  after 5,023 ms in `delete` mode, and let it commit in 3 ms in WAL mode.

  In WAL mode readers do not block a writer, and a writer does not block
  readers. Two writers still contend. SQLite then fails the later one at
  once with `database is locked` instead of waiting, and
  `Smolquery.Catalog.DuckLake` retries that commit. The mode is stored in the
  database file, so one switch covers every later connection, from any
  engine.

  DuckDB cannot make the switch: its `sqlite_query` fails on every statement
  in this build. So this module opens the file directly with `Exqlite`.

  ## Only before any engine opens the file

  `Exqlite` and DuckDB's SQLite extension each carry their own copy of
  SQLite. POSIX file locks belong to the process, and closing any descriptor
  of a file drops every lock the process holds on it. So when this module
  closes the file, it would drop the locks of a DuckDB engine in the same
  node that has the file open. SQLite's "How To Corrupt An SQLite Database
  File" names exactly this as a way to corrupt a database. `prepare/1`
  therefore runs only where no engine has attached the file yet:
  `Smolquery.Catalog.Migrator`'s boot step, which runs before the cluster
  and every service. A service's own `catalog:` override is not prepared,
  and stays in the mode its file already has.

  WAL keeps two more files beside the database, `<file>-wal` and
  `<file>-shm`. Every process that opens the database must run on the same
  host. A SQLite catalog is a single-node catalog, so that already holds.
  """

  require Logger

  alias Exqlite.Sqlite3

  @busy_timeout_ms 5_000

  @doc """
  Prepares `metadata` when it names a SQLite file: creates its directory and
  switches the file to WAL mode. Any other metadata answers `:ok` untouched.

  A failed switch logs a warning and answers `:ok`: the catalog still works
  in the old mode, only with the lock contention described above. Call it
  only before any engine in this node opens the file; see the moduledoc.
  """
  @spec prepare(String.t() | nil) :: :ok | {:error, File.posix()}
  def prepare("sqlite:" <> path) do
    with :ok <- File.mkdir_p(Path.dirname(path)) do
      case write_ahead_log(path) do
        :ok ->
          :ok

        {:error, reason} ->
          Logger.warning(
            "catalog metadata #{path} stays out of WAL mode, so a lake query longer " <>
              "than 5 s can fail catalog commits: #{inspect(reason)}"
          )
      end
    end
  end

  def prepare(_metadata), do: :ok

  @doc """
  Switches the SQLite database at `path` to WAL mode.

  It creates the file when it does not exist yet. DuckLake then creates its
  tables in it on attach. The switch needs a moment with no other open
  transaction on the file, so it waits up to #{@busy_timeout_ms} ms for one.
  A file already in WAL mode answers at once.
  """
  @spec write_ahead_log(Path.t()) :: :ok | {:error, term()}
  def write_ahead_log(path) do
    with {:ok, db} <- Sqlite3.open(path) do
      try do
        switch(db)
      after
        Sqlite3.close(db)
      end
    end
  end

  @doc """
  The journal mode of the SQLite database at `path`, such as `"wal"` or
  `"delete"`.
  """
  @spec journal_mode(Path.t()) :: {:ok, String.t()} | {:error, term()}
  def journal_mode(path) do
    with {:ok, db} <- Sqlite3.open(path, mode: [:readonly]) do
      try do
        pragma(db, "PRAGMA journal_mode")
      after
        Sqlite3.close(db)
      end
    end
  end

  defp switch(db) do
    with :ok <- Sqlite3.set_busy_timeout(db, @busy_timeout_ms),
         {:ok, mode} <- pragma(db, "PRAGMA journal_mode=WAL") do
      if mode == "wal", do: :ok, else: {:error, {:journal_mode, mode}}
    end
  end

  defp pragma(db, sql) do
    with {:ok, statement} <- Sqlite3.prepare(db, sql) do
      try do
        case Sqlite3.fetch_all(db, statement) do
          {:ok, [[mode]]} -> {:ok, mode}
          {:ok, rows} -> {:error, {:unexpected_rows, rows}}
          {:error, reason} -> {:error, reason}
        end
      after
        Sqlite3.release(db, statement)
      end
    end
  end
end
