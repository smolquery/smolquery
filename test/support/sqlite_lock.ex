defmodule Smolquery.Test.SqliteLock do
  @moduledoc """
  Holds a lock on a SQLite file from another OS process, for T-698's tests.

  POSIX file locks belong to the process, so a lock taken in the test's own
  BEAM never blocks a DuckDB engine in the same BEAM. `hold/3` starts a
  `:peer` node and takes the lock there.
  """

  alias Exqlite.Sqlite3

  @doc """
  Starts a peer node and, in it, opens `path` and runs `sql`, such as
  `"BEGIN IMMEDIATE"` or `"BEGIN; SELECT ..."`. It answers once the lock is
  held. The peer commits and closes the file after `hold_ms`. The peer is
  linked to the caller, so it stops when the caller exits.
  """
  @spec hold(Path.t(), String.t(), non_neg_integer()) :: pid()
  def hold(path, sql, hold_ms) do
    {:ok, peer, _node} = :peer.start_link(%{connection: :standard_io})
    :ok = :peer.call(peer, :code, :add_paths, [:code.get_path()])
    :ok = :peer.call(peer, __MODULE__, :take, [path, sql, hold_ms])
    peer
  end

  @doc false
  @spec take(Path.t(), String.t(), non_neg_integer()) :: :ok
  def take(path, sql, hold_ms) do
    caller = self()

    spawn(fn ->
      {:ok, db} = Sqlite3.open(path)
      :ok = Sqlite3.execute(db, sql)
      send(caller, :held)
      Process.sleep(hold_ms)
      :ok = Sqlite3.execute(db, "COMMIT")
      Sqlite3.close(db)
    end)

    receive do
      :held -> :ok
    after
      10_000 -> exit(:not_held)
    end
  end
end
