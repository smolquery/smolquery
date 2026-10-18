defmodule Smolquery.Catalog.DuckLake.SqliteMetadataTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Exqlite.Sqlite3
  alias Smolquery.Catalog.DuckLake.SqliteMetadata

  @moduletag :tmp_dir

  describe "prepare/1" do
    test "creates the directory and a SQLite file in WAL mode", %{tmp_dir: tmp} do
      path = Path.join([tmp, "nested", "catalog.sqlite"])

      assert SqliteMetadata.prepare("sqlite:" <> path) == :ok
      assert SqliteMetadata.journal_mode(path) == {:ok, "wal"}
    end

    test "leaves other metadata alone" do
      assert SqliteMetadata.prepare("postgres:dbname=lake") == :ok
      assert SqliteMetadata.prepare(nil) == :ok
    end

    test "warns and answers :ok when the switch fails", %{tmp_dir: tmp} do
      path = Path.join(tmp, "not-a-database.sqlite")
      File.write!(path, String.duplicate("x", 4096))

      log = capture_log(fn -> assert SqliteMetadata.prepare("sqlite:" <> path) == :ok end)

      assert log =~ "stays out of WAL mode"
    end
  end

  describe "write_ahead_log/1" do
    test "switches an existing delete-mode database and keeps its rows", %{tmp_dir: tmp} do
      path = Path.join(tmp, "existing.sqlite")
      {:ok, db} = Sqlite3.open(path)
      :ok = Sqlite3.execute(db, "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (7)")
      :ok = Sqlite3.close(db)

      assert SqliteMetadata.journal_mode(path) == {:ok, "delete"}
      assert SqliteMetadata.write_ahead_log(path) == :ok
      assert SqliteMetadata.journal_mode(path) == {:ok, "wal"}

      {:ok, db} = Sqlite3.open(path, mode: [:readonly])
      {:ok, statement} = Sqlite3.prepare(db, "SELECT a FROM t")
      assert Sqlite3.fetch_all(db, statement) == {:ok, [[7]]}
      :ok = Sqlite3.close(db)
    end

    test "answers :ok again for a database already in WAL mode", %{tmp_dir: tmp} do
      path = Path.join(tmp, "twice.sqlite")

      assert SqliteMetadata.write_ahead_log(path) == :ok
      assert SqliteMetadata.write_ahead_log(path) == :ok
    end
  end

  describe "journal_mode/1" do
    test "fails for a file that does not exist", %{tmp_dir: tmp} do
      assert {:error, _reason} = SqliteMetadata.journal_mode(Path.join(tmp, "absent.sqlite"))
    end
  end
end
