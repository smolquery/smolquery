defmodule Smolquery.Engine.CancellerTest do
  use ExUnit.Case, async: false

  alias Smolquery.DuckDB
  alias Smolquery.Engine.Canceller
  alias Smolquery.Engine.Connection
  alias Smolquery.Identifier
  alias Smolquery.Test.Eventually

  @slow "SELECT max(a.range * b.range) FROM range(100000) a, range(100000) b"

  setup do
    database = start_supervised!({DuckDB, process_options: [name: __MODULE__.Database]})
    {:ok, connection} = Connection.start_link(database: database)
    {:ok, canceller} = Canceller.start(connection)

    %{connection: connection, canceller: canceller}
  end

  defp cancelled_while_running(connection, canceller, run) do
    adbc = Connection.adbc_connection(connection)
    running = Task.async(run)

    assert Eventually.until(fn -> in_driver?(connection) or in_driver?(adbc) end, 200, 10)
    assert Canceller.cancel(canceller) == :ok

    Task.await(running, 5_000)
  end

  defp in_driver?(pid) do
    match?({:current_function, {Adbc.Nif, _fun, _arity}}, Process.info(pid, :current_function))
  end

  test "cancel/1 stops a running query, then the canceller is gone", %{
    connection: connection,
    canceller: canceller
  } do
    assert {:error, %Adbc.Error{message: "INTERRUPT" <> _}} =
             cancelled_while_running(connection, canceller, fn ->
               Connection.frame(connection, @slow, [], :infinity)
             end)

    refute Process.alive?(canceller)
    assert Canceller.cancel(canceller) == :ok
  end

  test "cancel/1 stops a SELECT run through query/4 (T-726)", %{
    connection: connection,
    canceller: canceller
  } do
    assert {:error, %Adbc.Error{message: "INTERRUPT" <> _}} =
             cancelled_while_running(connection, canceller, fn ->
               Connection.query(connection, @slow, [], :infinity)
             end)
  end

  @tag :tmp_dir
  test "cancel/1 stops a bound COPY run through query/4, as a shard's scan runs (T-726)", %{
    connection: connection,
    canceller: canceller,
    tmp_dir: tmp_dir
  } do
    target = Identifier.sql_string(Path.join(tmp_dir, "partial.parquet"))

    copy =
      "COPY (SELECT max(a.range * b.range) AS n FROM range($1) a, range($1) b) " <>
        "TO #{target} (FORMAT parquet)"

    assert {:error, %Adbc.Error{message: "INTERRUPT" <> _}} =
             cancelled_while_running(connection, canceller, fn ->
               Connection.query(connection, copy, [100_000], :infinity)
             end)
  end

  test "a query/4 SELECT is cancelled after the same statement was cancelled as a frame on another engine (T-726)",
       %{connection: connection, canceller: canceller} do
    other_database = start_supervised!({DuckDB, []}, id: :other_database)
    other = start_supervised!({Connection, database: other_database}, id: :other_connection)
    {:ok, other_canceller} = Canceller.start(other)

    assert {:error, %Adbc.Error{message: "INTERRUPT" <> _}} =
             cancelled_while_running(other, other_canceller, fn ->
               Connection.frame(other, @slow, [], :infinity)
             end)

    assert {:error, %Adbc.Error{message: "INTERRUPT" <> _}} =
             cancelled_while_running(connection, canceller, fn ->
               Connection.query(connection, @slow, [], :infinity)
             end)
  end

  test "cancel/1 on an idle connection leaves it usable, and the canceller spent", %{
    connection: connection,
    canceller: canceller
  } do
    assert Canceller.cancel(canceller) == :ok
    refute Process.alive?(canceller)
    assert {:ok, _frame} = Connection.frame(connection, "SELECT 1", [], :infinity)
  end

  test "the canceller cancels and exits when its connection dies", %{
    connection: connection,
    canceller: canceller
  } do
    monitor = Process.monitor(canceller)
    Process.unlink(connection)
    Process.exit(connection, :kill)

    assert_receive {:DOWN, ^monitor, :process, ^canceller, _reason}, 5_000
  end

  test "a process linked to the canceller cancels the running read when it dies (T-719)", %{
    connection: connection,
    canceller: canceller
  } do
    running = Task.async(fn -> Connection.frame(connection, @slow, [], :infinity) end)

    owner =
      spawn(fn ->
        Process.link(canceller)
        receive(do: (:never -> :ok))
      end)

    assert Eventually.until(fn -> in_driver?(connection) end, 200, 10)
    Process.exit(owner, :kill)

    assert {:error, %Adbc.Error{}} = Task.await(running, 5_000)
  end
end
