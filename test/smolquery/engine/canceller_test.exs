defmodule Smolquery.Engine.CancellerTest do
  use ExUnit.Case, async: false

  alias Smolquery.DuckDB
  alias Smolquery.Engine.Canceller
  alias Smolquery.Engine.Connection

  @slow "SELECT max(a.range * b.range) FROM range(100000) a, range(100000) b"

  setup do
    database = start_supervised!({DuckDB, process_options: [name: __MODULE__.Database]})
    {:ok, connection} = Connection.start_link(database: database)
    {:ok, canceller} = Canceller.start(connection)

    %{connection: connection, canceller: canceller}
  end

  test "cancel/1 stops a running query, then the canceller is gone", %{
    connection: connection,
    canceller: canceller
  } do
    running = Task.async(fn -> Connection.frame(connection, @slow, [], :infinity) end)
    Process.sleep(200)

    assert Canceller.cancel(canceller) == :ok
    assert {:error, %Adbc.Error{}} = Task.await(running, 5_000)
    refute Process.alive?(canceller)
    assert Canceller.cancel(canceller) == :ok
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

    Process.sleep(200)
    Process.exit(owner, :kill)

    assert {:error, %Adbc.Error{}} = Task.await(running, 5_000)
  end
end
