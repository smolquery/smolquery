defmodule Smolquery.Engine.Canceller do
  @moduledoc """
  Holds a connection's cancel handle in a process of its own, and cancels
  what the connection runs through it, when asked or when the connection
  dies (T-724).

  A query runs inside a NIF on a dirty scheduler, which a kill of the
  connection's processes does not interrupt, and the connection process is
  busy running it, so a cancel has to come from elsewhere: the driver's
  cancel handle, taken while the connection is idle (T-708).

  The handle is a NIF resource that keeps the driver's connection, and so the
  whole DuckDB instance with its threads, alive for as long as any process
  heap refers to it. Kept in a map that is copied around, every copy held the
  instance until its process happened to collect garbage, and query pods
  piled up thousands of idle DuckDB threads. So one process holds it and
  nothing else ever sees it: callers hold the canceller's pid.

  A canceller cancels once, and that is the end of it: `cancel/1` is the first
  half of stopping an engine (`Smolquery.QueryService.JobEngine.stop/1`), and
  the canceller exits after it. It is also linked to the connection and traps
  its exit, so it cancels whatever the connection was running when the
  connection dies, whoever killed it, and exits. Either way its exit drops the
  handle, so the instance goes when the connection's processes do.

  Neither `start/1` nor `cancel/1` waits on a canceller for longer than 5
  seconds: a stop must not hang its caller, the engine pool among them.
  """

  alias Smolquery.Engine.Connection

  @timeout_ms 5_000

  @doc """
  Starts the canceller of `connection`, a running `Smolquery.Engine.Connection`,
  once it holds the handle. A connection that cannot give its handle, or that
  is gone, is an error, and the connection is left as it was.
  """
  @spec start(pid()) :: {:ok, pid()} | {:error, term()}
  def start(connection) do
    owner = self()
    adbc = Connection.adbc_connection(connection)
    {canceller, monitor} = spawn_monitor(fn -> hold(owner, connection, adbc) end)

    receive do
      {^canceller, :holding} ->
        Process.demonitor(monitor, [:flush])
        {:ok, canceller}

      {:DOWN, ^monitor, :process, ^canceller, reason} ->
        {:error, {:canceller_failed, reason}}
    after
      @timeout_ms ->
        Process.demonitor(monitor, [:flush])
        Process.exit(canceller, :kill)
        {:error, :canceller_timeout}
    end
  end

  @doc """
  Cancels what the connection is running and waits until it has, after which
  the canceller is gone. A canceller already gone has nothing to cancel.
  """
  @spec cancel(pid()) :: :ok
  def cancel(canceller) do
    monitor = Process.monitor(canceller)
    send(canceller, {:cancel, self(), monitor})

    receive do
      {^monitor, :cancelled} -> Process.demonitor(monitor, [:flush])
      {:DOWN, ^monitor, :process, ^canceller, _gone} -> :ok
    after
      @timeout_ms -> Process.demonitor(monitor, [:flush])
    end

    :ok
  end

  defp hold(owner, connection, adbc) do
    handle = Adbc.Connection.cancel_handle(adbc)
    Process.flag(:trap_exit, true)
    Process.link(connection)
    send(owner, {self(), :holding})

    receive do
      {:cancel, from, ref} ->
        Adbc.Connection.cancel(handle)
        send(from, {ref, :cancelled})

      {:EXIT, ^connection, _reason} ->
        Adbc.Connection.cancel(handle)
    end
  end
end
