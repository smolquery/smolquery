defmodule Smolquery.QueryService.ReadBytes do
  @moduledoc """
  What a scan fetched over HTTP, measured on the engine that ran it (T-706).

  `bytesScanned` is plan-derived: the referenced columns of the files the
  planner keeps. The engine reads less than that when it skips a row
  group by its Parquet statistics, and a little more for footers and
  dictionary pages, and the plan cannot see either. DuckDB's own log can:
  with `HTTP` logging on, every request `httpfs` makes lands in the
  in-memory `duckdb_logs` table with its method, range, status and the
  response's `content-length`. A shard runs on a private engine, so the
  log is the shard's alone; `enable/1` turns it on before the scan and
  `measure/1` sums it after. The job engine does the same around a
  single-engine query.

  The number is the bytes the object store sent: a local-disk store issues
  no HTTP and measures zero, and a file-cache hit on a pod fetches nothing
  and counts nothing, which is the honest meaning of what a scan pulled
  over the network. Logging costs nothing the probe could see: a scan with
  it on ran at the speed of one without, and reading two hundred rows back
  took 2 ms; a shard issues tens.
  """

  alias Smolquery.Engine.Connection

  @typedoc "Bytes the responses carried and requests that got one."
  @type measured :: %{bytes: non_neg_integer(), requests: non_neg_integer()}

  @timeout_ms 5_000
  @request ~r/'type': (\w+)/
  @status ~r/'status': \w*?(\d{3})/
  @length ~r/content-length=(\d+)/i

  @doc """
  Turns `HTTP` logging on for `connection`'s engine, in memory.
  """
  @spec enable(GenServer.server()) :: :ok | {:error, term()}
  def enable(connection) do
    statement = "CALL enable_logging(['HTTP'], level := 'trace', storage := 'memory')"

    with {:ok, _enabled} <- Connection.query(connection, statement, [], @timeout_ms), do: :ok
  end

  @doc """
  What the engine's `HTTP` log holds so far.
  """
  @spec measure(GenServer.server()) :: {:ok, measured()} | {:error, term()}
  def measure(connection) do
    statement = "SELECT message FROM duckdb_logs WHERE type = 'HTTP'"

    with {:ok, %{rows: rows}} <- Connection.query(connection, statement, [], @timeout_ms),
         do: {:ok, sum(Enum.map(rows, &hd/1))}
  end

  @doc """
  The bytes and requests in a list of `HTTP` log messages: a request
  counts when it got a response, and its bytes count when it was a `GET`
  answered with a 2xx status, from the response's `content-length`. A
  `HEAD` moves no body and a failed or retried request without a response
  moves nothing.
  """
  @spec sum([String.t()]) :: measured()
  def sum(messages) do
    Enum.reduce(messages, %{bytes: 0, requests: 0}, fn message,
                                                       %{bytes: bytes, requests: requests} ->
      case response(message) do
        nil -> %{bytes: bytes, requests: requests}
        status -> %{bytes: bytes + body(message, status), requests: requests + 1}
      end
    end)
  end

  defp response(message) do
    case Regex.run(@status, message) do
      [_match, status] -> String.to_integer(status)
      nil -> nil
    end
  end

  defp body(message, status) when status in 200..299 do
    with [_match, "GET"] <- Regex.run(@request, message),
         [_match, length] <- Regex.run(@length, message) do
      String.to_integer(length)
    else
      _head_or_unsized -> 0
    end
  end

  defp body(_message, _failed), do: 0
end
