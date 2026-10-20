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
  log is the shard's alone; `enabled?/1` turns it on before the scan and
  `measured/1` sums it after. The job engine does the same around a
  single-engine query.

  The sum is computed inside DuckDB and two integers come back: a log row
  carries the request's headers, the S3 signature and session token among
  them, and they never leave the engine. A request counts when it got a
  response; its bytes count when it was a `GET` answered with a 2xx
  status, from the response's `content-length`. A `HEAD` moves no body
  and a failed or retried request without a response moves nothing.

  The number is the bytes the object store sent: a local-disk store issues
  no HTTP and measures zero, and a file-cache hit on a pod fetches nothing
  and counts nothing, which is the honest meaning of what a scan pulled
  over the network. Logging costs nothing the probe could see: a scan with
  it on ran at the speed of one without, and the two statements here add
  about 2 ms to a shard.
  """

  alias Smolquery.Engine.Connection

  @typedoc "Bytes the responses carried and requests that got one."
  @type measured :: %{bytes: non_neg_integer(), requests: non_neg_integer()}

  @timeout_ms 5_000

  @enable "CALL enable_logging(['HTTP'], level := 'trace', storage := 'memory')"

  @measure "SELECT CAST(count(*) AS BIGINT), CAST(COALESCE(SUM(" <>
             "CASE WHEN message LIKE '%''type'': GET%' " <>
             "AND regexp_matches(message, '''status'': \\w*?2\\d\\d') " <>
             "THEN TRY_CAST(regexp_extract(message, '(?i)content-length=(\\d+)', 1) AS BIGINT) " <>
             "END), 0) AS BIGINT) " <>
             "FROM duckdb_logs WHERE type = 'HTTP' AND message LIKE '%''response'': {%'"

  @doc """
  Turns `HTTP` logging on for `connection`'s engine, in memory, and says
  whether it is on.
  """
  @spec enabled?(GenServer.server()) :: boolean()
  def enabled?(connection),
    do: match?({:ok, _enabled}, Connection.query(connection, @enable, [], @timeout_ms))

  @doc """
  What the engine's `HTTP` log holds so far, or `nil` when it cannot be
  read.
  """
  @spec measured(GenServer.server()) :: measured() | nil
  def measured(connection) do
    case Connection.query(connection, @measure, [], @timeout_ms) do
      {:ok, %{rows: [[requests, bytes]]}} when is_integer(requests) and is_integer(bytes) ->
        %{bytes: bytes, requests: requests}

      _unreadable ->
        nil
    end
  end
end
