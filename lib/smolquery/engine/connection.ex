defmodule Smolquery.Engine.Connection do
  @moduledoc """
  A bootstrapped DuckDB connection.

  Wraps an `Adbc.Connection` so that a connection is never observable before its
  extensions and session settings are applied: `init/1` starts the ADBC
  connection linked to this process and runs the bootstrap statements before
  returning. A connection that fails to bootstrap fails to start.

  Because the ADBC connection is linked, its death takes this wrapper with it
  and the supervisor rebuilds both — a restarted connection is a bootstrapped
  connection.

  ## Fatal errors take the database down

  DuckDB answers a fatal or internal error by invalidating the whole database:
  every later query on it fails with "database has been invalidated ... must be
  restarted", forever. Returning that as an ordinary `{:error, _}` would leave
  the engine serving a corpse, so `query/4` replies with the error and then
  kills the `Adbc.Database` it is connected to. The engine subtree is
  `rest_for_one` with the database first, so killing it rebuilds the database
  and every connection after it — a restart being the only recovery DuckDB
  offers.

  DuckDB's other catastrophic mode, an assertion failure, kills the ADBC
  connection process outright instead of returning an error. That path needs no
  policy: this wrapper is linked to it, so both die and the supervisor rebuilds
  the subtree the same way.

  ## Every DuckDB error is an `Adbc.Error`

  adbc returns an error DuckDB raises while executing as an `Adbc.Error`, and
  one it raises while the result streams as a bare string or an
  `ArgumentError`. DuckDB 2.0 streams more of its execution, so the same
  failure, a JSON cast in `read_json` say, arrives either way depending on
  the build. Every reply here carries an `Adbc.Error`, so a caller sorting
  refusals from failures reads one type.

  ## Spill isolation

  Connections default to distinct children of the application `:spill_dir`
  (`.tmp` unless configured). DuckDB temp-file names are not instance-specific,
  so live instances sharing a directory can corrupt each other's spilled sorts.
  `:temp_directory` overrides the default.

  The default leaf names the *instance*, not the engine or the connection: a
  registered connection's leaf carries the name of the `Adbc.Database` it
  connects to, the OS pid, and that database's pid. Every slot of a
  multi-connection engine derives the same leaf, which matters because
  `temp_directory` is one setting per instance and DuckDB refuses to change
  it once a spill has used it — a slot that derived its own leaf would fail
  to restart after any spill. A connection-only restart reuses the
  surviving database's leaf, and a rebuilt database gets a fresh one, because
  the instance a rebuild replaces is still alive — a killed database process
  releases nothing native while its connection is mid-statement, so the
  abandoned statement runs to completion on the old instance. Under one
  shared leaf the new instance opened the old one's spill files by name,
  overwrote them and deleted them when its query finished, and the old
  instance's sort read them back and took the VM down with SIGSEGV
  (T-460). Each instance removes its own leaf when it closes. The OS pid
  keeps two VMs sharing one spill root apart.

  That makes an abandoned instance visible: until its statement finishes,
  its leaf is still there beside the rebuilt one's. `abandoned_spill/1`
  lists those leaves, so a caller can refuse to start another spilling
  merge while one it gave up on is still writing to the same disk (T-601).
  Only a leaf written to recently counts: a leaf that outlived its instance,
  because the instance died without removing it, stops counting once it has
  been quiet for the window, rather than blocking the caller until a restart.

  ## A bootstrap statement waits out a SQLite lock

  A lake on SQLite metadata (a single node, every test) has one file every
  engine's `ATTACH` and side-table `CREATE TABLE` reads, and DuckDB's sqlite
  extension sets no busy timeout: while one connection writes, another
  connection's statement fails with `database is locked` at once. Warm job
  engines, edge supervisors and seals starting together made that one CI run
  in three (T-604, T-574, T-580). A bootstrap statement that fails that way
  is retried, seven attempts over about 1.6 s; any other failure fails the
  start as before.

  Isolation lives here because `Smolquery.QueryService.Runner` creates
  connections directly rather than through `Smolquery.Engine`.

  ## Every session is in UTC

  DuckDB's `TimeZone` defaults to the host's. It decides how a
  `TIMESTAMP WITH TIME ZONE` compares with a plain `TIMESTAMP`, what
  `CAST(now() AS TIMESTAMP)` answers, and so what `now64()` means on the
  ClickHouse edge. A table's timestamps are stored as UTC with no zone, and
  the planner prunes files by them: `Smolquery.QueryService.Pruner` turns a
  `DateTime` bound as `$n` into its UTC instant and compares that with a
  file's bounds (T-426). On an engine in another zone the engine and the
  pruner disagree by the offset, and the files in the gap are pruned while
  their rows match (T-543). A deployment's containers are in UTC and never
  saw it; a developer's machine is not. So every connection sets
  `TimeZone = 'UTC'` first, here for the same reason spill isolation is
  here: it must hold for a job's connection too.

  ## Extensions load one connection at a time per node

  `LOAD` verifies an extension's signature, and DuckDB's
  `ExtensionHelper::CheckExtensionSignature` is not safe to run from two
  instances at once against the same extension directory: a burst of job
  engines starting together threw a C++ exception inside it, which ended in
  `std::terminate` and took the whole VM down, every job with it (T-415).
  So every `INSTALL` and `LOAD` a connection runs, the `:extensions` list
  at its start and `load_extensions/2` later, goes through
  `Smolquery.Engine.ExtensionLoader`, one connection at a time on this
  node, and the settings and statements that follow run outside it. What
  the loader does not see is an extension DuckDB autoloads at first use on
  an engine that did not list it, so an engine lists what it uses.

  A connection that loads `httpfs`, either way, points `ca_cert_file` at the
  node's `Smolquery.Engine.CaBundle`, so a locked engine reads that one file
  to open HTTPS and the lockdown allows nothing else (T-718).
  """

  use GenServer

  require Logger

  alias Smolquery.Engine.CaBundle
  alias Smolquery.Engine.ExtensionLoader
  alias Smolquery.Engine.IpcSchema
  alias Smolquery.Engine.Params
  alias Smolquery.Engine.Result
  alias Smolquery.Engine.ResultTooLarge
  alias Smolquery.Engine.UntypedResult
  alias Smolquery.Identifier

  @default_spill_root ".tmp"

  @locked_attempts 7
  @locked_wait_ms 25

  @fatal_markers ["database has been invalidated", "FATAL Error", "INTERNAL Error"]
  @unconvertible_arrow "Arrow error: the engine cannot convert a column of the result to Arrow"
  @unloadable_arrow "Arrow error: Polars cannot load a column of the result, " <>
                      "such as a VARIANT, which it has no type for"
  @unwrappable ["Parser Error", "Binder Error", "Catalog Error"]
  @interval_in_type ~r/"(?:[^"]|"")*"|'(?:[^']|'')*'|\bINTERVAL\b/
  @unloadable_interval "Arrow error: Polars cannot load an INTERVAL column of the result; " <>
                         "cast it, to VARCHAR for its text or with epoch() for seconds"
  @session [TimeZone: "UTC"]

  @type option ::
          {:database, GenServer.server()}
          | {:name, GenServer.name()}
          | {:extensions, [extension()]}
          | {:settings, keyword()}
          | {:statements, [String.t()]}
          | {:max_rows, pos_integer() | :infinity}
          | {:temp_directory, Path.t()}
          | {:max_temp_directory_size, String.t() | (-> String.t() | nil)}

  @typedoc """
  A DuckDB extension: a core one by name, or `{name, :community}` for one
  `INSTALL`ed `FROM community`.
  """
  @type extension :: atom() | String.t() | {atom() | String.t(), :community}

  @typedoc "A transaction statement: SQL, or SQL labelled with the kind its span reports."
  @type statement :: String.t() | {atom(), String.t()}

  @doc """
  Starts a bootstrapped connection to the ADBC database in `:database`.

  ## Options

    * `:database` (required) — the `Adbc.Database` process to connect to
    * `:name` — process name to register under
    * `:extensions` — DuckDB extensions to `INSTALL` and `LOAD`; `{name, :community}`
      installs from the community repository
    * `:settings` — `SET key = value` pairs applied to the session, after
      `TimeZone = 'UTC'`, which every session gets (see below)
    * `:statements` — SQL run after extensions and settings, in order
    * `:max_rows` — most rows `query/4` will convert to Elixir terms before
      refusing with `Smolquery.Engine.ResultTooLarge`. `:infinity` disables the
      ceiling.
    * `:temp_directory` — spill directory. Defaults to a distinct child of the
      application `:spill_dir` (`.tmp` unless configured).
    * `:max_temp_directory_size` — per-instance spill limit (e.g. `"10GiB"`),
      or a zero-arity function answering one (or `nil` for none). A function
      is called each time an instance starts, so a rebuilt instance sizes its
      limit from the disk as it is then, not as it was at boot (T-601).
      Defaults to the application key of the same name; unset uses DuckDB's
      default of 90% of free space.

  """
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Installs and loads `extensions` on a running connection, through the
  node's `Smolquery.Engine.ExtensionLoader` as the start did: for a
  federated query's `postgres` or `ducklake`, loaded after planning and
  before lockdown. `:ok`, or `{:error, {:extension_failed, name, reason}}`.
  """
  @spec load_extensions(GenServer.server(), [extension()]) :: :ok | {:error, term()}
  def load_extensions(conn, extensions),
    do: GenServer.call(conn, {:load_extensions, extensions}, :infinity)

  @doc """
  Runs `statements` one after another with no parameters and no timeout,
  stopping at the first that fails: the job engine's setup statements,
  whose results nobody reads.
  """
  @spec query_each(GenServer.server(), [String.t()]) :: :ok | {:error, Exception.t()}
  def query_each(conn, statements) do
    Enum.reduce_while(statements, :ok, fn statement, :ok ->
      case query(conn, statement, [], :infinity) do
        {:ok, _result} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @doc """
  Runs `sql` with positional `params` (`$1..$n`), returning a `Result`.

  Parameters are bound through `Smolquery.Engine.Params`, which types timestamps
  to match the columns rather than letting ADBC infer them — an inferred
  timestamp silently costs every query its file pruning.

  A result over `:max_rows` is refused with `Smolquery.Engine.ResultTooLarge`
  instead of converted, because the conversion is what would hurt.
  """
  @spec query(GenServer.server(), String.t(), [term()], timeout()) ::
          {:ok, Result.t()} | {:error, Exception.t()}
  def query(conn, sql, params \\ [], timeout \\ 30_000) do
    GenServer.call(conn, {:query, sql, Params.normalize(params)}, timeout)
  end

  @doc """
  The columns `sql` would produce, as `{name, type}` pairs in result order —
  a `DESCRIBE`, which binds the statement without running it.
  """
  @spec describe(GenServer.server(), String.t(), [term()], timeout()) ::
          {:ok, [{String.t(), String.t()}]} | {:error, Exception.t()}
  def describe(conn, sql, params \\ [], timeout \\ 30_000) do
    with {:ok, result} <- query(conn, "DESCRIBE " <> sql, params, timeout),
         do: {:ok, described_columns(result)}
  end

  @doc """
  Runs `sql` and returns the result as an `Explorer.DataFrame`.

  The Arrow stream is dumped to Arrow IPC (`Adbc.StreamResult.to_ipc_stream/1`)
  and loaded into Polars (`Explorer.DataFrame.load_ipc_stream/2`), so no row is
  ever built as an Elixir term. This runs inside the connection process rather
  than against the ADBC pid directly, so a frame read is subject to the same
  fatal-error policy as `query/4` and holds its connection for the duration —
  which is what it is actually doing.

  Both halves are dirty NIFs, which is why the frame goes through IPC rather than
  `Explorer.DataFrame.from_query/4`. Since DuckDB 2.0 a query executes as its
  result is read, and `from_query/4` reads it in a NIF on a normal scheduler: a
  slow query pinned that scheduler for its whole run, stalling the timers on it,
  a job's deadline among them, and a kill of the reader until it returned. Enough
  concurrent frames would pin every scheduler and stall the node. The price is a
  copy of the whole result through a binary: on DuckDB 1.x the IPC route measured
  38% slower on five million rows (388 ms against 282 ms) through a 226 MiB
  binary.

  A `VARIANT` column has no Arrow type Polars reads. DuckDB 1.5 cannot
  convert one to Arrow and fails the read with an empty message; 2.0 exports
  it as the extension type `arrow.parquet.variant`, on which Polars' IPC
  reader panics. Both are answered as an `Adbc.Error` whose message names
  Arrow, so `Smolquery.QueryService.VariantResults` can tell an export
  failure apart. Any other error raised while the result is read keeps
  DuckDB's message, as an `Adbc.Error`, so every caller sees one error shape
  and `fatal?/1` can read it.

  A frame names each column once, and Polars' IPC reader does not refuse a
  result that repeats a name: it panics with a batch and drops a column
  without one. So the names are read off the IPC schema first
  (`Smolquery.Engine.IpcSchema`), and a repeated one is answered as an
  `Adbc.Error` saying "duplicate", which `SmolqueryPg.PgCatalog` re-selects
  under unique labels.

  An `INTERVAL` column is exported as an Arrow interval, which Polars cannot
  load at all: its IPC reader panics (T-728). A timestamp minus a timestamp
  is one, so a duration column is an ordinary result. So the schema is read
  for intervals too, and a result with one is asked for again with each
  interval as its `VARCHAR` text (`3 days`, `01:00:00`), the same text on
  every edge. A column that holds intervals inside a list, struct or map is
  cast to its own type with `VARCHAR` in place of `INTERVAL`, read off one
  `DESCRIBE` of the wrapped statement, so it keeps its shape and its other
  fields' types (T-737): an `INTERVAL[]` answers a list of text, not one
  display string. Only such a result pays for the `DESCRIBE`; a column that
  is an interval itself is cast to `VARCHAR` without one.
  The statement runs twice, which only a result with an interval pays, and
  the answer is the second run's: a `now()` in it is read again. A
  statement that cannot be wrapped as a subquery (`EXECUTE`, `PRAGMA`, or
  one ending in `; -- comment`) answers an `Adbc.Error` that names
  `INTERVAL` and the cast to write. Any other error of the second run is
  answered as it is, so a fatal one still stops the connection.

  Arrow IPC carries the schema before any batch, so a statement that selects
  nothing keeps its names and types. Explorer's pointer read did not: a
  statement that selects nothing handed it no batch, and a frame built from no
  batch has no columns (T-509). The fallback for that stays for a frame that
  still comes back with none, and for a return to that read. A frame
  that comes back with none is asked for once more — the statement under
  `LIMIT 0`, left-joined to one row, answers a single all-`NULL` row carrying every
  column's type, and its head of zero rows is the result. `LIMIT 0` reads no row,
  but the second statement is bound again, and a bind is not free: a view over
  `read_parquet` reads its files' footers to bind, so an empty result over a
  remote tier pays that I/O twice. It is paid only by a result with no rows. The
  dtypes are the ones Explorer gives the same statement with rows, by
  construction. Only a query wraps as a subquery, so a `CREATE`, a `SET` or a
  statement ending in a semicolon fails to parse there, runs nothing twice, and
  keeps the frame it had. Any other failure of the second statement keeps the
  frame too, except a fatal one, which is answered and stops the connection as
  it would have on the first (`fatal?/1`).
  """
  @spec frame(GenServer.server(), String.t(), [term()], timeout()) ::
          {:ok, Explorer.DataFrame.t()} | {:error, Exception.t()}
  def frame(conn, sql, params \\ [], timeout \\ 30_000) do
    GenServer.call(conn, {:frame, sql, Params.normalize(params)}, timeout)
  end

  @doc """
  The columns `sql` answers, as a frame of no rows: what `frame/4` falls back
  to, asked for directly. `sql` has to wrap as a subquery, so a caller passes
  text with no trailing semicolon or comment. One statement, never the
  statement itself: it runs under `LIMIT 0`.
  """
  @spec shape(GenServer.server(), String.t(), [term()], timeout()) ::
          {:ok, Explorer.DataFrame.t()} | {:error, Exception.t()}
  def shape(conn, sql, params \\ [], timeout \\ 30_000) do
    GenServer.call(conn, {:shape, sql, Params.normalize(params)}, timeout)
  end

  @doc """
  Runs `statements` in order inside one transaction: all commit, or the first
  failure rolls every prior statement back and is returned.

  The whole transaction executes within one call to this process, and that is
  what makes it a transaction at all. An engine has a single connection, so a
  statement from another caller landing between `BEGIN` and `COMMIT` would
  silently join — and be rolled back with — a transaction it knows nothing
  about; serializing through this process closes that interleaving by
  construction.

  Statements carry no parameters. A transaction's SQL is built by trusted
  callers from validated identifiers and escaped literals
  (`Smolquery.Identifier`), and offering bindings here would suggest
  user-supplied values belong in one. They do not.

  ## Timing each statement

  With `span: event`, each statement given as `{kind, sql}` and the `COMMIT`
  (kind `:commit`) are each one `event` span, metadata `%{kind: kind,
  result: :ok | :error}`, timed inside this process. A caller outside it sees
  only the whole call, so this is the only place that can tell a slow
  statement from a slow commit (T-573). A plain string statement is untimed.
  """
  @spec transaction(GenServer.server(), [statement()], timeout(), keyword()) ::
          :ok | {:error, Exception.t()}
  def transaction(conn, statements, timeout \\ 30_000, opts \\ []) do
    case GenServer.call(conn, {:transaction, statements, Keyword.get(opts, :span)}, timeout) do
      {:ok, :committed} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  The underlying `Adbc.Connection` pid, for callers that need ADBC directly.
  """
  @spec adbc_connection(GenServer.server()) :: pid()
  def adbc_connection(conn), do: GenServer.call(conn, :adbc_connection)

  @doc """
  Whether an error means DuckDB has invalidated the database behind it.

  Fatal and internal errors are unrecoverable in place: DuckDB refuses every
  subsequent query on that database until it is restarted.
  """
  @spec fatal?(Exception.t()) :: boolean()
  def fatal?(error) do
    message = Exception.message(error)

    Enum.any?(@fatal_markers, &String.contains?(message, &1))
  end

  @impl true
  def init(opts) do
    database = Keyword.fetch!(opts, :database)
    extensions = Keyword.get(opts, :extensions, [])
    settings = @session ++ spill_settings(opts) ++ Keyword.get(opts, :settings, [])
    statements = Keyword.get(opts, :statements, [])
    max_rows = Keyword.get(opts, :max_rows, :infinity)

    with {:ok, adbc} <- Adbc.Connection.start_link(database: database),
         :ok <- apply_settings(adbc, extension_directory()),
         :ok <- load_extensions_on(adbc, extensions),
         :ok <- apply_settings(adbc, settings),
         :ok <- run_statements(adbc, statements) do
      {:ok, %{adbc: adbc, database: database, max_rows: max_rows}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:query, sql, params}, _from, state) do
    state.adbc
    |> Adbc.Connection.query(sql, params)
    |> exception_shaped()
    |> case do
      {:ok, result} -> convert(result, state.max_rows)
      {:error, error} -> {:error, error}
    end
    |> reply_or_stop(state)
  end

  @impl true
  def handle_call({:frame, sql, params}, _from, state) do
    state.adbc
    |> loadable_frame(sql, params)
    |> shaped(state.adbc, sql, params)
    |> exception_shaped()
    |> reply_or_stop(state)
  end

  @impl true
  def handle_call({:shape, sql, params}, _from, state) do
    state.adbc
    |> shape_of(sql, params)
    |> reply_or_stop(state)
  end

  @impl true
  def handle_call({:transaction, statements, span}, _from, state) do
    state.adbc
    |> run_transaction(statements, span)
    |> reply_or_stop(state)
  end

  @impl true
  def handle_call(:adbc_connection, _from, state) do
    {:reply, state.adbc, state}
  end

  @impl true
  def handle_call({:load_extensions, extensions}, _from, state) do
    {:reply, load_extensions_on(state.adbc, extensions), state}
  end

  defp load_extensions_on(adbc, extensions) do
    with :ok <- ExtensionLoader.load(adbc, extensions) do
      apply_settings(adbc, ca_cert_file(extensions, CaBundle.path()))
    end
  end

  defp ca_cert_file(_extensions, nil), do: []

  defp ca_cert_file(extensions, bundle) do
    if Enum.any?(extensions, &(extension_name(&1) == "httpfs")),
      do: [ca_cert_file: bundle],
      else: []
  end

  defp extension_name({name, _repository}), do: to_string(name)
  defp extension_name(name), do: to_string(name)

  defp shaped({:ok, frame} = answer, adbc, sql, params) do
    with 0 <- Explorer.DataFrame.n_columns(frame),
         {:ok, shape} <- shape_of(adbc, sql, params) do
      {:ok, shape}
    else
      {:error, error} -> if fatal?(error), do: {:error, error}, else: answer
      _carries_its_columns -> answer
    end
  end

  defp shaped(answer, _adbc, _sql, _params), do: answer

  defp shape_of(adbc, sql, params) do
    case adbc |> loadable_frame(shape_sql(sql), params) |> exception_shaped() do
      {:ok, one} -> {:ok, Explorer.DataFrame.head(one, 0)}
      {:error, error} -> {:error, error}
    end
  end

  defp read_frame(adbc, sql, params) do
    with {:ok, answer} <- Adbc.Connection.query_pointer(adbc, sql, params, &ipc_frame/1),
         do: answer
  end

  defp ipc_frame(stream) do
    stream
    |> Adbc.StreamResult.to_ipc_stream()
    |> load_ipc()
  rescue
    error in [Adbc.Error, ArgumentError] -> {:error, described(error)}
  end

  defp described(%{message: ""}), do: %Adbc.Error{message: @unconvertible_arrow}
  defp described(error), do: error

  defp load_ipc(ipc) do
    case ipc |> IpcSchema.fields() |> unloadable() do
      :none -> Explorer.DataFrame.load_ipc_stream(ipc)
      {:repeated, names} -> {:error, %Adbc.Error{message: duplicate_columns(names)}}
      {:intervals, names} -> {:error, {:interval_columns, names}}
    end
  rescue
    error in ErlangError ->
      if error.original == :nif_panicked,
        do: {:error, %Adbc.Error{message: @unloadable_arrow}},
        else: reraise(error, __STACKTRACE__)
  end

  defp unloadable(:error), do: :none

  defp unloadable({:ok, fields}) do
    names = Enum.map(fields, &elem(&1, 0))

    case Enum.uniq(names -- Enum.uniq(names)) do
      [] -> intervals(for {name, kind} <- fields, kind != nil, do: {name, kind})
      repeated -> {:repeated, repeated}
    end
  end

  defp intervals([]), do: :none
  defp intervals(names), do: {:intervals, names}

  defp loadable_frame(adbc, sql, params) do
    case read_frame(adbc, sql, params) do
      {:error, {:interval_columns, columns}} -> intervals_as_text(adbc, sql, params, columns)
      answer -> answer
    end
  end

  defp intervals_as_text(adbc, sql, params, columns) do
    statement = "(" <> (sql |> String.trim() |> String.trim_trailing(";")) <> "\n)"

    with {:ok, casts} <- text_casts(adbc, statement, params, columns) do
      read_frame(adbc, "SELECT * REPLACE (#{casts}) FROM #{statement}", params)
    end
    |> wrapped()
  end

  defp wrapped({:error, {:interval_columns, _cast_did_not_take}}), do: unloadable_interval()
  defp wrapped({:error, :interval_type_unknown}), do: unloadable_interval()

  defp wrapped({:error, %Adbc.Error{message: message}} = error) do
    if String.starts_with?(message, @unwrappable), do: unloadable_interval(), else: error
  end

  defp wrapped(answer), do: answer

  defp unloadable_interval, do: {:error, %Adbc.Error{message: @unloadable_interval}}

  defp text_casts(adbc, statement, params, columns) do
    with {:ok, types} <- text_types(adbc, statement, params, columns) do
      {:ok,
       Enum.map_join(types, ", ", fn {column, type} ->
         quoted = Identifier.quote_label(column)
         "CAST(#{quoted} AS #{type}) AS #{quoted}"
       end)}
    end
  end

  defp text_types(adbc, statement, params, columns) do
    if Enum.all?(columns, &match?({_name, :interval}, &1)) do
      {:ok, for({name, :interval} <- columns, do: {name, "VARCHAR"})}
    else
      with {:ok, described} <- described_types(adbc, statement, params),
           do: nested_text_types(columns, described)
    end
  end

  defp described_types(adbc, statement, params) do
    with {:ok, result} <-
           adbc
           |> Adbc.Connection.query("DESCRIBE SELECT * FROM " <> statement, params)
           |> exception_shaped(),
         {:ok, converted} <- convert(result, :infinity),
         do: {:ok, Map.new(described_columns(converted))}
  end

  defp nested_text_types(columns, described) do
    if Enum.all?(columns, fn {name, _kind} -> Map.has_key?(described, name) end),
      do:
        {:ok, for({name, _kind} <- columns, do: {name, text_type(Map.fetch!(described, name))})},
      else: {:error, :interval_type_unknown}
  end

  defp described_columns(%Result{columns: columns, rows: rows}) do
    Enum.map(rows, fn row ->
      fields = columns |> Enum.zip(row) |> Map.new()

      {fields["column_name"], fields["column_type"]}
    end)
  end

  defp text_type(type) do
    Regex.replace(@interval_in_type, type, fn
      "INTERVAL" -> "VARCHAR"
      quoted -> quoted
    end)
  end

  defp duplicate_columns(repeated),
    do: "duplicate column names in the result: " <> Enum.map_join(repeated, ", ", &inspect/1)

  defp shape_sql(sql) do
    "SELECT shape.* FROM (SELECT 1) AS one LEFT JOIN " <>
      "(SELECT * FROM (#{sql}) LIMIT 0) AS shape ON true"
  end

  @dialyzer {:nowarn_function, exception_shaped: 1}
  defp exception_shaped({:error, message}) when is_binary(message),
    do: {:error, %Adbc.Error{message: message}}

  defp exception_shaped({:error, %ArgumentError{message: message}}),
    do: {:error, %Adbc.Error{message: message}}

  defp exception_shaped(other), do: other

  defp reply_or_stop({:ok, value}, state), do: {:reply, {:ok, value}, state}

  defp reply_or_stop({:error, error}, state) do
    if fatal?(error) do
      invalidate(state, error)
      {:stop, {:database_invalidated, error}, {:error, error}, state}
    else
      {:reply, {:error, error}, state}
    end
  end

  defp run_transaction(adbc, statements, span) do
    with {:ok, _begun} <- Adbc.Connection.query(adbc, "BEGIN TRANSACTION"),
         :ok <- apply_or_rollback(adbc, statements, span),
         {:ok, _committed} <- timed(adbc, {:commit, "COMMIT"}, span) do
      {:ok, :committed}
    end
  end

  defp apply_or_rollback(adbc, statements, span) do
    Enum.reduce_while(statements, :ok, fn statement, :ok ->
      case timed(adbc, statement, span) do
        {:ok, _result} -> {:cont, :ok}
        {:error, error} -> {:halt, rollback(adbc, error)}
      end
    end)
  end

  defp timed(adbc, {kind, sql}, span) when is_list(span) do
    Smolquery.Telemetry.span(
      span,
      &{%{}, %{kind: kind, result: Smolquery.Telemetry.outcome(&1)}},
      fn -> adbc |> Adbc.Connection.query(sql) |> exception_shaped() end
    )
  end

  defp timed(adbc, {_kind, sql}, nil),
    do: adbc |> Adbc.Connection.query(sql) |> exception_shaped()

  defp timed(adbc, sql, _span) when is_binary(sql),
    do: adbc |> Adbc.Connection.query(sql) |> exception_shaped()

  defp rollback(adbc, error) do
    case Adbc.Connection.query(adbc, "ROLLBACK") do
      {:ok, _rolled_back} ->
        {:error, error}

      {:error, rollback_error} ->
        {:error, if(fatal?(rollback_error), do: rollback_error, else: error)}
    end
  end

  defp convert(result, max_rows) do
    case Result.from_adbc(result, max_rows) do
      {:ok, converted} -> {:ok, converted}
      {:error, :too_many_rows} -> {:error, %ResultTooLarge{max: max_rows}}
      {:error, {:untyped, reason}} -> {:error, %UntypedResult{reason: reason}}
    end
  end

  defp apply_settings(adbc, settings) do
    bootstrap(settings, fn {key, value} -> apply_setting(adbc, key, value) end)
  end

  defp extension_directory do
    case Application.get_env(:smolquery, :extension_directory) do
      nil -> []
      directory -> [extension_directory: directory]
    end
  end

  # DuckDB refuses to switch temp_directory once the current one has been
  # used, even to the same value — and a connection-only restart re-runs
  # settings against the surviving database. Skip the SET when it would
  # change nothing.
  defp apply_setting(adbc, :temp_directory = key, value) do
    if current_setting(adbc, key) == to_string(value) do
      :ok
    else
      set_setting(adbc, key, value)
    end
  end

  defp apply_setting(adbc, key, value), do: set_setting(adbc, key, value)

  defp set_setting(adbc, key, value) do
    case Adbc.Connection.query(adbc, "SET #{key} = #{quote_setting(value)}") do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, {:setting_failed, key, reason}}
    end
  end

  defp current_setting(adbc, key) do
    with {:ok, raw} <- Adbc.Connection.query(adbc, "SELECT current_setting('#{key}')"),
         {:ok, result} <- Result.from_adbc(raw, :infinity) do
      Result.one!(result)
    else
      _unreadable -> nil
    end
  end

  defp run_statements(adbc, statements),
    do: bootstrap(statements, &run_statement(adbc, &1))

  defp run_statement(adbc, sql) do
    case unlocked(fn -> Adbc.Connection.query(adbc, sql) end, @locked_attempts) do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, {:statement_failed, safe_sql(sql), reason}}
    end
  end

  defp unlocked(run, attempts) do
    result = run.()

    if attempts > 1 and locked_failure?(result) do
      Process.sleep(@locked_wait_ms * Integer.pow(2, @locked_attempts - attempts))
      unlocked(run, attempts - 1)
    else
      result
    end
  end

  defp locked_failure?({:error, error}), do: locked?(error)
  defp locked_failure?(_result), do: false

  @doc """
  Whether an error is SQLite metadata refusing a statement because another
  connection holds its lock. DuckDB's sqlite extension sets no busy timeout,
  so a lake on SQLite answers every connection but the lock's holder this way
  at once, however briefly the lock is held.
  """
  @spec locked?(term()) :: boolean()
  def locked?(%{__exception__: true} = error),
    do: String.contains?(Exception.message(error), "database is locked")

  def locked?(_error), do: false

  defp safe_sql(sql) do
    if sql =~ ~r/password|secret|key_id/i do
      prefix = sql |> String.split(["'", "("], parts: 2) |> hd() |> String.trim()

      prefix <> " … [statement redacted: carries credentials]"
    else
      sql
    end
  end

  defp invalidate(state, error) do
    Logger.error("""
    DuckDB invalidated the database; restarting the engine subtree.
    #{Exception.message(error)}
    """)

    case GenServer.whereis(state.database) do
      nil -> :ok
      database -> Process.exit(database, :kill)
    end
  end

  defp bootstrap(steps, fun) do
    Enum.reduce_while(steps, :ok, fn step, :ok ->
      case fun.(step) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp quote_setting(value) when is_integer(value), do: Integer.to_string(value)
  defp quote_setting(value) when is_boolean(value), do: Atom.to_string(value)
  defp quote_setting(value), do: value |> to_string() |> Identifier.sql_string()

  # Caller settings run later and may override these defaults. The mkdir is
  # best-effort on purpose: a bad spill root should fail the first spill, at
  # query time, not every boot of an engine that may never spill.
  defp spill_settings(opts) do
    directory =
      Keyword.get_lazy(opts, :temp_directory, fn ->
        default_temp_directory(Keyword.fetch!(opts, :database))
      end)

    File.mkdir_p(Path.dirname(directory))

    [temp_directory: directory] ++ temp_directory_size(opts)
  end

  defp temp_directory_size(opts) do
    opts
    |> Keyword.get_lazy(:max_temp_directory_size, fn ->
      Application.get_env(:smolquery, :max_temp_directory_size)
    end)
    |> case do
      size when is_function(size, 0) -> size.()
      size -> size
    end
    |> case do
      nil -> []
      size -> [max_temp_directory_size: size]
    end
  end

  @doc """
  The spill leaves, under the application `:spill_dir`, of `database`'s other
  instances in this OS process that were written to in the last `quiet_ms`:
  instances a rebuild left running a statement. Empty when `database` is not
  running, or the spill root cannot be listed.
  """
  @spec abandoned_spill(GenServer.server(), non_neg_integer()) :: [Path.t()]
  def abandoned_spill(database, quiet_ms \\ 600_000) do
    root = Application.get_env(:smolquery, :spill_dir, @default_spill_root)

    with pid when is_pid(pid) <- GenServer.whereis(database),
         {:ok, entries} <- File.ls(root) do
      prefix = instance_prefix(database)
      current = prefix <> pid_token(pid)

      since = System.os_time(:second) - div(quiet_ms, 1_000)

      for entry <- entries,
          String.starts_with?(entry, prefix),
          entry != current,
          leaf = Path.join(root, entry),
          written_since?(leaf, since),
          do: leaf
    else
      _none -> []
    end
  end

  defp written_since?(leaf, since) do
    files =
      case File.ls(leaf) do
        {:ok, names} -> Enum.map(names, &Path.join(leaf, &1))
        {:error, _gone} -> []
      end

    Enum.any?([leaf | files], fn path ->
      match?({:ok, %File.Stat{mtime: mtime}} when mtime >= since, File.stat(path, time: :posix))
    end)
  end

  defp default_temp_directory(database) do
    Path.join(
      Application.get_env(:smolquery, :spill_dir, @default_spill_root),
      instance_token(database)
    )
  end

  defp instance_token(database) do
    case Process.info(self(), :registered_name) do
      {:registered_name, name} when is_atom(name) -> registered_instance_token(database)
      _unregistered -> "connection-#{System.unique_integer([:positive])}-os#{System.pid()}"
    end
  end

  defp registered_instance_token(database),
    do: instance_prefix(database) <> pid_token(GenServer.whereis(database))

  defp instance_prefix(database) do
    label = if is_atom(database), do: Atom.to_string(database), else: "database"

    "#{URI.encode(label, &URI.char_unreserved?/1)}-os#{System.pid()}-db"
  end

  defp pid_token(pid) do
    pid
    |> :erlang.pid_to_list()
    |> to_string()
    |> String.trim_leading("<")
    |> String.trim_trailing(">")
  end
end
