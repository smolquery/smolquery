defmodule Smolquery.IngestService.SchemaCache do
  @moduledoc """
  Table schemas, cached so the insert hot path does not pay a catalog query
  per request.

  A public named ETS table owned by a GenServer that does nothing else but
  listen — reads and misses run entirely in the caller, so the cache adds no
  process hop. Entries expire after the runtime's `schema_cache_ttl_ms`; the
  API's CRUD routes invalidate eagerly on the node they run on; and the owner
  subscribes to `Smolquery.Lifecycle.subscribe_schema/0`, so a column added
  or dropped anywhere in the cluster — another node's API, an `ALTER TABLE`
  on the query path — drops the table here as soon as the broadcast lands
  (PL-61 L3). The TTL bounds staleness for whatever no broadcast reaches: a
  node partitioned from the PubSub, an out-of-band catalog change.

  A miss reads through `Smolquery.Catalog.table_schema/2` and caches only
  success — an unknown table stays a per-request catalog answer rather than a
  cached rejection that would outlive the table's creation.

  The table can be missing while the service's runtime is published: the
  ingest supervisor publishes it before this process starts, and a restart
  under its `rest_for_one` drops the table until the new one is made. Every
  ETS call here treats that as the service not being up yet, so an insert
  answers `:ingest_service_unavailable` (a 503) and an invalidation has
  nothing to drop. A `CREATE TABLE` in that window used to answer 500 after
  the catalog had already created the table (T-767).
  """

  use GenServer

  alias Smolquery.Catalog
  alias Smolquery.IngestService.Runtime
  alias Smolquery.Lifecycle
  alias Smolquery.Schema
  alias Smolquery.Segments.Store

  @spec start_link(Runtime.t()) :: GenServer.on_start()
  def start_link(%Runtime{} = runtime) do
    GenServer.start_link(__MODULE__, runtime, name: Runtime.cache(runtime.name))
  end

  @doc """
  The table's schema, from cache or through the catalog.

  `{:error, :ingest_service_unavailable}` while the cache has no table yet.
  """
  @spec fetch(Runtime.t(), Store.table_ref()) :: {:ok, Schema.t()} | {:error, term()}
  def fetch(%Runtime{} = runtime, table_ref) do
    table = Runtime.cache(runtime.name)

    case on_table(fn -> :ets.lookup(table, table_ref) end) do
      [{^table_ref, schema, expires_at}] ->
        if now() < expires_at, do: {:ok, schema}, else: read_through(runtime, table, table_ref)

      [] ->
        read_through(runtime, table, table_ref)

      :no_table ->
        {:error, :ingest_service_unavailable}
    end
  end

  @doc """
  Drops a table's cached schema, so the next insert reads the catalog.

  A cache that has no table yet holds nothing to drop.
  """
  @spec invalidate(Runtime.t(), Store.table_ref()) :: :ok
  def invalidate(%Runtime{} = runtime, table_ref) do
    _deleted = on_table(fn -> :ets.delete(Runtime.cache(runtime.name), table_ref) end)

    :ok
  end

  @impl GenServer
  def init(%Runtime{} = runtime) do
    table =
      :ets.new(Runtime.cache(runtime.name), [
        :named_table,
        :public,
        :set,
        read_concurrency: true
      ])

    _subscribed = subscribe_schema()

    {:ok, table}
  end

  @impl GenServer
  def handle_info({:lifecycle, %{kind: :schema_change, table_ref: table_ref}}, table) do
    :ets.delete(table, table_ref)

    {:noreply, table}
  end

  def handle_info(_message, table), do: {:noreply, table}

  defp subscribe_schema do
    Lifecycle.subscribe_schema()
  rescue
    ArgumentError -> {:error, :pubsub_unavailable}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp read_through(runtime, table, table_ref) do
    with {:ok, schema} <- Catalog.table_schema(runtime.catalog, table_ref) do
      _inserted =
        on_table(fn ->
          :ets.insert(table, {table_ref, schema, now() + runtime.schema_cache_ttl_ms})
        end)

      {:ok, schema}
    end
  end

  defp on_table(call) do
    call.()
  rescue
    ArgumentError -> :no_table
  end

  defp now, do: System.monotonic_time(:millisecond)
end
