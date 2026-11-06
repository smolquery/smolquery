defmodule SmolqueryWeb.QueryLiveTest do
  use SmolqueryWeb.ConnCase, async: false

  alias Smolquery.QueryService
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.FixedCatalog

  defp start_query!(opts) do
    query = :"web_query_#{:erlang.unique_integer([:positive])}"
    catalog = FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}})

    start_supervised!(
      {QueryService.Supervisor, [name: query, catalog: catalog] ++ opts},
      id: query
    )

    on_exit(fn -> QueryService.Runtime.delete(query) end)
    query
  end

  describe "without a query service" do
    test "renders the editor", %{conn: conn} do
      start_web!()

      {:ok, _lv, html} = live(conn, ~p"/query")

      assert html =~ "Query"
      assert html =~ "SELECT"
    end

    test "run reports the service as unavailable", %{conn: conn} do
      start_web!()

      {:ok, lv, _html} = live(conn, ~p"/query")

      html = render_submit(lv, "run", %{"query" => %{"sql" => "SELECT 1"}})

      assert html =~ "query service is not running"
    end

    test "run rejects blank SQL", %{conn: conn} do
      start_web!()

      {:ok, lv, _html} = live(conn, ~p"/query")

      html = render_submit(lv, "run", %{"query" => %{"sql" => "   "}})

      assert html =~ "Write some SQL first"
    end
  end

  describe "editor state in the URL" do
    test "the sql param repopulates the editor", %{conn: conn} do
      start_web!()

      {:ok, _lv, html} = live(conn, ~p"/query?#{[sql: "SELECT 7 AS seven"]}")

      assert html =~ "SELECT 7 AS seven"
    end

    test "the sql param survives into a run", %{conn: conn} do
      start_web!()

      {:ok, lv, _html} = live(conn, ~p"/query?#{[sql: "SELECT 7 AS seven"]}")

      refute render_click(lv, "explain", %{"mode" => "plan"}) =~ "Write some SQL first"
    end

    test "trace=false and distributed=false uncheck the toggles", %{conn: conn} do
      start_web!(query_name: start_query!(distributed: [local_workers: 2]))

      {:ok, _lv, html} = live(conn, ~p"/query?#{[trace: "false", distributed: "false"]}")

      assert html =~ ~s|name="query[distributed]"|
      refute html =~ "checked"
    end

    test "the file_cache param picks the File cache choice and survives a change", %{conn: conn} do
      start_web!()

      {:ok, lv, html} = live(conn, ~p"/query?#{[file_cache: "off"]}")

      assert html =~ ~s(<option value="off" selected)

      render_change(lv, "sql_changed", %{
        "query" => %{"sql" => "SELECT 1", "trace" => "true", "file_cache" => "on"}
      })

      assert assert_patch(lv) =~ "file_cache=on"
    end

    test "typing patches the sql into the URL", %{conn: conn} do
      start_web!(query_name: start_query!(distributed: [local_workers: 2]))

      {:ok, lv, _html} = live(conn, ~p"/query")

      render_change(lv, "sql_changed", %{
        "query" => %{"sql" => "SELECT 1", "trace" => "true", "distributed" => "true"}
      })

      assert assert_patch(lv) ==
               ~p"/query?#{[sql: "SELECT 1", trace: "true", distributed: "true", file_cache: "auto"]}"
    end

    test "clearing the editor drops the sql from the URL", %{conn: conn} do
      start_web!(query_name: start_query!(distributed: [local_workers: 2]))

      {:ok, lv, _html} = live(conn, ~p"/query?#{[sql: "SELECT 1"]}")

      render_change(lv, "sql_changed", %{
        "query" => %{"sql" => "", "trace" => "true", "distributed" => "true"}
      })

      assert assert_patch(lv) ==
               ~p"/query?#{[trace: "true", distributed: "true", file_cache: "auto"]}"
    end

    test "a query too long for the link stays out of it and says so", %{conn: conn} do
      start_web!(query_name: start_query!(distributed: [local_workers: 2]))

      {:ok, lv, _html} = live(conn, ~p"/query")

      long = "SELECT " <> String.duplicate("a", 5000)

      html =
        render_change(lv, "sql_changed", %{
          "query" => %{"sql" => long, "trace" => "true", "distributed" => "true"}
        })

      assert assert_patch(lv) ==
               ~p"/query?#{[trace: "true", distributed: "true", file_cache: "auto"]}"

      assert html =~ "too long for the link"
    end
  end

  describe "with a query service" do
    setup do
      query = :"web_query_#{:erlang.unique_integer([:positive])}"

      start_supervised!(
        {QueryService.Supervisor,
         name: query, catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}})},
        id: query
      )

      on_exit(fn -> QueryService.Runtime.delete(query) end)

      %{query: query}
    end

    test "runs a query to done and renders the rows", %{conn: conn, query: query} do
      start_web!(query_name: query)

      {:ok, lv, _html} = live(conn, ~p"/query")

      render_submit(lv, "run", %{"query" => %{"sql" => "SELECT 42 AS answer"}})

      assert Eventually.until(fn -> render(lv) =~ "done" end)

      html = render(lv)
      assert html =~ "answer"
      assert html =~ "42"
    end

    test "cancel is safe once the job is already terminal", %{conn: conn, query: query} do
      start_web!(query_name: query)

      {:ok, lv, _html} = live(conn, ~p"/query")

      render_submit(lv, "run", %{"query" => %{"sql" => "SELECT 1"}})

      assert Eventually.until(fn -> render(lv) =~ "done" end)

      assert render_click(lv, "cancel") =~ "done"
    end

    test "the job status renders on its own row under the controls (T-697)", %{
      conn: conn,
      query: query
    } do
      start_web!(query_name: query)

      {:ok, lv, _html} = live(conn, ~p"/query")

      refute has_element?(lv, "#job-status")

      render_submit(lv, "run", %{"query" => %{"sql" => "SELECT 1 AS n"}})

      assert Eventually.until(fn -> has_element?(lv, "#job-status .badge", "done") end)
      assert has_element?(lv, "#job-status", "1 rows")
      refute has_element?(lv, "#job-status button")
    end

    test "a run renders the scan statistics line", %{conn: conn, query: query} do
      start_web!(query_name: query)

      {:ok, lv, _html} = live(conn, ~p"/query")

      render_submit(lv, "run", %{"query" => %{"sql" => "SELECT 1 AS n"}})

      assert Eventually.until(fn -> render(lv) =~ "done" end)

      assert render(lv) =~ "0/0 files"
    end

    test "explain renders the engine's plan instead of rows", %{conn: conn, query: query} do
      start_web!(query_name: query)

      {:ok, lv, _html} = live(conn, ~p"/query")

      render_change(lv, "sql_changed", %{"query" => %{"sql" => "SELECT 1 AS n"}})
      render_click(lv, "explain", %{"mode" => "plan"})

      assert Eventually.until(fn -> render(lv) =~ "done" end)

      html = render(lv)
      assert html =~ ~r/projection/i
      refute html =~ "data_table"
    end

    test "explain rejects blank SQL", %{conn: conn, query: query} do
      start_web!(query_name: query)

      {:ok, lv, _html} = live(conn, ~p"/query")

      assert render_click(lv, "explain", %{"mode" => "plan"}) =~ "Write some SQL first"
    end

    test "the burst controls stay hidden on a node that leases no runners", %{
      conn: conn,
      query: query
    } do
      start_web!(query_name: query)

      {:ok, lv, html} = live(conn, ~p"/query")

      refute html =~ ~s|name="query[burst]"|

      render_change(lv, "sql_changed", %{
        "query" => %{"sql" => "SELECT 1", "trace" => "true", "distributed" => "true"}
      })

      refute assert_patch(lv) =~ "burst"
    end

    test "on a node that leases runners the burst toggle and nodes field render and survive the URL (T-679)",
         %{conn: conn} do
      pool = :"web_burst_pool_#{:erlang.unique_integer([:positive])}"

      start_web!(
        query_name: start_query!(burst: [enabled: true, pool: pool, backend_name: "lambda"])
      )

      {:ok, lv, html} = live(conn, ~p"/query?#{[burst: "false", burst_nodes: "4"]}")

      assert html =~ ~s|name="query[distributed]"|
      assert html =~ ~s|name="query[burst]"|
      assert html =~ ~s|name="query[burst_nodes]"|
      assert html =~ ~s|value="4"|

      render_change(lv, "sql_changed", %{
        "query" => %{
          "sql" => "SELECT 1",
          "trace" => "true",
          "distributed" => "true",
          "burst" => "true",
          "burst_nodes" => "8",
          "file_cache" => "auto"
        }
      })

      patched = assert_patch(lv)
      assert patched =~ "burst=true"
      assert patched =~ "burst_nodes=8"

      for junk <- ["lots", "0", "-1"] do
        render_change(lv, "sql_changed", %{
          "query" => %{"sql" => "SELECT 1", "trace" => "true", "burst_nodes" => junk}
        })

        assert assert_patch(lv) =~ "burst_nodes=8"
      end
    end

    test "the trace toggle starts enabled", %{conn: conn} do
      start_web!()

      {:ok, _lv, html} = live(conn, ~p"/query")

      assert html =~ ~s|name="query[trace]"|
      assert html =~ "checked"
    end

    test "the distribute toggle starts enabled", %{conn: conn} do
      start_web!(query_name: start_query!(distributed: [local_workers: 2]))

      {:ok, _lv, html} = live(conn, ~p"/query?#{[trace: "false"]}")

      assert html =~ ~s|name="query[distributed]"|
      assert html =~ "checked"
    end

    test "a hidden distribute toggle stays out of the URL and leaves the choice alone (T-769)", %{
      conn: conn,
      query: query
    } do
      start_web!(query_name: query)

      {:ok, lv, _html} = live(conn, ~p"/query")

      render_change(lv, "sql_changed", %{"query" => %{"sql" => "SELECT 1", "trace" => "true"}})

      assert assert_patch(lv) ==
               ~p"/query?#{[sql: "SELECT 1", trace: "true", file_cache: "auto"]}"

      assert :sys.get_state(lv.pid).socket.assigns.distributed
    end

    test "the distribute toggle stays hidden on one query node with no runners (T-769)", %{
      conn: conn,
      query: query
    } do
      start_web!(query_name: query)

      {:ok, _lv, html} = live(conn, ~p"/query")

      refute html =~ ~s|name="query[distributed]"|
    end

    test "one query node whose runners share its machine offers neither toggle (T-769)", %{
      conn: conn
    } do
      pool = :"web_burst_pool_#{:erlang.unique_integer([:positive])}"
      query = start_query!(burst: [enabled: true, pool: pool, backend_name: "local"])
      start_web!(query_name: query)

      {:ok, _lv, html} = live(conn, ~p"/query")

      refute html =~ ~s|name="query[distributed]"|
      refute html =~ ~s|name="query[burst]"|
    end

    test "a traced run renders a waterfall", %{conn: conn, query: query} do
      start_web!(query_name: query)

      {:ok, lv, _html} = live(conn, ~p"/query")

      render_submit(lv, "run", %{"query" => %{"sql" => "SELECT 1 AS n", "trace" => "true"}})

      assert Eventually.until(fn -> render(lv) =~ "done" end)

      html = render(lv)
      assert html =~ "Trace"
      assert html =~ "engine_start"
      assert html =~ "execute"
    end

    test "unchecking the toggle skips the trace", %{conn: conn, query: query} do
      start_web!(query_name: query)

      {:ok, lv, _html} = live(conn, ~p"/query")

      render_submit(lv, "run", %{"query" => %{"sql" => "SELECT 1 AS n"}})

      assert Eventually.until(fn -> render(lv) =~ "done" end)

      refute render(lv) =~ "engine_start"
    end

    test "a broken query surfaces the job error", %{conn: conn, query: query} do
      start_web!(query_name: query)

      {:ok, lv, _html} = live(conn, ~p"/query")

      render_submit(lv, "run", %{"query" => %{"sql" => "SELECT FROM WHERE"}})

      assert Eventually.until(fn -> render(lv) =~ "badge-error" end)
    end
  end
end
