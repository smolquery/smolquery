defmodule Smolquery.QueryService.LockdownTest do
  use ExUnit.Case, async: true

  alias Smolquery.Engine
  alias Smolquery.Engine.CaBundle
  alias Smolquery.InternalSecret
  alias Smolquery.QueryService.Lockdown

  describe "statements/2" do
    test "lists first, the node's CA bundle among the paths, external access off, then the lock" do
      bundle = CaBundle.path()
      assert is_binary(bundle)

      assert Lockdown.statements(["/data", "s3://sealed/"], ["http://hot/a.parquet"]) == [
               "SET allowed_directories = ['/data', 's3://sealed/']",
               "SET allowed_paths = ['http://hot/a.parquet', '#{bundle}']",
               "SET enable_external_access = false",
               "SET lock_configuration = true"
             ]
    end

    test "always closes external access: the lists bind nothing while it is open (T-690)" do
      assert "SET enable_external_access = false" in Lockdown.statements([], [])
    end
  end

  describe "an engine whose first HTTPS connection opens after the lockdown" do
    test "gets as far as an open engine does, past the CA bundle to the connect (T-718)" do
      url = "https://127.0.0.1:#{closed_port()}/partial.parquet"

      open = https_error(url, [])
      locked = https_error(url, Lockdown.statements([], [url]))

      assert open =~ "Could not connect"
      assert locked == open
    end
  end

  defp closed_port do
    {:ok, socket} = :gen_tcp.listen(0, ip: {127, 0, 0, 1})
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)

    port
  end

  defp https_error(url, lockdown) do
    engine = :"lockdown_https_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {Engine,
       name: engine,
       extensions: [:httpfs],
       statements: [
         "SET http_retries = 0",
         "SET http_timeout = 5",
         InternalSecret.create_secret_statement("https://")
       ]},
      id: engine
    )

    for statement <- lockdown, do: {:ok, _set} = Engine.query(engine, statement)

    assert {:error, error} = Engine.query(engine, "SELECT * FROM read_parquet('#{url}')")

    Exception.message(error)
  end
end
