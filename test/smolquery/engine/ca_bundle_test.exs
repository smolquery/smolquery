defmodule Smolquery.Engine.CaBundleTest do
  use ExUnit.Case, async: true

  alias Smolquery.Engine.CaBundle

  @moduletag :tmp_dir

  test "searches httpfs's own paths, in its order" do
    assert CaBundle.candidates() == [
             "/etc/ssl/certs/ca-certificates.crt",
             "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
             "/etc/pki/tls/certs/ca-bundle.crt",
             "/etc/ssl/ca-bundle.pem",
             "/etc/ssl/cert.pem"
           ]
  end

  test "SSL_CERT_FILE first, then the first candidate that is a file, else none", %{
    tmp_dir: dir
  } do
    [override, first, second] = for name <- ~w(override first second), do: Path.join(dir, name)
    for file <- [first, second], do: File.write!(file, "pem")
    missing = Path.join(dir, "missing")

    assert CaBundle.find(nil, [missing, first, second]) == first
    assert CaBundle.find(missing, [first]) == first
    assert CaBundle.find(dir, [second]) == second

    File.write!(override, "pem")
    assert CaBundle.find(override, [first]) == override

    assert CaBundle.find(nil, [missing]) == nil
  end

  test "the node's bundle is one of the files it searched" do
    assert CaBundle.path() in [System.get_env("SSL_CERT_FILE"), nil | CaBundle.candidates()]
  end

  test "writable?/1 is whether this node's user may write the file (T-722)", %{tmp_dir: dir} do
    own = Path.join(dir, "own.pem")
    File.write!(own, "pem")
    read_only = Path.join(dir, "read-only.pem")
    File.write!(read_only, "pem")
    File.chmod!(read_only, 0o444)

    assert CaBundle.writable?(own)
    refute CaBundle.writable?(read_only)
    refute CaBundle.writable?(Path.join(dir, "missing.pem"))
  end
end
