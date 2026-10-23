defmodule Smolquery.Engine.CaBundle do
  @moduledoc """
  The one CA bundle every engine's `httpfs` trusts (T-718).

  `httpfs` on DuckDB 2.0 reads its CA bundle through the engine's file
  system when the engine opens its first HTTPS connection, so a locked
  engine (`Smolquery.QueryService.Lockdown`) is refused that read unless the
  file is in `allowed_paths`. Left to itself, `httpfs` searches a private list
  of paths. Instead, the node finds its bundle once, every connection that
  loads `httpfs` points `ca_cert_file` at it, and the lockdown allows that one
  path and no other.

  The bundle is `SSL_CERT_FILE` when that names a file, then the first file
  among `candidates/0`, the paths `httpfs` itself searches, in its order. A
  node with none has no bundle: its engines keep `httpfs`'s own search, and a
  locked engine's HTTPS read fails with `httpfs`'s message.

  `allowed_paths` admits writes as well as reads, so SQL that reached a write
  under the lockdown could replace this file as whatever user the node runs
  as. The planner admits no write (`Smolquery.QueryService.Planner`), and the
  image runs the node as `smolquery` (uid 10001), which cannot write the
  root-owned bundle (T-722). A node that can write the bundle it found, run
  as root or pointed by `SSL_CERT_FILE` at a file of its own, logs a warning
  once, when the bundle is first looked up.
  """

  require Logger

  @candidates [
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/ca-bundle.pem",
    "/etc/ssl/cert.pem"
  ]

  @doc """
  The paths `httpfs` searches for a CA bundle, in its order.
  """
  @spec candidates() :: [String.t()]
  def candidates, do: @candidates

  @doc """
  This node's CA bundle, found on first use and kept for the node's life;
  `nil` when it has none.
  """
  @spec path() :: String.t() | nil
  def path do
    case :persistent_term.get(__MODULE__, :unset) do
      :unset ->
        found = find(System.get_env("SSL_CERT_FILE"), @candidates)
        warn_writable(found)
        :persistent_term.put(__MODULE__, found)
        found

      found ->
        found
    end
  end

  @doc """
  The bundle among `override` and `candidates`: `override` when it names a
  file, else the first candidate that does, else `nil`.
  """
  @spec find(String.t() | nil, [String.t()]) :: String.t() | nil
  def find(override, candidates),
    do: Enum.find(List.wrap(override) ++ candidates, &File.regular?/1)

  @doc """
  Whether this node's user may write `path`.
  """
  @spec writable?(String.t()) :: boolean()
  def writable?(path) do
    case File.stat(path) do
      {:ok, %File.Stat{access: access}} -> access in [:write, :read_write]
      {:error, _reason} -> false
    end
  end

  defp warn_writable(nil), do: :ok

  defp warn_writable(bundle) do
    if writable?(bundle) do
      Logger.warning(
        "the CA bundle #{bundle} is writable by this node, and every locked engine " <>
          "may write it (allowed_paths); run the node as a user that does not own it (T-722)"
      )
    end

    :ok
  end
end
