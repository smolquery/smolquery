defmodule Smolquery.Engine.CallExitedTest do
  use ExUnit.Case, async: true

  alias Smolquery.Engine.CallExited

  test "new/1 strips the GenServer.call mfa from an exit reason" do
    assert CallExited.new({:timeout, {GenServer, :call, [:conn, {:query, "SELECT 1"}, 5]}}) ==
             %CallExited{reason: :timeout}

    assert CallExited.new(:noproc) == %CallExited{reason: :noproc}
  end

  test "catching/1 answers the call's own value, and an exit as an error" do
    assert CallExited.catching(fn -> {:ok, 1} end) == {:ok, 1}
    assert CallExited.catching(fn -> exit(:boom) end) == {:error, %CallExited{reason: :boom}}
  end

  test "an exit reads as the reason in its message" do
    assert Exception.message(%CallExited{reason: :noproc}) =~ ":noproc"
  end
end
