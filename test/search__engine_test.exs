defmodule Search_EngineTest do
  use ExUnit.Case
  doctest Search_Engine

  test "greets the world" do
    assert Search_Engine.hello() == :world
  end
end
