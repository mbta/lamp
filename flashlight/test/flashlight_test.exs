defmodule FlashlightTest do
  use ExUnit.Case
  doctest Flashlight

  test "greets the world" do
    assert Flashlight.hello() == :world
  end
end
