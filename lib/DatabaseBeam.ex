defmodule DatabaseBeam do
  use GenServer

  @moduledoc """
  Loading large json files.
  """
  def start_link(initial_state \\ %{}) do
    GenServer.start_link(__MODULE__, initial_state, name: __MODULE__)
  end

  @impl true
  def init(initial_state) do
    {:ok, initial_state}
  end

  @impl true
  def handle_cast({:put, key, value}, state) do
    new_state = Map.put(state, key, value)

    {:noreply, new_state}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    value = Map.get(state, key)

    {:reply, value, state}
  end
end
