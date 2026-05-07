defmodule Database do
  use GenServer

  @doc """
  Starts a new bucket.
  """
  def start_link(opts) do
  end

  @doc """
  Gets a value from the `bucket` by `key`.
  """
  def get(bucket, key) do
    GenServer.call(bucket, {:get, key})
  end

  @doc """
  Puts the `value` for the given `key` in the `bucket`.
  """
  def put(bucket, key, value) do
    GenServer.call(bucket, {:put, key, value})
  end

  @doc """
  Deletes `key` from `bucket`.

  Returns the current value of `key`, if `key` exists.
  """
  def delete(bucket, key) do
    GenServer.call(bucket, {:delete, key})
  end

  ### Callbacks

  @impl true
  def init(:new) do
    json_stream = JsonLoader.load_n("data/arxiv_metadata.json", 10, ["title", "abstract"])
    state = %{}
    {:ok, state}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    value = get_in(state.bucket[key])
    {:reply, value, state}
  end

  def handle_call({:put, key, value}, _from, state) do
    state = put_in(state.bucket[key], value)
    {:reply, :ok, state}
  end

  def handle_call({:delete, key}, _from, state) do
    {value, state} = pop_in(state.bucket[key])
    {:reply, value, state}
  end
end
