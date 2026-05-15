defmodule PythonPort do
  use GenServer

  def start_link(db_path) do
    GenServer.start_link(__MODULE__, db_path, name: __MODULE__)
  end

  def search(matrix_name, query_vector, top_k) do
    GenServer.call(__MODULE__, {:search, matrix_name, query_vector, top_k}, :infinity)
  end

  def calculate_svd(matrix_name, new_matrix_name, k) do
    GenServer.call(__MODULE__, {:svd, matrix_name, new_matrix_name, k}, :infinity)
  end

  @impl true
  def init(db_path) do
    port =
      Port.open(
        {:spawn, "python3 lib/pySEMath.py --db #{db_path}"},
        [:binary, :line, :hide]
      )

    {:ok, %{port: port}}
  end

  @impl true
  def handle_call({:search, matrix_name, query_vector, top_k}, _from, state) do
    request = %{
      "action" => "search",
      "matrix_name" => matrix_name,
      "query" => Enum.map(query_vector, fn {id, w} -> [id, w] end),
      "top_k" => top_k
    }

    json_str = Jason.encode!(request)
    Port.command(state.port, json_str <> "\n")

    case read_line_from_port(state.port, "", :infinity) do
      {:ok, json_response} ->
        {:reply, Jason.decode!(json_response), state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:svd, matrix_name, new_matrix_name, k}, _from, state) do
    request = %{
      "action" => "svd",
      "matrix_name" => matrix_name,
      "new_matrix_name" => new_matrix_name,
      "k" => k
    }

    json_str = Jason.encode!(request)
    Port.command(state.port, json_str <> "\n")

    case read_line_from_port(state.port, "", :infinity) do
      {:ok, json_response} ->
        {:reply, Jason.decode!(json_response), state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp read_line_from_port(port, acc, timeout) do
    receive do
      {^port, {:data, {:noeol, chunk}}} ->
        read_line_from_port(port, acc <> chunk, timeout)

      {^port, {:data, {:eol, line}}} ->
        {:ok, acc <> line}
    after
      timeout ->
        {:error, "Python script timed out"}
    end
  end
end
