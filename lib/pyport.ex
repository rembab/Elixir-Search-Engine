defmodule PythonPort do
  use GenServer

  def start_link(db_path) do
    GenServer.start_link(__MODULE__, db_path, name: __MODULE__)
  end

  def search(matrix_name, query, top_k) do
    vector =
      query
      |> SEMath.stem_text()
      |> SEMath.words_to_vector(Database.count(), Database.get_dictionary_map())

    GenServer.call(__MODULE__, {:search, matrix_name, vector, top_k}, :infinity)
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
      "matrix_name" => matrix_name,
      "query" => Enum.map(query_vector, fn {id, w} -> [id, w] end),
      "top_k" => top_k
    }

    json_str = Jason.encode!(request)
    Port.command(state.port, json_str <> "\n")

    receive do
      {_port, {:data, {:eol, json_response}}} ->
        {:reply, Jason.decode!(json_response), state}
    after
      5000 -> {:reply, {:error, "Python script timed out"}, state}
    end
  end
end
