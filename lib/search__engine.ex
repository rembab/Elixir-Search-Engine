defmodule Search_Engine do
  def start(db_name \\ "main_database.db") do
    db_path = "data/db/#{db_name}"
    Database.start_link(db_path)
    PythonPort.start_link(db_path)
  end

  def reload_database(
        json_directory \\ "data/arxiv",
        title_field \\ "title",
        content_field \\ "abstract",
        base_matrix_name \\ "matrix_full",
        n,
        n_d
      ) do
    IO.puts("Wiping python caches")
    Path.wildcard("data/db/*.npz") |> Enum.each(&File.rm/1)

    if Process.whereis(PythonPort), do: GenServer.stop(PythonPort)
    PythonPort.start_link("data/db/main_database.db")

    IO.puts("Loading documents and dictionary")

    {micros, _result} =
      :timer.tc(fn ->
        Database.Setup.reload_database(json_directory, title_field, content_field, n, n_d)
      end)

    IO.puts("Took: #{micros / 1000} ms")

    IO.puts("Vectorizing vectors and building the full matrix")

    {micros, _result} =
      :timer.tc(fn -> Database.Setup.vectorize_documents_and_build_matrix(base_matrix_name) end)

    IO.puts("Took: #{micros / 1000} ms")
  end

  def reload_database() do
    reload_database(500_000, 500_000)
  end

  def search(query, k \\ nil, num_results \\ 10) do
    words = SEMath.stem_text(query)
    dict_map = Database.get_dictionary_map(words)
    total_docs = Database.count()

    query_vector =
      SEMath.words_to_vector(words, total_docs, dict_map)
      |> SEMath.normalize_doc_vector()

    if Enum.empty?(query_vector) do
      []
    else
      matrix_to_use = k_matrix(k)

      is_ready =
        if is_nil(k) do
          Database.matrix_table_exists(matrix_to_use)
        else
          File.exists?("data/db/#{matrix_to_use}_svd.npz")
        end

      unless is_ready do
        if is_number(k) do
          PythonPort.calculate_svd("matrix_full", matrix_to_use, k)
        end
      end

      %{"results" => results} = PythonPort.search(matrix_to_use, query_vector, num_results)

      results
      |> Enum.map(fn %{"doc_id" => doc_id} -> Database.get_doc_by_id(doc_id) end)
      |> Enum.map(fn {title, content} -> {Text.Clean.clean(title), Text.Clean.clean(content)} end)
    end
  end

  def stop() do
    GenServer.stop(Database)
  end

  defp k_matrix(k) do
    case k do
      n when is_number(n) -> "matrix_#{k}"
      _ -> "matrix_full"
    end
  end
end
