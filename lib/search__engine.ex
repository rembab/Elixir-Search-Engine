defmodule Search_Engine do
  @moduledoc """
  Documentation for `Search_Engine`.
  """
  @doc """
  Hello world.

  ## Examples

      iex> Search_Engine.hello()
      :world

  """

  def start(db_name \\ "arxiv.db") do
    db_path = "data/db/#{db_name}"

    IO.puts("Loading db #{db_name}")
    Database.start_link(db_path)

    IO.puts("Starting python math engine")
    PythonPort.start_link(db_path)
  end

  def reload_database(
        json \\ "data/arxiv_metadata.json",
        title_field \\ "title",
        content_field \\ "abstract",
        base_matrix_name \\ "full_matrix",
        n
      ) do
    IO.puts("Loading documents and dictionary")

    {micros, _result} =
      :timer.tc(fn -> Database.Setup.reload_database(json, title_field, content_field, n) end)

    IO.puts("Took: #{micros / 1000}")

    IO.puts("Vectorizing vectors and building the full matrix")

    {micros, _result} =
      :timer.tc(fn -> Database.Setup.vectorize_documents_and_build_matrix(base_matrix_name) end)
    IO.puts("Took: #{micros / 1000}")
  end

  def reload_database() do
    reload_database(1_000_000_000)
  end

  def show_docs(n) do
    Database.stream_documents() |> Stream.take(n) |> Enum.to_list()
  end

  def show_docs() do
    show_docs(1_000_000_000)
  end

  def show_dict() do
    Database.get_dictionary_map()
  end

  def query() do
  end
end
