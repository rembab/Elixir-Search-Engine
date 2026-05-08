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
  def start() do
    Database.start_link("data/db/arxiv.db")
  end

  def reload_database(n) do
    Database.reload_database("data/arxiv_metadata.json", "title", "abstract", n)
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

  def show_dict(n) do
    json_stream = JsonLoader.load_n("data/arxiv_metadata.json", n, ["title", "abstract"])
    SEMath.create_dict(json_stream, "abstract")
  end

  def show_matrix(n) do
    filename = "data/arxiv_metadata.json"
    text_key = "abstract"

    documents =
      JsonLoader.load_n(filename, n, ["title", text_key])
      |> Enum.to_list()

    IO.puts("Building global dictionary...")
    global_dict = SEMath.create_dict(documents, text_key)

    IO.puts("Building vocabulary map...")
    vocab_map = TermDictionary.new(global_dict)
    IO.puts("Loaded distinct words: ")
    IO.puts(map_size(vocab_map))

    IO.puts("Building sparse matrix...")
    sparse_matrix = SEMath.build_sparse_matrix(documents, text_key, vocab_map, global_dict)

    IO.inspect(sparse_matrix, label: "Sparse Matrix (Row, Col, TF-IDF)")
  end
end
