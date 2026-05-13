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
    PythonPort.start_link("data/db/arxiv.db")
  end

  def reload_database(n) do
    Database.Setup.reload_database("data/arxiv_metadata.json", "title", "abstract", n)
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
