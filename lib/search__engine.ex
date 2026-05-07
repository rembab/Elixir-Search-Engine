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
  def show_docs do
    json_stream = JsonLoader.load_n("data/arxiv_metadata.json", 1, ["title", "abstract"])
    json_stream |> Enum.to_list()
  end

  def show_dict do
    json_stream = JsonLoader.load_n("data/arxiv_metadata.json", 1, ["title", "abstract"])
    SEMath.create_dict(json_stream, "abstract")
  end

  def show_ngram_dict do
    json_stream = JsonLoader.load_n("data/arxiv_metadata.json", 1, ["title", "abstract"])
    SEMath.create_ngram_dict(json_stream, "abstract")
  end
end
