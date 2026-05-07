defmodule TermDictionary do
  def new(word_counts_map) do
    word_counts_map
    |> Map.keys()
    |> Enum.with_index()
    |> Map.new()
  end
end
