defmodule SEMath do
  def create_dict(str, text_key) do
    str
    |> Stream.map(fn json_map -> Map.get(json_map, text_key, "") end)
    |> Stream.flat_map(fn text ->
      text
      |> String.downcase()
      |> String.replace(~r/[^\w\s]/u, "")
      |> Text.Clean.clean()
      |> String.split()
      |> Enum.reject(fn word -> Text.Stopwords.contains?(:en, word) end)
    end)
    |> MapSet.new()
  end

  def create_ngram_dict(str, text_key, n_gram_size \\ 2) do
    str
    |> Stream.map(fn json_map -> Map.get(json_map, text_key, "") end)
    |> Stream.flat_map(fn text ->
      text
      |> String.downcase()
      |> String.replace(~r/[^\w\s]/u, "")
      |> Text.Clean.clean()
      |> String.split()
      |> Enum.reject(fn word -> Text.Stopwords.contains?(:en, word) end)
      |> Enum.chunk_every(n_gram_size, 1, :discard)
      |> Enum.map(fn chunk -> Enum.join(chunk, " ") end)
    end)
    |> MapSet.new()
  end

  def create_full_dict(str, text_key, n_gram_size \\ 2) do
    MapSet.union(create_dict(str, text_key), create_ngram_dict(str, text_key, n_gram_size))
  end
end
