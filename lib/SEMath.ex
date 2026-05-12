defmodule SEMath do
  def stem_text(str) do
    str
    |> String.replace(~r/[^\w\s]/u, "")
    |> String.downcase()
    |> String.split()
    |> Enum.reject(&Text.Stopwords.contains?(:en, &1))
    |> Enum.map(&Stemmer.stem(&1))
  end

  def create_dict(str, text_key) do
    str
    |> Stream.chunk_every(1000)
    |> Flow.from_enumerable()
    |> Flow.flat_map(fn chunk ->
      Enum.flat_map(chunk, fn json_map ->
        words =
          json_map
          |> Map.get(text_key, "")
          |> stem_text()

        local_freqs = Enum.frequencies(words)

        Map.to_list(local_freqs)
      end)
    end)
    |> Flow.partition()
    |> Flow.reduce(fn -> %{} end, fn {word, local_count}, acc ->
      Map.update(acc, word, {local_count, 1}, fn {current_tf, current_df} ->
        {current_tf + local_count, current_df + 1}
      end)
    end)
    |> Enum.into(%{})
  end
end
