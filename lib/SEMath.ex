defmodule SEMath do
  def stem_text(str) do
    str
    |> String.replace(~r/[^\w\s]/u, "")
    |> String.downcase()
    |> String.split()
    |> Enum.reject(&Stopwords.contains(&1))
    |> Text.Stemmer.stem_list(:en)
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

  def words_to_vector(words, total_docs, dict_map) do
    tf_map = Enum.frequencies(words)

    Enum.reduce(tf_map, [], fn {word, tf}, acc ->
      case Map.get(dict_map, word) do
        {word_id, df, _} when df > 0 ->
          idf = :math.log(total_docs / df)
          weight = tf * idf
          [{word_id, weight} | acc]

        _ ->
          acc
      end
    end)
  end

  def normalize_doc_vector(vector) do
    sqr_sum =
      Enum.reduce(vector, 0, fn {_term_id, val}, acc ->
        acc + val * val
      end)

    len_vec = :math.sqrt(sqr_sum)

    vector
    |> Enum.map(fn {term_id, val} -> {term_id, val / len_vec} end)
  end
end
