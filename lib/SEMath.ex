defmodule SEMath do
  def stem_text(str) do
    str
    |> String.replace(~r/[^\w\s]/u, "")
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

  def build_sparse_matrix(documents, text_key, vocab_map, global_dict) do
    total_docs = Enum.count(documents)

    documents
    |> Stream.with_index()
    |> Stream.flat_map(fn {doc, doc_index} ->
      text = Map.get(doc, text_key, "")

      word_frequencies =
        stem_text(text)
        |> Enum.frequencies()

      Enum.map(word_frequencies, fn {word, tf} ->
        case Map.get(vocab_map, word) do
          nil ->
            nil

          term_index ->
            {_global_tf, df} = Map.get(global_dict, word, {0, 1})
            idf = :math.log(total_docs / df)
            tf_idf = tf * idf
            {term_index, doc_index, tf_idf}
        end
      end)
      |> Enum.reject(&is_nil/1)
    end)
    |> Enum.to_list()
  end
end
