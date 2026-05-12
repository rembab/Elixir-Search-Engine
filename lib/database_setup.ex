defmodule Database.Setup do
  def reload_database(json_path, title_field, content_field, n) do
    GenServer.call(Database, :reset_table)

    JsonLoader.load_n(json_path, [title_field, content_field], n)
    |> Stream.map(fn map ->
      %{
        title: map[title_field] || "",
        content: map[content_field] || "",
        embed: [0]
      }
    end)
    |> Stream.chunk_every(1000)
    |> Stream.each(fn chunk ->
      Database.batch_write_documents(chunk)

      chunk_dict =
        Enum.reduce(chunk, %{}, fn doc, acc ->
          text = doc.title <> " " <> doc.content
          words = SEMath.stem_text(text)

          local_freqs = Enum.frequencies(words)

          Enum.reduce(local_freqs, acc, fn {word, count}, inner_acc ->
            Map.update(inner_acc, word, {1, count}, fn {current_df, current_tf} ->
              {current_df + 1, current_tf + count}
            end)
          end)
        end)

      Database.batch_update_dictionary(chunk_dict)
    end)
    |> Stream.run()
  end

  def vectorize_documents_and_build_matrix() do
    total_docs = Database.count()
    dict_map = Database.get_dictionary_map()

    Database.stream_documents()
    |> Stream.chunk_every(1000)
    |> Stream.each(fn chunk ->
      updates =
        Enum.map(chunk, fn doc ->
          text = doc.title <> " " <> doc.content
          words = SEMath.stem_text(text)

          tf_map = Enum.frequencies(words)

          vector =
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

          %{id: doc.id, embed: vector}
        end)

      matrix_entries =
        Enum.flat_map(updates, fn %{id: doc_id, embed: vector} ->
          Enum.map(vector, fn {term_id, weight} ->
            %{doc_id: doc_id, term_id: term_id, val: weight}
          end)
        end)

      Database.batch_update_embeds(updates)
      Database.batch_write_matrix(matrix_entries)
    end)
    |> Stream.run()
  end
end
