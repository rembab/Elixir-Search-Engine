defmodule Database.Setup do
  def reload_database(json_dir, title_field, content_field, n, n_dict) do
    GenServer.call(Database, :reset_table, :infinity)

    JsonLoader.load_directory_n(json_dir, [title_field, content_field], n)
    |> Stream.map(fn map ->
      %{
        title: map[title_field] || "",
        content: map[content_field] || "",
        stemmed: [],
        embed: [0]
      }
    end)
    |> Stream.chunk_every(100_000)
    |> Stream.each(fn chunk ->
      processed_docs =
        chunk
        |> Flow.from_enumerable()
        |> Flow.map(fn doc ->
          text = doc.title <> " " <> doc.content
          stemmed_words = SEMath.stem_text(text)
          %{doc | stemmed: stemmed_words}
        end)
        |> Enum.to_list()

      Database.batch_write_documents(processed_docs)

      chunk_dict =
        processed_docs
        |> Flow.from_enumerable()
        |> Flow.flat_map(fn doc ->
          Enum.frequencies(doc.stemmed)
          |> Enum.map(fn {word, count} -> {word, 1, count} end)
        end)
        |> Flow.partition(key: {:elem, 0})
        |> Flow.reduce(fn -> %{} end, fn {word, df, tf}, acc ->
          Map.update(acc, word, {df, tf}, fn {current_df, current_tf} ->
            {current_df + df, current_tf + tf}
          end)
        end)
        |> Enum.into(%{})

      Task.start(fn ->
        Database.batch_update_dictionary(chunk_dict)
      end)
    end)
    |> Stream.run()

    Database.delete_single_words()

    total_docs = Database.count()

    Database.score_dictionary(total_docs)

    Database.prune_dictionary(n_dict)
  end

  def vectorize_documents_and_build_matrix(matrix_name) do
    total_docs = Database.count()
    dict_map = Database.get_dictionary_map()
    Database.prepare_matrix_table(matrix_name)

    Database.stream_documents()
    |> Stream.chunk_every(100_000)
    |> Stream.each(fn chunk ->
      results =
        chunk
        |> Flow.from_enumerable()
        |> Flow.map(fn doc ->
          vector =
            SEMath.words_to_vector(doc.stemmed, total_docs, dict_map)
            |> SEMath.normalize_doc_vector()

          update = %{id: doc.id, embed: vector}

          entries =
            Enum.map(vector, fn {term_id, weight} ->
              %{doc_id: doc.id, term_id: term_id, val: weight}
            end)

          {update, entries}
        end)
        |> Enum.to_list()

      {updates, matrix_entries} = Enum.unzip(results)

      Database.batch_update_embeds(updates)
      Database.batch_write_matrix(matrix_name, List.flatten(matrix_entries))
    end)
    |> Stream.run()
  end
end
