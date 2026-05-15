defmodule Database do
  use GenServer

  alias Exqlite.Sqlite3

  def start_link(db_path) when is_binary(db_path) do
    GenServer.start_link(__MODULE__, db_path, name: __MODULE__)
  end

  def get_doc_by_id(id) when is_number(id) do
    GenServer.call(__MODULE__, {:get_doc_by_id, id}, :infinity)
  end

  def batch_write_documents(documents) when is_list(documents) do
    GenServer.call(__MODULE__, {:batch_write_doc, documents}, :infinity)
  end

  def batch_update_dictionary(term_stats) when is_map(term_stats) do
    GenServer.call(__MODULE__, {:batch_update_dictionary, term_stats}, :infinity)
  end

  def batch_write_matrix(matrix_name, matrix_entries) when is_list(matrix_entries) do
    GenServer.call(__MODULE__, {:batch_write_matrix, matrix_name, matrix_entries}, :infinity)
  end

  def count() do
    GenServer.call(__MODULE__, :count, :infinity)
  end

  def count_dict() do
    GenServer.call(__MODULE__, :count_dict, :infinity)
  end

  def prepare_matrix_table(name) do
    GenServer.call(__MODULE__, {:prepare_matrix_table, name}, :infinity)
  end

  def matrix_table_exists(matrix_name) when is_binary(matrix_name) do
    GenServer.call(__MODULE__, {:matrix_table_exists, matrix_name}, :infinity)
  end

  def fetch_document_column(matrix_name, doc_id) when is_binary(matrix_name) do
    GenServer.call(__MODULE__, {:fetch_doc_col, matrix_name, doc_id}, :infinity)
  end

  def update_document_column(matrix_name, doc_id, entries) when is_list(entries) do
    GenServer.call(__MODULE__, {:update_doc_col, matrix_name, doc_id, entries}, :infinity)
  end

  def get_dictionary_map() do
    GenServer.call(__MODULE__, :get_dictionary_map, :infinity)
  end

  def get_dictionary_map(words) do
    GenServer.call(__MODULE__, {:get_dictionary_map_from_words, words}, :infinity)
  end

  def batch_update_embeds(updates) when is_list(updates) do
    GenServer.call(__MODULE__, {:batch_update_embeds, updates}, :infinity)
  end

  def delete_single_words() do
    GenServer.call(__MODULE__, :delete_single_words, :infinity)
  end

  def score_dictionary(total_docs) do
    GenServer.call(__MODULE__, {:score_dictionary, total_docs}, :infinity)
  end

  def prune_dictionary(n_dict) do
    GenServer.call(__MODULE__, {:prune_dictionary, n_dict}, :infinity)
  end

  def stream_documents() do
    db_path = GenServer.call(__MODULE__, :get_db_path)

    Stream.resource(
      fn ->
        {:ok, conn} = Sqlite3.open(db_path)

        {:ok, statement} =
          Sqlite3.prepare(conn, "SELECT id, title, content, stemmed, embed FROM documents")

        %{conn: conn, statement: statement}
      end,
      fn %{conn: conn, statement: statement} = state ->
        case Sqlite3.step(conn, statement) do
          {:row, [id, title, content, stem_blob, embed_blob]} ->
            record = %{
              id: id,
              title: title,
              content: content,
              stemmed: :erlang.binary_to_term(stem_blob),
              embed: :erlang.binary_to_term(embed_blob)
            }

            {[record], state}

          :done ->
            {:halt, state}
        end
      end,
      fn %{conn: conn, statement: statement} ->
        Sqlite3.release(conn, statement)
        Sqlite3.close(conn)
      end
    )
  end

  @impl true
  def init(db_path) do
    {:ok, conn} = Sqlite3.open(db_path)

    :ok = Sqlite3.execute(conn, "PRAGMA journal_mode=WAL;")
    :ok = Sqlite3.execute(conn, "PRAGMA synchronous=OFF;")
    :ok = Sqlite3.execute(conn, "PRAGMA temp_store=MEMORY;")
    :ok = Sqlite3.execute(conn, "PRAGMA cache_size=-64000;")
    :ok = Sqlite3.execute(conn, "PRAGMA journal_size_limit = 524288000;")
    create_tables(conn)
    {:ok, %{conn: conn, db_path: db_path}}
  end

  @impl true
  def handle_call(:reset_table, _from, %{conn: conn} = state) do
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
    {:ok, statement} = Sqlite3.prepare(conn, query)

    tables_to_drop = fetch_all_table_names(conn, statement, [])
    :ok = Sqlite3.release(conn, statement)

    Enum.each(tables_to_drop, fn table ->
      :ok = Sqlite3.execute(conn, "DROP TABLE IF EXISTS #{table}")
    end)

    create_tables(conn)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:get_doc_by_id, id}, _from, %{conn: conn} = state) do
    {:ok, statement} =
      Sqlite3.prepare(conn, "SELECT title, content FROM documents WHERE id = ?;")

    :ok = Sqlite3.bind(statement, [id])

    {:row, [title, content]} = Sqlite3.step(conn, statement)

    {:reply, {title, content}, state}
  end

  @impl true
  def handle_call(:delete_single_words, _from, %{conn: conn} = state) do
    :ok = Sqlite3.execute(conn, "DELETE FROM dictionary WHERE global_frequency < 3")
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:score_dictionary, total_docs}, _from, %{conn: conn} = state) do
    {:ok, statement} =
      Sqlite3.prepare(conn, "SELECT id, document_frequency, global_frequency FROM dictionary")

    entries = fetch_dict_for_scoring(conn, statement, [])
    :ok = Sqlite3.release(conn, statement)

    :ok = Sqlite3.execute(conn, "BEGIN TRANSACTION")
    {:ok, update_stmt} = Sqlite3.prepare(conn, "UPDATE dictionary SET score = ?1 WHERE id = ?2")

    Enum.each(entries, fn {id, df, gf} ->
      idf = :math.log(total_docs / max(df, 1))
      score = gf * idf

      :ok = Sqlite3.bind(update_stmt, [score, id])
      :done = Sqlite3.step(conn, update_stmt)
    end)

    :ok = Sqlite3.release(conn, update_stmt)
    :ok = Sqlite3.execute(conn, "COMMIT")

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:prune_dictionary, n_dict}, _from, %{conn: conn} = state) do
    :ok = Sqlite3.execute(conn, "BEGIN TRANSACTION")

    create_temp_sql = """
    CREATE TABLE dictionary_temp (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      term TEXT UNIQUE NOT NULL,
      document_frequency INTEGER NOT NULL,
      global_frequency INTEGER NOT NULL,
      score FLOAT DEFAULT 0.0
    );
    """

    :ok = Sqlite3.execute(conn, create_temp_sql)

    insert_temp_sql = """
    INSERT INTO dictionary_temp (term, document_frequency, global_frequency, score)
    SELECT term, document_frequency, global_frequency, score
    FROM dictionary
    ORDER BY score DESC
    LIMIT ?1;
    """

    {:ok, statement} = Sqlite3.prepare(conn, insert_temp_sql)
    :ok = Sqlite3.bind(statement, [n_dict])
    :done = Sqlite3.step(conn, statement)
    :ok = Sqlite3.release(conn, statement)

    :ok = Sqlite3.execute(conn, "DROP TABLE dictionary;")

    :ok = Sqlite3.execute(conn, "ALTER TABLE dictionary_temp RENAME TO dictionary;")

    :ok = Sqlite3.execute(conn, "COMMIT")

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:prepare_matrix_table, matrix_name}, _from, %{conn: conn} = state) do
    drop_sql = "DROP TABLE IF EXISTS #{matrix_name};"
    :ok = Sqlite3.execute(conn, drop_sql)

    create_sql = """
    CREATE TABLE #{matrix_name} (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      doc_id INTEGER NOT NULL,
      term_id INTEGER NOT NULL,
      val FLOAT(24) NOT NULL,
      UNIQUE(doc_id, term_id)
    );
    """

    :ok = Sqlite3.execute(conn, create_sql)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:matrix_table_exists, matrix_name}, _from, %{conn: conn} = state) do
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?1;"
    {:ok, statement} = Sqlite3.prepare(conn, query)
    :ok = Sqlite3.bind(statement, [matrix_name])

    exists =
      case Sqlite3.step(conn, statement) do
        {:row, [_name]} -> true
        :done -> false
      end

    :ok = Sqlite3.release(conn, statement)

    {:reply, exists, state}
  end

  @impl true
  def handle_call({:batch_write_doc, documents}, _from, %{conn: conn} = state) do
    :ok = Sqlite3.execute(conn, "BEGIN TRANSACTION")

    query = "INSERT INTO documents (title, content, stemmed, embed) VALUES (?1, ?2, ?3, ?4)"
    {:ok, statement} = Sqlite3.prepare(conn, query)

    Enum.each(documents, fn %{title: title, content: content, stemmed: stemmed, embed: embed} ->
      embed_blob = :erlang.term_to_binary(embed)
      stem_blob = :erlang.term_to_binary(stemmed)
      :ok = Sqlite3.bind(statement, [title, content, stem_blob, embed_blob])
      :done = Sqlite3.step(conn, statement)
    end)

    :ok = Sqlite3.release(conn, statement)
    :ok = Sqlite3.execute(conn, "COMMIT")

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:batch_update_dictionary, term_stats}, _from, %{conn: conn} = state) do
    :ok = Sqlite3.execute(conn, "BEGIN TRANSACTION")

    query = """
    INSERT INTO dictionary (term, document_frequency, global_frequency)
    VALUES (?1, ?2, ?3)
    ON CONFLICT(term) DO UPDATE SET 
      document_frequency = document_frequency + excluded.document_frequency,
      global_frequency = global_frequency + excluded.global_frequency
    """

    {:ok, statement} = Sqlite3.prepare(conn, query)

    Enum.each(term_stats, fn {term, {df, tf}} ->
      :ok = Sqlite3.bind(statement, [term, df, tf])
      :done = Sqlite3.step(conn, statement)
    end)

    :ok = Sqlite3.release(conn, statement)
    :ok = Sqlite3.execute(conn, "COMMIT")

    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:count, _from, %{conn: conn} = state) do
    {:ok, statement} = Sqlite3.prepare(conn, "SELECT MAX(id) FROM documents")

    count =
      case Sqlite3.step(conn, statement) do
        {:row, [total]} when is_integer(total) -> total
        _ -> 0
      end

    :ok = Sqlite3.release(conn, statement)

    {:reply, count, state}
  end

  @impl true
  def handle_call(:count_dict, _from, %{conn: conn} = state) do
    {:ok, statement} = Sqlite3.prepare(conn, "SELECT MAX(id) FROM dictionary")

    count =
      case Sqlite3.step(conn, statement) do
        {:row, [total]} when is_integer(total) -> total
        _ -> 0
      end

    :ok = Sqlite3.release(conn, statement)

    {:reply, count, state}
  end

  @impl true
  def handle_call(:get_dictionary_map, _from, %{conn: conn} = state) do
    {:ok, statement} =
      Sqlite3.prepare(
        conn,
        "SELECT id, term, document_frequency, global_frequency FROM dictionary"
      )

    dict_map = fetch_dict_rows(conn, statement, %{})

    :ok = Sqlite3.release(conn, statement)

    {:reply, dict_map, state}
  end

  @impl true
  def handle_call({:get_dictionary_map_from_words, words}, _from, %{conn: conn} = state)
      when is_list(words) do
    dict_map =
      words
      |> Enum.chunk_every(500)
      |> Enum.reduce(%{}, fn chunk, acc ->
        placeholders =
          1..length(chunk)
          |> Enum.map(fn i -> "?#{i}" end)
          |> Enum.join(", ")

        query =
          "SELECT id, term, document_frequency, global_frequency FROM dictionary WHERE term IN (#{placeholders})"

        {:ok, statement} = Sqlite3.prepare(conn, query)
        :ok = Sqlite3.bind(statement, chunk)

        updated_acc = fetch_dict_rows(conn, statement, acc)

        :ok = Sqlite3.release(conn, statement)

        updated_acc
      end)

    {:reply, dict_map, state}
  end

  @impl true
  def handle_call({:batch_update_embeds, updates}, _from, %{conn: conn} = state) do
    :ok = Sqlite3.execute(conn, "BEGIN TRANSACTION")

    query = "UPDATE documents SET embed = ?1 WHERE id = ?2"
    {:ok, statement} = Sqlite3.prepare(conn, query)

    Enum.each(updates, fn %{id: id, embed: embed} ->
      embed_blob = :erlang.term_to_binary(embed)

      :ok = Sqlite3.bind(statement, [embed_blob, id])
      :done = Sqlite3.step(conn, statement)
    end)

    :ok = Sqlite3.release(conn, statement)
    :ok = Sqlite3.execute(conn, "COMMIT")

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:batch_write_matrix, matrix_name, vals}, _from, %{conn: conn} = state) do
    :ok = Sqlite3.execute(conn, "BEGIN TRANSACTION")

    query = """
    INSERT INTO #{matrix_name} (doc_id, term_id, val)
    VALUES (?1, ?2, ?3)
    """

    {:ok, statement} = Sqlite3.prepare(conn, query)

    Enum.each(vals, fn %{doc_id: doc_id, term_id: term_id, val: val} ->
      :ok = Sqlite3.bind(statement, [doc_id, term_id, val])
      :done = Sqlite3.step(conn, statement)
    end)

    :ok = Sqlite3.release(conn, statement)
    :ok = Sqlite3.execute(conn, "COMMIT")

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:fetch_doc_col, matrix_name, doc_id}, _from, %{conn: conn} = state) do
    query = "SELECT term_id, val FROM #{matrix_name} WHERE doc_id = ?1"

    {:ok, statement} = Sqlite3.prepare(conn, query)
    :ok = Sqlite3.bind(statement, [doc_id])

    entries = fetch_col_rows(conn, statement, [])

    :ok = Sqlite3.release(conn, statement)
    {:reply, entries, state}
  end

  @impl true
  def handle_call({:update_doc_col, matrix_name, doc_id, entries}, _from, %{conn: conn} = state) do
    :ok = Sqlite3.execute(conn, "BEGIN TRANSACTION")

    query = """
    INSERT INTO #{matrix_name} (doc_id, term_id, val)
    VALUES (?1, ?2, ?3)
    ON CONFLICT(doc_id, term_id) DO UPDATE SET val = excluded.val
    """

    {:ok, statement} = Sqlite3.prepare(conn, query)

    Enum.each(entries, fn %{term_id: term_id, val: val} ->
      :ok = Sqlite3.bind(statement, [doc_id, term_id, val])
      :done = Sqlite3.step(conn, statement)
    end)

    :ok = Sqlite3.release(conn, statement)
    :ok = Sqlite3.execute(conn, "COMMIT")

    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:get_db_path, _from, %{db_path: db_path} = state) do
    {:reply, db_path, state}
  end

  @impl true
  def terminate(_reason, %{conn: conn}) do
    Sqlite3.close(conn)
  end

  defp fetch_dict_for_scoring(conn, statement, acc) do
    case Sqlite3.step(conn, statement) do
      {:row, [id, df, gf]} ->
        fetch_dict_for_scoring(conn, statement, [{id, df, gf} | acc])

      :done ->
        acc
    end
  end

  defp fetch_col_rows(conn, statement, acc) do
    case Sqlite3.step(conn, statement) do
      {:row, [term_id, val]} ->
        fetch_col_rows(conn, statement, [%{term_id: term_id, val: val} | acc])

      :done ->
        Enum.reverse(acc)
    end
  end

  defp fetch_dict_rows(conn, statement, acc) do
    case Sqlite3.step(conn, statement) do
      {:row, [id, term, df, gf]} ->
        fetch_dict_rows(conn, statement, Map.put(acc, term, {id, df, gf}))

      :done ->
        acc
    end
  end

  defp fetch_all_table_names(conn, statement, acc) do
    case Sqlite3.step(conn, statement) do
      {:row, [name]} ->
        fetch_all_table_names(conn, statement, [name | acc])

      :done ->
        acc
    end
  end

  defp create_tables(conn) do
    create_sql = """
    CREATE TABLE IF NOT EXISTS documents (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      title TEXT NOT NULL,
      content TEXT NOT NULL,
      stemmed BLOB,
      embed BLOB
    );
    CREATE TABLE IF NOT EXISTS dictionary (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      term TEXT UNIQUE NOT NULL,
      document_frequency INTEGER NOT NULL,
      global_frequency INTEGER NOT NULL,
      score FLOAT DEFAULT 0.0
    );
    """

    :ok = Sqlite3.execute(conn, create_sql)
  end
end
