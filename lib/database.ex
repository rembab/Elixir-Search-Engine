defmodule Database do
  use GenServer

  alias Exqlite.Sqlite3

  def start_link(db_path) when is_binary(db_path) do
    GenServer.start_link(__MODULE__, db_path, name: __MODULE__)
  end

  def get_doc_by_id(id) when is_number(id) do
    GenServer.call(__MODULE__, {:get_doc_by_id, id})
  end

  def write_document(title, content, embed) do
    GenServer.call(__MODULE__, {:write_doc, title, content, embed})
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
    GenServer.call(__MODULE__, :count)
  end

  def prepare_matrix_table(name) do
    GenServer.call(__MODULE__, {:prepare_matrix_table, name})
  end

  def matrix_table_exists(matrix_name) when is_binary(matrix_name) do
    GenServer.call(__MODULE__, {:matrix_table_exists, matrix_name})
  end

  def fetch_document_column(matrix_name, doc_id) when is_binary(matrix_name) do
    GenServer.call(__MODULE__, {:fetch_doc_col, matrix_name, doc_id})
  end

  def update_document_column(matrix_name, doc_id, entries) when is_list(entries) do
    GenServer.call(__MODULE__, {:update_doc_col, matrix_name, doc_id, entries}, :infinity)
  end

  def get_dictionary_map() do
    GenServer.call(__MODULE__, :get_dictionary_map)
  end

  def get_dictionary_map(words) do
    GenServer.call(__MODULE__, {:get_dictionary_map_from_words, words})
  end

  def batch_update_embeds(updates) when is_list(updates) do
    GenServer.call(__MODULE__, {:batch_update_embeds, updates}, :infinity)
  end

  def stream_documents() do
    db_path = GenServer.call(__MODULE__, :get_db_path)

    Stream.resource(
      fn ->
        {:ok, conn} = Sqlite3.open(db_path)

        {:ok, statement} =
          Sqlite3.prepare(conn, "SELECT id, title, content, embed FROM documents")

        %{conn: conn, statement: statement}
      end,
      fn %{conn: conn, statement: statement} = state ->
        case Sqlite3.step(conn, statement) do
          {:row, [id, title, content, embed_blob]} ->
            record = %{
              id: id,
              title: title,
              content: content,
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
  def handle_call({:prepare_matrix_table, matrix_name}, _from, %{conn: conn} = state) do
    drop_sql = "DROP TABLE IF EXISTS #{matrix_name};"
    IO.puts(drop_sql)
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
  def handle_call({:write_doc, title, content, embed}, _from, %{conn: conn} = state) do
    embed_blob = :erlang.term_to_binary(embed)

    query = "INSERT INTO documents (title, content, embed) VALUES (?1, ?2, ?3)"
    {:ok, statement} = Sqlite3.prepare(conn, query)

    :ok = Sqlite3.bind(statement, [title, content, embed_blob])
    :done = Sqlite3.step(conn, statement)
    :ok = Sqlite3.release(conn, statement)

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

    query = "INSERT INTO documents (title, content, embed) VALUES (?1, ?2, ?3)"
    {:ok, statement} = Sqlite3.prepare(conn, query)

    Enum.each(documents, fn %{title: title, content: content, embed: embed} ->
      embed_blob = :erlang.term_to_binary(embed)

      :ok = Sqlite3.bind(statement, [title, content, embed_blob])
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
      embed BLOB
    );
    CREATE TABLE IF NOT EXISTS dictionary (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      term TEXT UNIQUE NOT NULL,
      document_frequency INTEGER NOT NULL,
      global_frequency INTEGER NOT NULL
    );
    """

    :ok = Sqlite3.execute(conn, create_sql)
  end
end
