defmodule Database do
  use GenServer

  alias Exqlite.Sqlite3

  def start_link(db_path) when is_binary(db_path) do
    GenServer.start_link(__MODULE__, db_path, name: __MODULE__)
  end

  def reload_database(json_path, title_field, content_field, n) do
    GenServer.call(__MODULE__, :reset_table)

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
      batch_write(chunk)

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

      batch_update_dictionary(chunk_dict)
    end)
    |> Stream.run()
  end

  def write(title, content, embed) do
    GenServer.call(__MODULE__, {:write, title, content, embed})
  end

  def batch_write(documents) when is_list(documents) do
    GenServer.call(__MODULE__, {:batch_write, documents}, :infinity)
  end

  def batch_update_dictionary(term_stats) when is_map(term_stats) do
    GenServer.call(__MODULE__, {:batch_update_dictionary, term_stats}, :infinity)
  end

  def count() do
    GenServer.call(__MODULE__, :count)
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
    :ok = Sqlite3.execute(conn, "DROP TABLE IF EXISTS documents")
    :ok = Sqlite3.execute(conn, "DROP TABLE IF EXISTS dictionary")
    create_tables(conn)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:write, title, content, embed}, _from, %{conn: conn} = state) do
    embed_blob = :erlang.term_to_binary(embed)

    query = "INSERT INTO documents (title, content, embed) VALUES (?1, ?2, ?3)"
    {:ok, statement} = Sqlite3.prepare(conn, query)

    :ok = Sqlite3.bind(statement, [title, content, embed_blob])
    :done = Sqlite3.step(conn, statement)
    :ok = Sqlite3.release(conn, statement)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:batch_write, documents}, _from, %{conn: conn} = state) do
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
  def handle_call(:get_db_path, _from, %{db_path: db_path} = state) do
    {:reply, db_path, state}
  end

  @impl true
  def terminate(_reason, %{conn: conn}) do
    Sqlite3.close(conn)
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
