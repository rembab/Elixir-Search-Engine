defmodule JsonLoader do
  defp filter_fields(json_stream, fields_to_keep) do
    json_stream
    |> Stream.map(fn map -> Map.take(map, fields_to_keep) end)
  end

  def load_all(filename, fields_to_keep) do
    filename
    |> File.stream!(read_ahead: 100_000)
    |> Stream.flat_map(fn line ->
      case Jason.decode(line) do
        {:ok, map} -> [map]
        _ -> []
      end
    end)
    |> filter_fields(fields_to_keep)
  end

  def load_n(filename, fields_to_keep, n) do
    load_all(filename, fields_to_keep)
    |> Stream.take(n)
  end

  def load_directory(dir_path, fields_to_keep) do
    dir_path
    |> Path.join("**/*.json*")
    |> Path.wildcard()
    |> Stream.flat_map(fn filename ->
      load_all(filename, fields_to_keep)
    end)
  end

  def load_directory_n(dir_path, fields_to_keep, n) do
    load_directory(dir_path, fields_to_keep)
    |> Stream.take(n)
  end
end
