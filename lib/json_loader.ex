defmodule JsonLoader do
  defp filter_fields(json_str, fields_to_keep) do
    json_str
    |> Stream.map(fn map -> Map.take(map, fields_to_keep) end)
  end

  def load_all(filename, fields_to_keep) do
    json_map =
      filename
      |> File.stream!(read_ahead: 100_000)
      |> Stream.flat_map(fn line ->
        case Jason.decode(line) do
          {:ok, map} -> [map]
          _ -> []
        end
      end)

    filter_fields(json_map, fields_to_keep)
  end

  def load_n(filename, fields_to_keep, n) do
    load_all(filename, fields_to_keep)
    |> Stream.take(n)
  end
end
