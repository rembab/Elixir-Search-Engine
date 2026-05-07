defmodule JsonLoader do
  @moduledoc """
  Loading large json files.
  """

  defp filter_fields(json_str, fields_to_keep) do
    json_str
    |> Stream.map(fn map -> Map.take(map, fields_to_keep) end)
  end

  @doc """
  Loads a json file and returns a list of its first n values.
  Keeps only the fields specified in fields_to_keep

  """
  def load_n(filename, n, fields_to_keep) do
    json_map =
      filename
      |> File.stream!(read_ahead: 100_000)
      |> Stream.flat_map(fn line ->
        case JSON.decode(line) do
          {:ok, map} -> [map]
          _ -> []
        end
      end)
      |> Stream.take(n)

    filter_fields(json_map, fields_to_keep)
  end
end
