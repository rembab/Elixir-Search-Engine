defmodule Search_Engine.MixProject do
  use Mix.Project

  def project do
    [
      app: :search_engine,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:nx, "~> 0.11.0"},
      {:text, "~> 0.6.0"},
      {:json_polyfill, "~> 0.2"},
      {:stemmer, "~> 1.2"},
      {:exqlite, "~> 0.27"}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
