defmodule Flashlight.MixProject do
  use Mix.Project

  def project do
    [
      app: :flashlight,
      version: "0.1.0",
      elixir: "~> 1.14",
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
      # {:explorer, "~> 0.7.1"},
      {:explorer, path: "/Users/mzappitello/mbta/explorer"},
      {:ex_aws, "~> 2.5.0"},
      {:ex_aws_s3, "~> 2.0"},
      {:hackney, "~> 1.9"},
      {:sweet_xml, "~> 0.6"},
      {:timex, "~> 3.7.11"},
      {:jason, "~> 1.4.1"},
      {:benchee, "~>1.1.0"}

      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
