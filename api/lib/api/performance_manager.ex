defmodule Api.PerformanceManager do
  @moduledoc """
  The PerformanceManager context.
  """

  import Ecto.Query, warn: false
  alias Api.Repo

  alias Api.PerformanceManager.MetadataLog

  @doc """
  Returns the list of metadata_log.

  ## Examples

      iex> list_metadata_log()
      [%MetadataLog{}, ...]

  """
  def list_metadata_log do
    Repo.all(MetadataLog)
  end

  @doc """
  Gets a single metadata_log.

  Raises `Ecto.NoResultsError` if the Metadata log does not exist.

  ## Examples

      iex> get_metadata_log!(123)
      %MetadataLog{}

      iex> get_metadata_log!(456)
      ** (Ecto.NoResultsError)

  """
  def get_metadata_log!(pk_id), do: Repo.get!(MetadataLog, pk_id)

end
