defmodule ApiWeb.MetadataLogJSON do
  alias Api.PerformanceManager.MetadataLog

  @doc """
  Renders a list of metadata_log.
  """
  def index(%{metadata_log: metadata_log}) do
    %{data: for(metadata_log <- metadata_log, do: data(metadata_log))}
  end

  @doc """
  Renders a single metadata_log.
  """
  def show(%{metadata_log: metadata_log}) do
    %{data: data(metadata_log)}
  end

  defp data(%MetadataLog{} = metadata_log) do
    %{
      pk_id: metadata_log.pk_id,
      path: metadata_log.path,
      processed: metadata_log.processed,
      process_fail: metadata_log.process_fail,
      created_on: metadata_log.created_on
    }
  end
end
