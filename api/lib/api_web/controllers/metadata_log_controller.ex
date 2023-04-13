defmodule ApiWeb.MetadataLogController do
  use ApiWeb, :controller

  alias Api.PerformanceManager
  alias Api.PerformanceManager.MetadataLog

  action_fallback ApiWeb.FallbackController

  def index(conn, _params) do
    metadata_log = PerformanceManager.list_metadata_log()
    render(conn, :index, metadata_log: metadata_log)
  end

  def show(conn, %{"pk_id" => pk_id}) do
    metadata_log = PerformanceManager.get_metadata_log!(pk_id)
    render(conn, :show, metadata_log: metadata_log)
  end
end
