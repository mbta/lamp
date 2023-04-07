defmodule ApiWeb.MetadataLogControllerTest do
  use ApiWeb.ConnCase

  setup %{conn: conn} do
    {:ok, conn: put_req_header(conn, "accept", "application/json")}
  end

  describe "index" do
    test "lists all metadata_log", %{conn: conn} do
      conn = get(conn, ~p"/api/metadata_log")
      data = json_response(conn, 200)["data"]
      assert Enum.count(data) == 101
    end
  end
end
