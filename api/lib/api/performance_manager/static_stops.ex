defmodule Api.PerformanceManager.StaticStops do
  use Ecto.Schema

  @primary_key {:pk_id, :id, autogenerate: true}
  schema "static_stops" do
    field(:stop_id, :string)
    field(:stop_name, :string)
    field(:stop_desc, :string)
    field(:platform_code, :string)
    field(:platform_name, :string)
    field(:parent_station, :string)
    field(:static_version_key, :integer)
  end
end
