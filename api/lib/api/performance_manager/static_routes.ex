defmodule Api.PerformanceManager.StaticRoutes do
  use Ecto.Schema

  @primary_key {:pk_id, :id, autogenerate: true}
  schema "static_routes" do
    field(:route_id, :string)
    field(:agency_id, :integer)
    field(:route_short_name, :string)
    field(:route_long_name, :string)
    field(:route_desc, :string)
    field(:route_type, :integer)
    field(:route_sort_order, :integer)
    field(:route_fare_class, :string)
    field(:line_id, :string)
    field(:static_version_key, :integer)
  end
end
