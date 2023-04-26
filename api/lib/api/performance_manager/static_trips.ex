defmodule Api.PerformanceManager.StaticTrips do
  use Ecto.Schema

  @primary_key {:pk_id, :id, autogenerate: true}
  schema "static_trips" do
    field(:route_id, :string)
    field(:branch_route_id, :string)
    field(:trunk_route_id, :string)
    field(:service_id, :string)
    field(:trip_id, :string)
    field(:direction_id, :boolean)
    field(:timestamp, :integer)
  end
end
