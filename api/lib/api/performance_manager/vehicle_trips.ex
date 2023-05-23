defmodule Api.PerformanceManager.VehicleTrips do
  use Ecto.Schema

  @primary_key false
  schema "vehicle_trips" do
    # trip identifiers
    field(:direction_id, :boolean)
    field(:route_id, :string)
    field(:trunk_route_id, :string)
    field(:branch_route_id, :string)
    field(:service_date, :integer)
    field(:start_time, :integer)
    field(:vehicle_id, :string)
    field(:stop_count, :integer)
    field(:trip_id, :string)

    field(:trip_hash, :binary)
    field(:vehicle_label, :string)
    field(:vehicle_consist, :string)
    field(:direction, :string)
    field(:direction_destination, :string)

    # static trip matching
    field(:static_trip_id_guess, :string)
    field(:static_start_time, :integer)
    field(:static_stop_count, :integer)
    field(:first_last_station_match, :boolean)

    # foreign key to static schedule expected values
    belongs_to(:static_feed_info, Api.PerformanceManager.StaticFeedInfo,
      foreign_key: :static_version_key,
      references: :static_version_key,
      define_field: false
    )

    field(:updated_on, :utc_datetime)
  end
end
