defmodule Api.PerformanceManager.VehicleTrips do
  use Ecto.Schema

  @primary_key false
  schema "vehicle_trips" do
    # trip identifiers
    field(:direction_id, :boolean)
    field(:route_id, :string)
    field(:trunk_route_id, :string)
    field(:start_date, :integer)
    field(:start_time, :integer)
    field(:vehicle_id, :string)
    field(:stop_count, :integer)

    field(:trip_hash, :binary)

    # static trip matching
    field(:static_trip_id_guess, :string)
    field(:static_start_time, :integer)
    field(:static_stop_count, :integer)
    field(:first_last_station_match, :boolean)

    # foreign key to static schedule expected values
    belongs_to(:static_feed_info, Api.PerformanceManager.StaticFeedInfo,
      foreign_key: :fk_static_timestamp,
      references: :timestamp,
      define_field: false
    )

    field(:updated_on, :utc_datetime)
  end
end
