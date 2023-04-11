defmodule Api.PerformanceManager.VehicleEvents do
  use Ecto.Schema

  @primary_key {:pk_id, :id, autogenerate: true}
  schema "vehicle_events" do
    # trip identifiers
    field(:direction_id, :boolean)
    field(:route_id, :string)
    field(:start_date, :integer)
    field(:start_time, :integer)
    field(:vehicle_id, :string)

    # hash of trip identifiers
    field(:trip_hash, :binary)

    # stop identifiers
    field(:stop_sequence, :integer)
    field(:stop_id, :string)
    field(:parent_station, :string)

    # hash of trip and stop identifiers
    field(:trip_stop_hash, :binary)

    # event timestamps used for metrics
    field(:vp_move_timestamp, :integer)
    field(:vp_stop_timestamp, :integer)
    field(:tu_stop_timestamp, :integer)

    # foreign key to static schedule expected values
    belongs_to(:static_feed_info, Api.PerformanceManager.StaticFeedInfo,
      foreign_key: :fk_static_timestamp,
      references: :timestamp,
      define_field: false
    )

    field(:updated_on, :utc_datetime)
  end
end
