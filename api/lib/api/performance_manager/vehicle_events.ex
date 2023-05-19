defmodule Api.PerformanceManager.VehicleEvents do
  use Ecto.Schema

  @primary_key {:pk_id, :id, autogenerate: true}
  schema "vehicle_events" do
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

    field(:updated_on, :utc_datetime)
  end
end
