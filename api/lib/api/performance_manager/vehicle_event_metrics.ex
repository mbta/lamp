defmodule Api.PerformanceManager.VehicleEventMetrics do
  use Ecto.Schema

  @primary_key false
  schema "vehicle_event_metrics" do
    field(:trip_stop_hash, :binary)

    field(:travel_time_seconds, :integer)
    field(:dwell_time_seconds, :integer)
    field(:headway_trunk_seconds, :integer)
    field(:headway_branch_seconds, :integer)

    field(:updated_on, :utc_datetime)
  end
end
