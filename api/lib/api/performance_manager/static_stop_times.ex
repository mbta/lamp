defmodule Api.PerformanceManager.StaticStopTimes do
  use Ecto.Schema

  @primary_key {:pk_id, :id, autogenerate: true}
  schema "static_stop_times" do
    field(:trip_id, :string)
    field(:arrival_time, :integer)
    field(:departure_time, :integer)
    field(:stop_id, :string)
    field(:stop_sequence, :integer)
    field(:timestamp, :integer)
  end
end
