defmodule Api.PerformanceManager.StaticCalendar do
  use Ecto.Schema

  @primary_key {:pk_id, :id, autogenerate: true}
  schema "static_calendar" do
    field(:service_id, :string)
    field(:monday, :boolean)
    field(:tuesday, :boolean)
    field(:wednesday, :boolean)
    field(:thursday, :boolean)
    field(:friday, :boolean)
    field(:saturday, :boolean)
    field(:sunday, :boolean)
    field(:start_date, :integer)
    field(:end_date, :integer)
    field(:timestamp, :integer)
  end
end
