defmodule Api.PerformanceManager.StaticCalendarDates do
  use Ecto.Schema

  @primary_key {:pk_id, :id, autogenerate: true}
  schema "static_calendar_dates" do
    field(:service_id, :string)
    field(:date, :integer)
    field(:exception_type, :integer)
    field(:holiday_name, :string)
    field(:static_version_key, :integer)
  end
end
