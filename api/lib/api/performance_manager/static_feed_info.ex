defmodule Api.PerformanceManager.StaticFeedInfo do
  use Ecto.Schema

  @primary_key {:pk_id, :id, autogenerate: true}
  schema "static_feed_info" do
    field(:feed_start_date, :integer)
    field(:feed_end_date, :integer)
    field(:feed_version, :string)
    field(:feed_active_date, :integer)
    field(:timestamp, :integer)
    field(:created_on, :utc_datetime)
  end
end
