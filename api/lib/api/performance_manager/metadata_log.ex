defmodule Api.PerformanceManager.MetadataLog do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:pk_id, :id, autogenerate: true}
  schema "metadata_log" do
    field :processed, :boolean, default: false
    field :process_fail, :boolean, default: false
    field :path, :string
    field :created_on, :utc_datetime
  end

  @doc false
  def changeset(metadata_log, attrs) do
    metadata_log
    |> cast(attrs, [])
    |> validate_required([])
  end
end
