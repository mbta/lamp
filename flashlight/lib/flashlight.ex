defmodule Flashlight do
  require Explorer
  require Benchee

  alias Explorer.DataFrame
  alias Explorer.Series

  alias ExAws.S3

  @moduledoc """
  """

  @doc """
  """
  @springboard_bucket "mbta-ctd-dataplatform-dev-springboard"
  @old_date ~N[2023-09-22 16:01:01]
  @old_run_id "126-1024"

  @busloc_columns [
    :run_id,
    :block_id,
    :first_name,
    :last_name,
    :operator_id,
    :logon_time,
    :vehicle_timestamp,
    :vehicle_id
  ]
  @vp_columns [
    :current_stop_sequence,
    :stop_id,
    :current_status,
    :vehicle_timestamp,
    :route_id,
    :trip_id,
    :vehicle_id
  ]

  def tasks(datetime, count) do
    tasks =
      for filename <- get_s3_filenames(datetime, 0, "BUS_VEHICLE_POSITIONS") do
        Task.async(fn -> download_parquet(filename) end)
      end

    tasks_with_results = Task.yield_many(tasks, 100_000_000)

    results =
      Enum.map(tasks_with_results, fn {task, res} ->
        res || Task.shutdown(task, :brutal_kill)
      end)

    bus_data =
      Enum.map(results, fn {:ok, fp} -> busloc_from_parquet(fp) end)
      |> DataFrame.concat_rows()

    # get the vehicle position data for the vehicle ids on the run
    # vp_data =
    # pull_vp_info(())
    # |> DataFrame.filter_with(
    # &Explorer.Series.in(&1[:vehicle_id], Series.unordered_distinct(bus_data[:vehicle_id]))
    # )

    # DataFrame.join(bus_data, vp_data)
  end

  def multiple_hours(date, start_hour, end_hour) do
    Enum.to_list(start_hour..end_hour)
    |> Enum.map(&extract_data_download(date, &1))
    |> DataFrame.concat_rows()
    |> DataFrame.distinct()
    |> DataFrame.to_rows()
  end

  def extract_data_download(date, hour) do
    busloc_dataframe =
      get_s3_filenames(date, hour, "BUS_VEHICLE_POSITIONS")
      |> Enum.map(&download_parquet/1)
      |> Enum.map(&busloc_from_parquet/1)
      |> DataFrame.concat_rows()
      # |> IO.inspect(label: "busloc")
      |> DataFrame.filter_with(&Series.equal(&1[:run_id], "123-3097"))

    vp_dataframe =
      get_s3_filenames(date, hour, "RT_VEHICLE_POSITIONS")
      |> Enum.map(&download_parquet/1)
      |> Enum.map(&vp_from_parquet/1)
      |> DataFrame.concat_rows()
      # |> IO.inspect(label: "vp")
      |> DataFrame.filter_with(
        &Explorer.Series.in(
          &1[:vehicle_id],
          Series.unordered_distinct(busloc_dataframe[:vehicle_id])
        )
      )

    DataFrame.join(busloc_dataframe, vp_dataframe)
  end

  def extract_data_get(date, hour) do
    busloc_dataframe =
      get_s3_filenames(date, hour, "BUS_VEHICLE_POSITIONS")
      |> Enum.map(&get_parquet_object/1)
      |> Enum.map(&busloc_from_memory/1)
      |> DataFrame.concat_rows()
      # |> IO.inspect(label: "busloc")
      |> DataFrame.filter_with(&Series.equal(&1[:run_id], "123-3097"))

    vp_dataframe =
      get_s3_filenames(date, hour, "RT_VEHICLE_POSITIONS")
      |> Enum.map(&get_parquet_object/1)
      |> Enum.map(&vp_from_memory/1)
      |> DataFrame.concat_rows()
      # |> IO.inspect(label: "vp")
      |> DataFrame.filter_with(
        &Explorer.Series.in(
          &1[:vehicle_id],
          Series.unordered_distinct(busloc_dataframe[:vehicle_id])
        )
      )

    DataFrame.join(busloc_dataframe, vp_dataframe)
  end

  def run_benchmarks() do
    date = ~D[2023-09-25]

    Benchee.run(
      %{
        "download" => fn hour -> extract_data_download(date, hour) end,
        "get" => fn hour -> extract_data_get(date, hour) end
      },
      inputs: %{
        "zero" => 0,
        "six" => 6,
        "twelve" => 12,
        "eighteen" => 18
      },
      warmup: 0,
      time: 60,
      memory_time: 2
    )
  end

  def busloc_from_parquet(filepath) do
    DataFrame.from_parquet!(
      filepath,
      columns: @busloc_columns
    )
    |> DataFrame.distinct()
    |> DataFrame.drop_nil([:run_id, :vehicle_timestamp, :vehicle_id])
  end

  def busloc_from_memory(binary) do
    DataFrame.load_parquet!(
      binary,
      columns: @busloc_columns
    )
    |> DataFrame.distinct()
    |> DataFrame.drop_nil([:run_id, :vehicle_timestamp, :vehicle_id])
  end

  def vp_from_parquet(filepath) do
    DataFrame.from_parquet!(
      filepath,
      columns: @vp_columns
    )
    |> DataFrame.distinct()
    |> DataFrame.drop_nil([:vehicle_timestamp, :vehicle_id])
  end

  def vp_from_memory(binary) do
    DataFrame.load_parquet!(
      binary,
      columns: @vp_columns
    )
    |> DataFrame.distinct()
    |> DataFrame.drop_nil([:vehicle_timestamp, :vehicle_id])
  end

  def get_parquet_object(filename) do
    {:ok, %{body: body, status_code: 200}} =
      S3.get_object(
        @springboard_bucket,
        filename
      )
      |> ExAws.request()

    body
  end

  def download_parquet(filename) do
    S3.download_file(
      @springboard_bucket,
      filename,
      save_file_path(filename)
    )
    # |> IO.inspect(label: "download request")
    |> ExAws.request()

    save_file_path(filename)
  end

  def get_s3_filenames(date, hour, gtfs_rt_type) do
    result =
      S3.list_objects(
        @springboard_bucket,
        prefix: generate_s3_path(date, hour, gtfs_rt_type)
      )
      |> ExAws.request()

    case result do
      {:ok, data} ->
        for item <- data[:body][:contents], item[:key] =~ ~r/.parquet/, do: item[:key]

      {:error, reason} ->
        nil
    end
  end

  def save_file_path(s3_path) do
    path =
      Path.join([
        "/Users",
        "mzappitello",
        "Desktop",
        "demo",
        s3_path
      ])

    File.mkdir_p!(Path.dirname(path))
    path
  end

  def generate_s3_path(date, hour, gtfs_rt_type) do
    Path.join([
      "lamp",
      "sorted",
      gtfs_rt_type,
      "year=" <> Integer.to_string(date.year),
      "month=" <> Integer.to_string(date.month),
      "day=" <> Integer.to_string(date.day),
      "hour=" <> Integer.to_string(hour)
    ]) <> "/"

    # |> IO.inspect(label: "s3_prefix")
  end
end
