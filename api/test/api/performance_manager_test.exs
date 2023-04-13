defmodule Api.PerformanceManagerTest do
  use Api.DataCase

  alias Api.PerformanceManager

  describe "metadata_log" do
    import Api.PerformanceManagerFixtures

    test "list_metadata_log/0 returns all metadata_log" do
      all_metadata = PerformanceManager.list_metadata_log()
      assert Enum.count(all_metadata) == 101
    end

    test "get_metadata_log!/1 returns the metadata_log with given id" do
      first_entry = PerformanceManager.get_metadata_log!(1)

      assert first_entry.processed == false

      assert first_entry.path ==
               "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=0/6e247daf3469436aa574f93fa4cb7c48-0.parquet"
    end
  end
end
