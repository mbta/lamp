defmodule Api.PerformanceManagerTest do
  use Api.DataCase

  alias Api.Repo
  alias Api.PerformanceManager.MetadataLog
  alias Api.PerformanceManager.StaticCalendar
  alias Api.PerformanceManager.StaticCalendarDates
  alias Api.PerformanceManager.StaticFeedInfo
  alias Api.PerformanceManager.StaticRoutes
  alias Api.PerformanceManager.StaticStopTimes
  alias Api.PerformanceManager.StaticStops
  alias Api.PerformanceManager.StaticTrips
  alias Api.PerformanceManager.VehicleEventMetrics
  alias Api.PerformanceManager.VehicleEvents
  alias Api.PerformanceManager.VehicleTrips

  describe "metadata log table" do
    test "metadata log schema formatted correctly" do
      assert Repo.all(MetadataLog) == []
    end
  end

  describe "realtime tables" do
    test "list vehicle event_metrics schema formatted correctly" do
      assert Repo.all(VehicleEventMetrics) == []
    end

    test "vehicle events schema formatted correctly" do
      assert Repo.all(VehicleEvents) == []
    end

    test "vehicle trips schema formatted correctly" do
      assert Repo.all(VehicleTrips) == []
    end
  end

  describe "static tables" do
    test "static calendar dates schema formatted correctly" do
      assert Repo.all(StaticCalendarDates) == []
    end

    test "static calendar schema formatted correctly" do
      assert Repo.all(StaticCalendar) == []
    end

    test "static feed info schema formatted correctly" do
      assert Repo.all(StaticFeedInfo) == []
    end

    test "static routes schema formatted correctly" do
      assert Repo.all(StaticRoutes) == []
    end

    test "static stop times schema formatted correctly" do
      assert Repo.all(StaticStopTimes) == []
    end

    test "static stops schema formatted correctly" do
      assert Repo.all(StaticStops) == []
    end

    test "static trips schema formatted correctly" do
      assert Repo.all(StaticTrips) == []
    end
  end
end
