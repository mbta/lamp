import os

import pyarrow
import pyarrow.parquet as pq
import sqlalchemy as sa

from lamp_py.tableau.hyper import HyperJob


class HyperGTFS(HyperJob):
    """Base Class for GTFS Hyper Jobs"""

    def __init__(
        self,
        gtfs_table_name: str,
        table_query: str,
    ) -> None:
        HyperJob.__init__(
            self,
            hyper_file_name=f"LAMP_GTFS_Rail_{gtfs_table_name}.hyper",
            remote_parquet_path=f"s3://{os.getenv('PUBLIC_BUCKET')}/lamp/tableau/rail/LAMP_GTFS_Rail_{gtfs_table_name}.parquet",
        )
        self.gtfs_table_name = gtfs_table_name
        self.create_query = table_query % ""
        self.update_query = table_query

    @property
    def parquet_schema(self) -> pyarrow.schema:
        """Define GTFS Table Schema"""

    def create_parquet(self) -> None:
        if os.path.exists(self.local_parquet_path):
            os.remove(self.local_parquet_path)

        pq.write_table(
            pyarrow.Table.from_pylist(
                mapping=self.db_manager.select_as_list(
                    sa.text(self.create_query)
                ),
                schema=self.parquet_schema,
            ),
            self.local_parquet_path,
        )

    def update_parquet(self) -> bool:
        self.download_parquet()

        max_stats = self.max_stats_of_parquet()

        max_parquet_key = max_stats["static_version_key"]

        max_key_query = (
            f"SELECT MAX(static_version_key) FROM {self.gtfs_table_name};"
        )

        max_db_key = self.db_manager.select_as_list(sa.text(max_key_query))[0][
            "max"
        ]

        # no update needed
        if max_db_key <= max_parquet_key:
            return False

        # add WHERE clause to UPDATE query
        update_query = self.update_query % (
            f" WHERE static_version_key > {max_parquet_key} ",
        )

        pq.write_table(
            pyarrow.concat_tables(
                [
                    pq.read_table(self.local_parquet_path),
                    pyarrow.Table.from_pylist(
                        mapping=self.db_manager.select_as_list(
                            sa.text(update_query)
                        ),
                        schema=self.parquet_schema,
                    ),
                ]
            ),
            self.local_parquet_path,
        )

        return True


class HyperServiceIdByRoute(HyperGTFS):
    """Hyper Job for service_id_by_date_and_route VIEW"""

    def __init__(self) -> None:
        HyperGTFS.__init__(
            self,
            gtfs_table_name="service_id_by_date_and_route",
            table_query=(
                "SELECT "
                "   route_id"
                "   ,service_id"
                "   ,date(service_date::text) as service_date"
                "   ,static_version_key "
                "FROM service_id_by_date_and_route "
                "%s"
                "ORDER BY static_version_key, service_id, service_date"
            ),
        )

    @property
    def parquet_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("route_id", pyarrow.string()),
                ("service_id", pyarrow.string()),
                ("service_date", pyarrow.date32()),
                ("static_version_key", pyarrow.int64()),
            ]
        )


class HyperStaticTrips(HyperGTFS):
    """Hyper Job for static_trips table"""

    def __init__(self) -> None:
        HyperGTFS.__init__(
            self,
            gtfs_table_name="static_trips",
            table_query=(
                "SELECT "
                "   pk_id"
                "   ,route_id"
                "   ,branch_route_id"
                "   ,trunk_route_id "
                "   ,service_id"
                "   ,trip_id"
                "   ,direction_id::int"
                "   ,block_id"
                "   ,static_version_key "
                "FROM static_trips "
                "%s"
                "ORDER BY static_version_key, direction_id, route_id, service_id;"
            ),
        )

    @property
    def parquet_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("pk_id", pyarrow.int64()),
                ("route_id", pyarrow.string()),
                ("branch_route_id", pyarrow.string()),
                ("trunk_route_id", pyarrow.string()),
                ("service_id", pyarrow.string()),
                ("trip_id", pyarrow.string()),
                ("direction_id", pyarrow.int8()),
                ("block_id", pyarrow.string()),
                ("static_version_key", pyarrow.int64()),
            ]
        )


class HyperStaticStops(HyperGTFS):
    """Hyper Job for static_stops table"""

    def __init__(self) -> None:
        HyperGTFS.__init__(
            self,
            gtfs_table_name="static_stops",
            table_query=(
                "SELECT "
                "   pk_id"
                "   ,stop_id"
                "   ,stop_name"
                "   ,stop_desc "
                "   ,platform_code"
                "   ,platform_name"
                "   ,parent_station"
                "   ,static_version_key "
                "FROM static_stops "
                "%s"
                "ORDER BY static_version_key, parent_station;"
            ),
        )

    @property
    def parquet_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("pk_id", pyarrow.int64()),
                ("stop_id", pyarrow.string()),
                ("stop_name", pyarrow.string()),
                ("stop_desc", pyarrow.string()),
                ("platform_code", pyarrow.string()),
                ("platform_name", pyarrow.string()),
                ("parent_station", pyarrow.string()),
                ("static_version_key", pyarrow.int64()),
            ]
        )


class HyperStaticCalendar(HyperGTFS):
    """Hyper Job for static_calendar table"""

    def __init__(self) -> None:
        HyperGTFS.__init__(
            self,
            gtfs_table_name="static_calendar",
            table_query=(
                "SELECT "
                "   pk_id"
                "   ,service_id"
                "   ,monday"
                "   ,tuesday"
                "   ,wednesday"
                "   ,thursday"
                "   ,friday"
                "   ,saturday"
                "   ,sunday"
                "   ,date(start_date::text) as start_date"
                "   ,date(end_date::text) as end_date"
                "   ,static_version_key "
                "FROM static_calendar "
                "%s"
                "ORDER BY static_version_key, start_date, end_date;"
            ),
        )

    @property
    def parquet_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("pk_id", pyarrow.int64()),
                ("service_id", pyarrow.string()),
                ("monday", pyarrow.bool_()),
                ("tuesday", pyarrow.bool_()),
                ("wednesday", pyarrow.bool_()),
                ("thursday", pyarrow.bool_()),
                ("friday", pyarrow.bool_()),
                ("saturday", pyarrow.bool_()),
                ("sunday", pyarrow.bool_()),
                ("start_date", pyarrow.date32()),
                ("end_date", pyarrow.date32()),
                ("static_version_key", pyarrow.int64()),
            ]
        )


class HyperStaticCalendarDates(HyperGTFS):
    """Hyper Job for static_calendar_dates table"""

    def __init__(self) -> None:
        HyperGTFS.__init__(
            self,
            gtfs_table_name="static_calendar_dates",
            table_query=(
                "SELECT "
                "   pk_id"
                "   ,service_id"
                "   ,date(date::text) as date"
                "   ,exception_type"
                "   ,holiday_name"
                "   ,static_version_key "
                "FROM static_calendar_dates "
                "%s"
                "ORDER BY static_version_key, service_id, date;"
            ),
        )

    @property
    def parquet_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("pk_id", pyarrow.int64()),
                ("service_id", pyarrow.string()),
                ("date", pyarrow.date32()),
                ("exception_type", pyarrow.int8()),
                ("holiday_name", pyarrow.string()),
                ("static_version_key", pyarrow.int64()),
            ]
        )


class HyperStaticRoutes(HyperGTFS):
    """Hyper Job for static_routes table"""

    def __init__(self) -> None:
        HyperGTFS.__init__(
            self,
            gtfs_table_name="static_routes",
            table_query=(
                "SELECT "
                "   pk_id"
                "   ,route_id"
                "   ,agency_id"
                "   ,route_short_name"
                "   ,route_long_name"
                "   ,route_desc"
                "   ,route_type"
                "   ,route_sort_order"
                "   ,route_fare_class"
                "   ,line_id"
                "   ,static_version_key "
                "FROM static_routes "
                "%s"
                "ORDER BY static_version_key, route_id, route_type;"
            ),
        )

    @property
    def parquet_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("pk_id", pyarrow.int64()),
                ("route_id", pyarrow.string()),
                ("agency_id", pyarrow.int8()),
                ("route_short_name", pyarrow.string()),
                ("route_long_name", pyarrow.string()),
                ("route_desc", pyarrow.string()),
                ("route_type", pyarrow.int8()),
                ("route_sort_order", pyarrow.int32()),
                ("route_fare_class", pyarrow.string()),
                ("line_id", pyarrow.string()),
                ("static_version_key", pyarrow.int64()),
            ]
        )


class HyperStaticFeedInfo(HyperGTFS):
    """Hyper Job for static_feed_info table"""

    def __init__(self) -> None:
        HyperGTFS.__init__(
            self,
            gtfs_table_name="static_feed_info",
            table_query=(
                "SELECT "
                "   pk_id"
                "   ,date(feed_start_date::text) as feed_start_date"
                "   ,date(feed_end_date::text) as feed_end_date"
                "   ,feed_version"
                "   ,date(feed_active_date::text) as feed_active_date"
                "   ,static_version_key "
                "FROM static_feed_info "
                "%s"
                ";"
            ),
        )

    @property
    def parquet_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("pk_id", pyarrow.int64()),
                ("feed_start_date", pyarrow.date32()),
                ("feed_end_date", pyarrow.date32()),
                ("feed_version", pyarrow.string()),
                ("feed_active_date", pyarrow.date32()),
                ("static_version_key", pyarrow.int64()),
            ]
        )


class HyperStaticStopTimes(HyperGTFS):
    """Hyper Job for static_stop_times table"""

    def __init__(self) -> None:
        HyperGTFS.__init__(
            self,
            gtfs_table_name="static_stop_times",
            table_query=(
                "SELECT "
                "   pk_id"
                "   ,trip_id"
                "   ,arrival_time"
                "   ,departure_time"
                "   ,schedule_travel_time_seconds"
                "   ,schedule_headway_trunk_seconds"
                "   ,schedule_headway_branch_seconds"
                "   ,stop_id"
                "   ,stop_sequence"
                "   ,static_version_key "
                "FROM static_stop_times "
                "%s"
                "ORDER BY static_version_key, trip_id;"
            ),
        )

    @property
    def parquet_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("pk_id", pyarrow.int64()),
                ("trip_id", pyarrow.string()),
                ("arrival_time", pyarrow.int32()),
                ("departure_time", pyarrow.int32()),
                ("schedule_travel_time_seconds", pyarrow.int32()),
                ("schedule_headway_trunk_seconds", pyarrow.int32()),
                ("schedule_headway_branch_seconds", pyarrow.int32()),
                ("stop_id", pyarrow.string()),
                ("stop_sequence", pyarrow.int16()),
                ("static_version_key", pyarrow.int64()),
            ]
        )
