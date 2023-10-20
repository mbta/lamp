import os

import pyarrow
import pyarrow.parquet as pq
import sqlalchemy as sa

from lamp_py.tableau.hyper import HyperJob


class HyperRtRail(HyperJob):
    """HyperJob for LAMP RT Rail data"""

    def __init__(self) -> None:
        HyperJob.__init__(
            self,
            hyper_file_name="LAMP_RT_Rail.hyper",
            remote_parquet_path=f"s3://{os.getenv('PUBLIC_BUCKET')}/lamp/tableau/rail/LAMP_RT_Rail.parquet",
        )
        self.table_query = (
            "SELECT"
            "   date(vt.service_date::text) as service_date"
            "   , ve.pm_trip_id"
            "   , ve.stop_sequence"
            "   , ve.canonical_stop_sequence"
            "   , prev_ve.canonical_stop_sequence as previous_canonical_stop_sequence"
            "   , ve.sync_stop_sequence"
            "   , prev_ve.sync_stop_sequence as previous_sync_stop_sequence"
            "   , ve.stop_id"
            "   , prev_ve.stop_id as previous_stop_id"
            "   , ve.parent_station"
            "   , prev_ve.parent_station as previous_parent_station"
            "   , ve.vp_move_timestamp as previous_stop_departure_timestamp"
            "   , COALESCE(ve.vp_stop_timestamp,  ve.tu_stop_timestamp) as stop_arrival_timestamp"
            "   , COALESCE(ve.vp_stop_timestamp,  ve.tu_stop_timestamp) + ve.dwell_time_seconds as stop_departure_timestamp"
            "   , vt.direction_id::int"
            "   , vt.route_id"
            "   , vt.branch_route_id"
            "   , vt.trunk_route_id"
            "   , vt.start_time"
            "   , vt.vehicle_id"
            "   , vt.stop_count"
            "   , vt.trip_id"
            "   , vt.vehicle_label"
            "   , vt.vehicle_consist"
            "   , vt.direction"
            "   , vt.direction_destination"
            "   , vt.static_trip_id_guess"
            "   , vt.static_start_time"
            "   , vt.static_stop_count"
            "   , vt.first_last_station_match"
            "   , vt.static_version_key"
            "   , ve.travel_time_seconds"
            "   , ve.dwell_time_seconds"
            "   , ve.headway_trunk_seconds"
            "   , ve.headway_branch_seconds"
            "   , ve.updated_on "
            "FROM "
            "   vehicle_events ve "
            "LEFT JOIN "
            "   vehicle_trips vt "
            "ON "
            "   ve.pm_trip_id = vt.pm_trip_id "
            "LEFT JOIN "
            "   vehicle_events prev_ve "
            "ON "
            "   ve.pm_event_id = prev_ve.next_trip_stop_pm_event_id "
            "WHERE "
            "   ve.previous_trip_stop_pm_event_id is not NULL "
            "   AND ( "
            "       ve.vp_stop_timestamp IS NOT null "
            "       OR ve.vp_move_timestamp IS NOT null "
            "   ) "
            "   %s"
            "ORDER BY "
            "   ve.service_date, vt.route_id, vt.direction_id, vt.vehicle_id"
            ";"
        )

    @property
    def parquet_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("service_date", pyarrow.date32()),
                ("pm_trip_id", pyarrow.int64()),
                ("stop_sequence", pyarrow.int16()),
                ("canonical_stop_sequence", pyarrow.int16()),
                ("previous_canonical_stop_sequence", pyarrow.int16()),
                ("sync_stop_sequence", pyarrow.int16()),
                ("previous_sync_stop_sequence", pyarrow.int16()),
                ("stop_id", pyarrow.string()),
                ("previous_stop_id", pyarrow.string()),
                ("parent_station", pyarrow.string()),
                ("previous_parent_station", pyarrow.string()),
                ("previous_stop_departure_timestamp", pyarrow.int64()),
                ("stop_arrival_timestamp", pyarrow.int64()),
                ("stop_departure_timestamp", pyarrow.int64()),
                ("direction_id", pyarrow.int8()),
                ("route_id", pyarrow.string()),
                ("branch_route_id", pyarrow.string()),
                ("trunk_route_id", pyarrow.string()),
                ("start_time", pyarrow.int32()),
                ("vehicle_id", pyarrow.string()),
                ("stop_count", pyarrow.int16()),
                ("trip_id", pyarrow.string()),
                ("vehicle_label", pyarrow.string()),
                ("vehicle_consist", pyarrow.string()),
                ("direction", pyarrow.string()),
                ("direction_destination", pyarrow.string()),
                ("static_trip_id_guess", pyarrow.string()),
                ("static_start_time", pyarrow.int64()),
                ("static_stop_count", pyarrow.int64()),
                ("first_last_station_match", pyarrow.bool_()),
                ("static_version_key", pyarrow.int64()),
                ("travel_time_seconds", pyarrow.int16()),
                ("dwell_time_seconds", pyarrow.int16()),
                ("headway_trunk_seconds", pyarrow.int16()),
                ("headway_branch_seconds", pyarrow.int16()),
                ("updated_on", pyarrow.timestamp("us")),
            ]
        )

    def create_parquet(self) -> None:
        create_query = self.table_query % ""

        if os.path.exists(self.local_parquet_path):
            os.remove(self.local_parquet_path)

        pq.write_table(
            pyarrow.Table.from_pylist(
                mapping=self.db_manager.select_as_list(sa.text(create_query)),
                schema=self.parquet_schema,
            ),
            self.local_parquet_path,
        )

    def update_parquet(self) -> bool:
        self.download_parquet()

        max_stats = self.max_stats_of_parquet()

        max_start_date = max_stats["service_date"]

        update_query = self.table_query % (
            f" AND vt.service_date >= {max_start_date} ",
        )

        pq.write_table(
            pyarrow.concat_tables(
                [
                    pq.read_table(
                        self.local_parquet_path,
                        filters=[("service_date", "<", max_start_date)],
                    ),
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
