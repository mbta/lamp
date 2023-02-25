
import sqlalchemy as sa
from typing import Dict, List
import logging

from .logging_utils import ProcessLogger
from .postgres_utils import DatabaseManager
from .postgres_schema import (
    VehicleEvents,
    VehicleTrips,
    VehicleEventMetrics,
)
from .l1_cte_statements import (
    get_rt_trips_cte,
    get_static_trips_cte,
)

def get_seed_data(db_manager: DatabaseManager) -> Dict[int, List[int]]:
    """
    get seed data for populating trip and metrics tables

    return: Dict[start_date:int : List[fk_static_timestamps]]
    """
    last_metric_update = (
        sa.select(
            sa.func.coalesce(
                    sa.func.max(VehicleTrips.updated_on),
                    sa.func.to_timestamp(0),
            )
        ).scalar_subquery()
    )

    seed_query = (
        sa.select(
            VehicleEvents.start_date,
            VehicleEvents.fk_static_timestamp,
        ).distinct(
            VehicleEvents.start_date,
            VehicleEvents.fk_static_timestamp,
        ).where(
            VehicleEvents.updated_on > last_metric_update
        ).order_by(
            VehicleEvents.start_date
        ).limit(100)
    )

    # returns empty list if no records to process
    seed_data = db_manager.select_as_list(seed_query)

    seed_data_return: Dict[int, List[int]] = {}
    for record in seed_data:
        start_date = record.get("start_date")
        timestamp = record.get("fk_static_timestamp")

        if start_date not in seed_data_return:
            seed_data_return[start_date] = []
        seed_data_return[start_date].append(timestamp)

    return seed_data_return

def process_trips_tables(db_manager: DatabaseManager, seed_data: Dict[int, List[int]]) -> None:
    """
    process updates to multiple trips tables
    """

    MAX_TABLES_TO_PROCESS = 3

    process_logger = ProcessLogger("l1_rt_trips_tables_loader", seed_data_size=len(seed_data))
    process_logger.log_start()

    tables_processed=0
    for seed_start_date in sorted(seed_data.keys())[:MAX_TABLES_TO_PROCESS]:
        seed_timestamps = seed_data[seed_start_date]
        process_trips_table(db_manager, seed_start_date, seed_timestamps)
        tables_processed += 1

    process_logger.add_metadata(tables_processed=tables_processed)
    process_logger.log_complete()


def process_trips_table(db_manager: DatabaseManager, seed_start_date: int, seed_timestamps: List[int]) -> None:
    """
    processs updates to trips table

    will only process one days worth of trips data per run, but can be modfied
    to process additional says provided in seed_data
    """
    process_logger = ProcessLogger("l1_rt_trips_table_loader")
    process_logger.log_start()

    static_trips_cte = get_static_trips_cte(seed_timestamps, seed_start_date)

    first_stop_static_cte = (
        sa.select(
            static_trips_cte.c.timestamp,
            static_trips_cte.c.static_trip_id,
            static_trips_cte.c.static_stop_timestamp.label("static_start_time"),
            static_trips_cte.c.parent_station.label("static_trip_first_stop"),
        ).select_from(
            static_trips_cte
        ).where(
            static_trips_cte.c.static_trip_first_stop == sa.true()
        ).cte(name="first_stop_static_cte")
    )

    static_trips_summary_cte = (
        sa.select(
            static_trips_cte.c.timestamp,
            static_trips_cte.c.static_trip_id,
            static_trips_cte.c.route_id,
            static_trips_cte.c.direction_id,
            first_stop_static_cte.c.static_start_time,
            first_stop_static_cte.c.static_trip_first_stop,
            static_trips_cte.c.parent_station.label("static_trip_last_stop"),
            static_trips_cte.c.static_trip_stop_rank.label("static_stop_count"),
        ).select_from(
            static_trips_cte
        ).join(
            first_stop_static_cte,
            sa.and_(
                static_trips_cte.c.timestamp == first_stop_static_cte.c.timestamp,
                static_trips_cte.c.static_trip_id == first_stop_static_cte.c.static_trip_id,
            )
        ).where(
            static_trips_cte.c.static_trip_last_stop == sa.true()
        ).cte(name="static_trips_summary_cte")
    )

    rt_trips_cte = get_rt_trips_cte(seed_start_date)

    first_stop_rt_cte = (
        sa.select(
            rt_trips_cte.c.trip_hash,
            rt_trips_cte.c.parent_station.label("rt_trip_first_stop"),
        ).select_from(
            rt_trips_cte
        ).where(
            rt_trips_cte.c.rt_trip_first_stop_flag == sa.true()
        )
    ).cte(name="first_stop_rt_cte")

    rt_trips_summary_cte = (
        sa.select(
            rt_trips_cte.c.fk_static_timestamp.label("timestamp"),
            rt_trips_cte.c.trip_hash,
            rt_trips_cte.c.direction_id,
            rt_trips_cte.c.route_id,
            rt_trips_cte.c.start_date,
            rt_trips_cte.c.start_time,
            rt_trips_cte.c.vehicle_id,
            first_stop_rt_cte.c.rt_trip_first_stop,
            rt_trips_cte.c.parent_station.label("rt_trip_last_stop"),
            rt_trips_cte.c.rt_trip_stop_rank.label("rt_stop_count")
        ).select_from(
            rt_trips_cte
        ).join(
            first_stop_rt_cte,
            rt_trips_cte.c.trip_hash == first_stop_rt_cte.c.trip_hash
        ).where(
            rt_trips_cte.c.rt_trip_last_stop_flag == sa.true()
        )
    ).cte(name="rt_trips_summary_cte")

    exact_trips_match = (
        sa.select(
            rt_trips_summary_cte.c.timestamp,
            rt_trips_summary_cte.c.trip_hash,
            rt_trips_summary_cte.c.direction_id,
            rt_trips_summary_cte.c.route_id,
            rt_trips_summary_cte.c.start_date,
            rt_trips_summary_cte.c.start_time,
            rt_trips_summary_cte.c.vehicle_id,
            rt_trips_summary_cte.c.rt_stop_count,
            static_trips_summary_cte.c.static_trip_id,
            static_trips_summary_cte.c.static_start_time,
            static_trips_summary_cte.c.static_stop_count,
            sa.literal(True).label("first_last_station_match"),
        ).distinct(
            rt_trips_summary_cte.c.trip_hash,
        ).select_from(
            rt_trips_summary_cte
        ).join(
            static_trips_summary_cte,
            sa.and_(
                rt_trips_summary_cte.c.timestamp == static_trips_summary_cte.c.timestamp,
                rt_trips_summary_cte.c.direction_id == static_trips_summary_cte.c.direction_id,
                rt_trips_summary_cte.c.route_id == static_trips_summary_cte.c.route_id,
                rt_trips_summary_cte.c.rt_trip_first_stop == static_trips_summary_cte.c.static_trip_first_stop,
                rt_trips_summary_cte.c.rt_trip_last_stop == static_trips_summary_cte.c.static_trip_last_stop,
                sa.func.abs(rt_trips_summary_cte.c.start_time - static_trips_summary_cte.c.static_start_time) < 3600,
            ),
            isouter=True
        ).order_by(
            rt_trips_summary_cte.c.trip_hash,
            sa.func.abs(rt_trips_summary_cte.c.start_time - static_trips_summary_cte.c.static_start_time),
        )
    ).cte(name="exact_trips_match")

    backup_trips_match = (
        sa.select(
            exact_trips_match.c.timestamp,
            exact_trips_match.c.trip_hash,
            exact_trips_match.c.direction_id,
            exact_trips_match.c.route_id,
            exact_trips_match.c.start_date,
            exact_trips_match.c.start_time,
            exact_trips_match.c.vehicle_id,
            exact_trips_match.c.rt_stop_count,
            static_trips_summary_cte.c.static_trip_id,
            static_trips_summary_cte.c.static_start_time,
            static_trips_summary_cte.c.static_stop_count,
            sa.literal(False).label("first_last_station_match"),
        ).distinct(
            exact_trips_match.c.trip_hash,
        ).select_from(
            exact_trips_match
        ).join(
            static_trips_summary_cte,
            sa.and_(
                exact_trips_match.c.timestamp == static_trips_summary_cte.c.timestamp,
                exact_trips_match.c.direction_id == static_trips_summary_cte.c.direction_id,
                exact_trips_match.c.route_id == static_trips_summary_cte.c.route_id,
            ),
        ).where(
            exact_trips_match.c.static_trip_id == None
        ).order_by(
            exact_trips_match.c.trip_hash,
            sa.func.abs(exact_trips_match.c.start_time - static_trips_summary_cte.c.static_start_time),
        )
    ).cte(name="backup_trips_match")

    all_new_trips = (
        sa.union(
            sa.select(exact_trips_match).where(exact_trips_match.c.static_trip_id != None),
            sa.select(backup_trips_match),
        )
    ).cte(name="all_new_trips")

    update_insert_query = (
        sa.select(
            all_new_trips.c.trip_hash,
            VehicleTrips.trip_hash.label("existing_trip_hash"),
            all_new_trips.c.direction_id,
            all_new_trips.c.route_id,
            all_new_trips.c.start_date,
            all_new_trips.c.start_time,
            all_new_trips.c.vehicle_id,
            all_new_trips.c.rt_stop_count.label("stop_count"),
            all_new_trips.c.static_trip_id.label("static_trip_id_guess"),
            all_new_trips.c.static_start_time,
            all_new_trips.c.static_stop_count,
            all_new_trips.c.first_last_station_match,
            sa.case(
                [
                    (
                        sa.or_(
                            all_new_trips.c.rt_stop_count != VehicleTrips.stop_count,
                            all_new_trips.c.static_trip_id != VehicleTrips.static_trip_id_guess,
                            all_new_trips.c.static_start_time != VehicleTrips.static_start_time,
                            all_new_trips.c.static_stop_count != VehicleTrips.static_stop_count,
                            all_new_trips.c.first_last_station_match != VehicleTrips.first_last_station_match,
                        ),
                        sa.literal(True)
                    )
                ],
                else_=sa.literal(False),
            ).label("to_update")
        ).select_from(
            all_new_trips
        ).join(
            VehicleTrips,
            sa.and_(
                all_new_trips.c.trip_hash == VehicleTrips.trip_hash,
                VehicleTrips.start_date == seed_start_date,
            ),
            isouter=True,
        )
    )

    update_insert_trips = db_manager.select_as_dataframe(update_insert_query)

    if update_insert_trips.shape[0] == 0:
        process_logger.add_metadata(
            trip_insert_count=0,
            trip_update_count=0,
        )
        process_logger.log_complete()
        return


    update_mask = (
        (update_insert_trips["existing_trip_hash"].notna())
        & (update_insert_trips["to_update"] == True)
    )
    insert_mask = update_insert_trips["existing_trip_hash"].isna()

    process_logger.add_metadata(
        trip_insert_count=insert_mask.sum(),
        trip_update_count=update_mask.sum(),
    )

    if update_mask.sum() > 0:
        update_cols = [
            "trip_hash",
            "stop_count",
            "static_trip_id_guess",
            "static_start_time",
            "static_stop_count",
            "first_last_station_match"
        ]
        db_manager.execute_with_data(
            sa.update(VehicleTrips.__table__).where(
                VehicleTrips.trip_hash == sa.bindparam("b_trip_hash"),
            ),
            update_insert_trips.loc[update_mask, update_cols].rename(
                columns={"trip_hash":"b_trip_hash"}
            )
        )
    
    if insert_mask.sum() > 0:
        insert_cols = [
            "trip_hash",
            "direction_id",
            "route_id",
            "start_date",
            "start_time",
            "vehicle_id",
            "stop_count",
            "static_trip_id_guess",
            "static_start_time",
            "static_stop_count",
            "first_last_station_match"
        ]
        db_manager.execute_with_data(
            sa.insert(VehicleTrips.__table__),
            update_insert_trips.loc[insert_mask, insert_cols]
        )

    process_logger.log_complete()


def process_trips_and_metrics(db_manager: DatabaseManager) -> None:
    try:
        seed_data = get_seed_data(db_manager)
        process_trips_tables(db_manager, seed_data)

    except Exception as e:
        logging.exception(e)
