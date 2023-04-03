from typing import Dict, List, Any
import logging

import numpy
import sqlalchemy as sa

from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.postgres.postgres_schema import (
    VehicleEvents,
    VehicleTrips,
    VehicleEventMetrics,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger
from .l1_cte_statements import (
    get_rt_trips_cte,
    get_static_trips_cte,
    get_trips_for_metrics,
)


def get_seed_data(db_manager: DatabaseManager) -> Dict[int, List[int]]:
    """
    get seed data for populating trip and metrics tables

    distinct combinations of start_date and fk_static_timestamp allow for the pulling
    of all records associated with a full service date

    return: Dict[start_date:int : List[fk_static_timestamps]]
    """
    max_days_to_process = 3

    last_metric_update = sa.select(
        sa.func.coalesce(
            sa.func.max(VehicleTrips.updated_on),
            sa.func.to_timestamp(0),
        )
    ).scalar_subquery()

    seed_query = (
        sa.select(
            VehicleEvents.start_date,
            VehicleEvents.fk_static_timestamp,
        )
        .distinct(
            VehicleEvents.start_date,
            VehicleEvents.fk_static_timestamp,
        )
        .where(VehicleEvents.updated_on > last_metric_update)
        .order_by(VehicleEvents.start_date)
        .limit(100)
    )

    # returns empty list if no records to process
    seed_data = db_manager.select_as_list(seed_query)

    seed_data_return: Dict[Any, List[Any]] = {}
    for record in seed_data:
        start_date = record.get("start_date")
        timestamp = record.get("fk_static_timestamp")

        if start_date not in seed_data_return:
            if len(seed_data_return) < max_days_to_process:
                seed_data_return[start_date] = []
            else:
                break
        seed_data_return[start_date].append(timestamp)

    return seed_data_return


def process_tables(
    db_manager: DatabaseManager, seed_data: Dict[int, List[int]]
) -> None:
    """
    process updates to multiple days of trips / metrics

    during normal running this will coninually process one day at a time, but
    during initial loading this number could be increased to speed up loading of
    trips and metrics table
    """
    process_logger = ProcessLogger(
        "l1_tables_loader", seed_data_size=len(seed_data)
    )
    process_logger.log_start()

    days_processed = 0
    for seed_start_date in sorted(seed_data.keys()):
        seed_timestamps = seed_data[seed_start_date]
        process_trips_table(db_manager, seed_start_date, seed_timestamps)
        process_metrics_table(db_manager, seed_start_date, seed_timestamps)
        days_processed += 1

    process_logger.add_metadata(days_processed=days_processed)
    process_logger.log_complete()


# pylint: disable=R0914
# pylint too many local variables (more than 15)
def process_trips_table(
    db_manager: DatabaseManager,
    seed_start_date: int,
    seed_timestamps: List[int],
) -> None:
    """
    processs updates to trips table

    TODO: Move to L0 processing when events table no longer has trip data
    """
    process_logger = ProcessLogger("l1_rt_trips_table_loader")
    process_logger.log_start()

    static_trips_cte = get_static_trips_cte(seed_timestamps, seed_start_date)

    # to build a 'summary' trips table only the first and last records for each
    # static trip are needed.
    first_stop_static_cte = (
        sa.select(
            static_trips_cte.c.timestamp,
            static_trips_cte.c.static_trip_id,
            static_trips_cte.c.static_stop_timestamp.label("static_start_time"),
            static_trips_cte.c.parent_station.label("static_trip_first_stop"),
        )
        .select_from(static_trips_cte)
        .where(static_trips_cte.c.static_trip_first_stop == sa.true())
        .cte(name="first_stop_static_cte")
    )

    # join first_stop_static_cte with last stop records to create trip summary records
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
        )
        .select_from(static_trips_cte)
        .join(
            first_stop_static_cte,
            sa.and_(
                static_trips_cte.c.timestamp
                == first_stop_static_cte.c.timestamp,
                static_trips_cte.c.static_trip_id
                == first_stop_static_cte.c.static_trip_id,
            ),
        )
        .where(static_trips_cte.c.static_trip_last_stop == sa.true())
        .cte(name="static_trips_summary_cte")
    )

    rt_trips_cte = get_rt_trips_cte(seed_start_date)

    # same methodology as static stop trips summary, but for RT trips
    # pulling values associated with first stop of trips based on trip_hash
    first_stop_rt_cte = (
        sa.select(
            rt_trips_cte.c.trip_hash,
            rt_trips_cte.c.parent_station.label("rt_trip_first_stop"),
        )
        .select_from(rt_trips_cte)
        .where(rt_trips_cte.c.rt_trip_first_stop_flag == sa.true())
    ).cte(name="first_stop_rt_cte")

    # join first_stop_rt_cte with values from last stop
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
            rt_trips_cte.c.rt_trip_stop_rank.label("rt_stop_count"),
        )
        .select_from(rt_trips_cte)
        .join(
            first_stop_rt_cte,
            rt_trips_cte.c.trip_hash == first_stop_rt_cte.c.trip_hash,
        )
        .where(rt_trips_cte.c.rt_trip_last_stop_flag == sa.true())
    ).cte(name="rt_trips_summary_cte")

    # perform matching between static trips summary records and RT trips summary records
    #
    # first match is "exact" indicating that the first stop station and last stop station
    # are equivalent and trip start times are within one of
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
        )
        .distinct(
            rt_trips_summary_cte.c.trip_hash,
        )
        .select_from(rt_trips_summary_cte)
        .join(
            static_trips_summary_cte,
            sa.and_(
                rt_trips_summary_cte.c.timestamp
                == static_trips_summary_cte.c.timestamp,
                rt_trips_summary_cte.c.direction_id
                == static_trips_summary_cte.c.direction_id,
                rt_trips_summary_cte.c.route_id
                == static_trips_summary_cte.c.route_id,
                rt_trips_summary_cte.c.rt_trip_first_stop
                == static_trips_summary_cte.c.static_trip_first_stop,
                rt_trips_summary_cte.c.rt_trip_last_stop
                == static_trips_summary_cte.c.static_trip_last_stop,
                sa.func.abs(
                    rt_trips_summary_cte.c.start_time
                    - static_trips_summary_cte.c.static_start_time
                )
                < 3600,
            ),
            isouter=True,
        )
        .order_by(
            rt_trips_summary_cte.c.trip_hash,
            sa.func.abs(
                rt_trips_summary_cte.c.start_time
                - static_trips_summary_cte.c.static_start_time
            ),
        )
    ).cte(name="exact_trips_match")

    # backup matching logic, should match all remaining RT trips to static trips,
    # assuming that the route_id exists in the static schedule data
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
        )
        .distinct(
            exact_trips_match.c.trip_hash,
        )
        .select_from(exact_trips_match)
        .join(
            static_trips_summary_cte,
            sa.and_(
                exact_trips_match.c.timestamp
                == static_trips_summary_cte.c.timestamp,
                exact_trips_match.c.direction_id
                == static_trips_summary_cte.c.direction_id,
                exact_trips_match.c.route_id
                == static_trips_summary_cte.c.route_id,
            ),
            isouter=True,
        )
        .where(exact_trips_match.c.static_trip_id.is_(None))
        .order_by(
            exact_trips_match.c.trip_hash,
            sa.func.abs(
                exact_trips_match.c.start_time
                - static_trips_summary_cte.c.static_start_time
            ),
        )
    ).cte(name="backup_trips_match")

    # union exact and backup trip matching records for final "new" trips summary records
    all_new_trips = (
        sa.union(
            sa.select(exact_trips_match).where(
                exact_trips_match.c.static_trip_id.is_not(None)
            ),
            sa.select(backup_trips_match),
        )
    ).cte(name="all_new_trips")

    # compare all_new_trips records to existing records in RDS based on trip_hash
    #
    # if trip_hash does not exist in RDS, insert records
    # if trip_hash does exist and specified fields are not equivlanet, update records
    # update based on:
    # - stop_count
    # - static_trip_id_guess
    # - static_start_time
    # - static_stop_count
    # - first_last_station_match (exact matching)
    update_insert_query = (
        sa.select(
            all_new_trips.c.trip_hash,
            VehicleTrips.trip_hash.label("existing_trip_hash"),
            all_new_trips.c.direction_id,
            all_new_trips.c.route_id,
            sa.case(
                [
                    (
                        all_new_trips.c.route_id.like("Green%"),
                        sa.literal("Green"),
                    ),
                ],
                else_=all_new_trips.c.route_id,
            ).label("trunk_route_id"),
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
                            all_new_trips.c.rt_stop_count
                            != VehicleTrips.stop_count,
                            all_new_trips.c.static_trip_id
                            != VehicleTrips.static_trip_id_guess,
                            all_new_trips.c.static_start_time
                            != VehicleTrips.static_start_time,
                            all_new_trips.c.static_stop_count
                            != VehicleTrips.static_stop_count,
                            all_new_trips.c.first_last_station_match
                            != VehicleTrips.first_last_station_match,
                        ),
                        sa.literal(True),
                    )
                ],
                else_=sa.literal(False),
            ).label("to_update"),
        )
        .select_from(all_new_trips)
        .join(
            VehicleTrips,
            sa.and_(
                all_new_trips.c.trip_hash == VehicleTrips.trip_hash,
                VehicleTrips.start_date == seed_start_date,
            ),
            isouter=True,
        )
    )

    # perform update/insert opertions with dataframe pulled from RDS
    update_insert_trips = db_manager.select_as_dataframe(update_insert_query)

    if update_insert_trips.shape[0] == 0:
        process_logger.add_metadata(
            trip_insert_count=0,
            trip_update_count=0,
        )
        process_logger.log_complete()
        return

    update_insert_trips["start_date"] = update_insert_trips[
        "start_date"
    ].astype("int64")
    update_insert_trips["start_time"] = update_insert_trips[
        "start_time"
    ].astype("int64")
    update_insert_trips["stop_count"] = update_insert_trips[
        "stop_count"
    ].astype("int64")
    update_insert_trips["static_start_time"] = update_insert_trips[
        "static_start_time"
    ].astype("Int64")
    update_insert_trips["static_stop_count"] = update_insert_trips[
        "static_stop_count"
    ].astype("Int64")
    update_insert_trips = update_insert_trips.fillna(numpy.nan).replace(
        [numpy.nan], [None]
    )

    update_mask = (
        update_insert_trips["existing_trip_hash"].notna()
    ) & numpy.equal(update_insert_trips["to_update"], True)
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
            "first_last_station_match",
        ]
        db_manager.execute_with_data(
            sa.update(VehicleTrips.__table__).where(
                VehicleTrips.trip_hash == sa.bindparam("b_trip_hash"),
            ),
            update_insert_trips.loc[update_mask, update_cols].rename(
                columns={"trip_hash": "b_trip_hash"}
            ),
        )

    if insert_mask.sum() > 0:
        insert_cols = [
            "trip_hash",
            "direction_id",
            "route_id",
            "trunk_route_id",
            "start_date",
            "start_time",
            "vehicle_id",
            "stop_count",
            "static_trip_id_guess",
            "static_start_time",
            "static_stop_count",
            "first_last_station_match",
        ]
        db_manager.execute_with_data(
            sa.insert(VehicleTrips.__table__),
            update_insert_trips.loc[insert_mask, insert_cols],
        )

    process_logger.log_complete()


# pylint: enable=R0914


def process_metrics_table(
    db_manager: DatabaseManager,
    seed_start_date: int,
    seed_timestamps: List[int],
) -> None:
    """
    processs updates to metrics table

    """

    process_logger = ProcessLogger("l1_rt_metrics_table_loader")
    process_logger.log_start()

    trips_for_metrics = get_trips_for_metrics(seed_timestamps, seed_start_date)

    # travel_times calculation:
    # limited to records where stop_timestamp > move_timestamp to avoid negative travel times
    # limited to non NULL stop and move timestmaps to avoid NULL results
    # negative travel times are error records, should flag???
    travel_times_cte = (
        sa.select(
            trips_for_metrics.c.trip_stop_hash,
            (
                trips_for_metrics.c.stop_timestamp
                - trips_for_metrics.c.move_timestamp
            ).label("travel_time_seconds"),
        )
        .where(
            trips_for_metrics.c.first_stop_flag == sa.false(),
            trips_for_metrics.c.stop_timestamp.is_not(None),
            trips_for_metrics.c.move_timestamp.is_not(None),
            trips_for_metrics.c.stop_timestamp
            > trips_for_metrics.c.move_timestamp,
        )
        .cte(name="travel_times")
    )

    # dwell_times calculations are different for the first stop of a trip
    # the first stop of a trip includes the dwell time since the stop_timestamp of
    # the end of the previous trip going in the opposite direcion at that station
    #
    # all remaining dwell_times calcuations are based on subtracting the a stop_timestamp
    # of a vehcile from the next move_timestamp of a vehicle
    #
    # the where statement of this CTE filters out any vehicle trips comprised of only 1 stop
    # on the assumption that those are not valid trips to include in the metrics calculations
    #
    # for consecutive trips that do not have same vehicle ID the first_stop headway
    # logic could have issues
    t_dwell_times_cte = (
        sa.select(
            trips_for_metrics.c.trip_stop_hash,
            sa.case(
                [
                    (
                        sa.and_(
                            trips_for_metrics.c.last_stop_flag == sa.false(),
                            trips_for_metrics.c.first_stop_flag == sa.false(),
                        ),
                        sa.func.lead(
                            trips_for_metrics.c.move_timestamp,
                        ).over(
                            partition_by=trips_for_metrics.c.vehicle_id,
                            order_by=trips_for_metrics.c.sort_timestamp,
                        )
                        - trips_for_metrics.c.stop_timestamp,
                    ),
                    (
                        trips_for_metrics.c.first_stop_flag == sa.true(),
                        sa.func.lead(
                            trips_for_metrics.c.move_timestamp,
                        ).over(
                            partition_by=trips_for_metrics.c.vehicle_id,
                            order_by=trips_for_metrics.c.sort_timestamp,
                        )
                        - sa.func.lag(
                            trips_for_metrics.c.stop_timestamp,
                        ).over(
                            partition_by=trips_for_metrics.c.vehicle_id,
                            order_by=trips_for_metrics.c.sort_timestamp,
                        ),
                    ),
                ],
                else_=sa.literal(None),
            ).label("dwell_time_seconds"),
        )
        .where(
            sa.or_(
                trips_for_metrics.c.stop_count > 1,
                trips_for_metrics.c.first_stop_flag == sa.false(),
            ),
        )
        .cte(name="t_dwell_times")
    )

    # limit dwell times caluclations to NON-NULL positivie integers
    # would be nice if this could be done in the first CTE, but I can't 
    # get it to work with sqlalchemy
    dwell_times_cte = (
        sa.select(
            t_dwell_times_cte.c.trip_stop_hash,
            t_dwell_times_cte.c.dwell_time_seconds,
        ).where(
            t_dwell_times_cte.c.dwell_time_seconds.is_not(None),
            t_dwell_times_cte.c.dwell_time_seconds > 0,
        )
        .cte(name="dwell_times")
    )

    # this headways CTE calculation is incomplete
    #
    # trunk and branch headways are the same except for one is partiioned on
    # trunk_route_id and the later on route_id. route_id does not correctly
    # partition red line branch headways.
    #
    # first stop headways are calculated with move_timestamp to move_timestamp
    # for the next station in a trip
    #
    # all other headways are calculated with stop_timstamp to stop_timestamp by
    # subsequent vehicles on the same route/direction
    t_headways_branch_cte = (
        sa.select(
            trips_for_metrics.c.trip_stop_hash,
            sa.case(
                [
                    (
                        trips_for_metrics.c.first_stop_flag == sa.false(),
                        trips_for_metrics.c.stop_timestamp
                        - sa.func.lag(
                            trips_for_metrics.c.stop_timestamp,
                        ).over(
                            partition_by=(
                                trips_for_metrics.c.parent_station,
                                trips_for_metrics.c.route_id,
                                trips_for_metrics.c.direction_id,
                            ),
                            order_by=trips_for_metrics.c.sort_timestamp,
                        ),
                    ),
                ],
                else_=trips_for_metrics.c.next_station_move
                - sa.func.lag(
                    trips_for_metrics.c.next_station_move,
                ).over(
                    partition_by=(
                        trips_for_metrics.c.parent_station,
                        trips_for_metrics.c.route_id,
                        trips_for_metrics.c.direction_id,
                    ),
                    order_by=trips_for_metrics.c.sort_timestamp,
                ),
            ).label("headway_branch_seconds"),
        )
        .where(
            sa.or_(
                trips_for_metrics.c.stop_count > 1,
                trips_for_metrics.c.first_stop_flag == sa.false(),
            ),
        )
        .cte(name="t_headways_branch")
    )

    # limit headways branch calculations to NON-NULL positive integers
    # would be nice if this could be done in the first CTE, but I can't 
    # get it to work with sqlalchemy
    headways_branch_cte = (
        sa.select(
            t_headways_branch_cte.c.trip_stop_hash,
            t_headways_branch_cte.c.headway_branch_seconds,
        )
        .where(
            t_headways_branch_cte.c.headway_branch_seconds.is_not(None),
            t_headways_branch_cte.c.headway_branch_seconds > 0,
        )
        .cte(name="headways_branch")
    )

    t_headways_trunk_cte = (
        sa.select(
            trips_for_metrics.c.trip_stop_hash,
            sa.case(
                [
                    (
                        trips_for_metrics.c.first_stop_flag == sa.false(),
                        trips_for_metrics.c.stop_timestamp
                        - sa.func.lag(
                            trips_for_metrics.c.stop_timestamp,
                        ).over(
                            partition_by=(
                                trips_for_metrics.c.parent_station,
                                trips_for_metrics.c.trunk_route_id,
                                trips_for_metrics.c.direction_id,
                            ),
                            order_by=trips_for_metrics.c.sort_timestamp,
                        ),
                    ),
                ],
                else_=trips_for_metrics.c.next_station_move
                - sa.func.lag(
                    trips_for_metrics.c.next_station_move,
                ).over(
                    partition_by=(
                        trips_for_metrics.c.parent_station,
                        trips_for_metrics.c.trunk_route_id,
                        trips_for_metrics.c.direction_id,
                    ),
                    order_by=trips_for_metrics.c.sort_timestamp,
                ),
            ).label("headway_trunk_seconds"),
        )
        .where(
            sa.or_(
                trips_for_metrics.c.stop_count > 1,
                trips_for_metrics.c.first_stop_flag == sa.false(),
            ),
        )
        .cte(name="t_headways_trunk")
    )

    # limit headways trunk calculations to NON-NULL positive integers
    # would be nice if this could be done in the first CTE, but I can't 
    # get it to work with sqlalchemy
    headways_trunk_cte = (
        sa.select(
            t_headways_trunk_cte.c.trip_stop_hash,
            t_headways_trunk_cte.c.headway_trunk_seconds,
        )
        .where(
            t_headways_trunk_cte.c.headway_trunk_seconds.is_not(None),
            t_headways_trunk_cte.c.headway_trunk_seconds > 0,
        )
        .cte(name="headways_trunk")
    )

    # all_new_metrics combines travel_times, dwell_times and headways into one result set
    # previous CTE's ensure that individual input tables will not have null or negative values
    # would be nice to be able to use USING instead of COALESCE function for combining multiple
    # tables with same key, but I can't locate support in sqlalchemy
    all_new_metrics = (
        sa.select(
            sa.func.coalesce(
                travel_times_cte.c.trip_stop_hash,
                dwell_times_cte.c.trip_stop_hash,
                headways_branch_cte.c.trip_stop_hash,
                headways_trunk_cte.c.trip_stop_hash,
            ).label("trip_stop_hash"),
            travel_times_cte.c.travel_time_seconds,
            dwell_times_cte.c.dwell_time_seconds,
            headways_branch_cte.c.headway_branch_seconds,
            headways_trunk_cte.c.headway_trunk_seconds,
        )
        .select_from(
            travel_times_cte,
        )
        .join(
            dwell_times_cte,
            sa.and_(
                travel_times_cte.c.trip_stop_hash
                == dwell_times_cte.c.trip_stop_hash,
            ),
            full=True,
        )
        .join(
            headways_branch_cte,
            sa.and_(
                sa.func.coalesce(
                    travel_times_cte.c.trip_stop_hash,
                    dwell_times_cte.c.trip_stop_hash,
                )
                == headways_branch_cte.c.trip_stop_hash,
            ),
            full=True,
        )
        .join(
            headways_trunk_cte,
            sa.and_(
                sa.func.coalesce(
                    travel_times_cte.c.trip_stop_hash,
                    dwell_times_cte.c.trip_stop_hash,
                    headways_branch_cte.c.trip_stop_hash,
                )
                == headways_trunk_cte.c.trip_stop_hash,
            ),
            full=True,
        )
    ).cte(name="all_new_metrics")

    # update / insert logic matches what is done for the trips table
    # all_new_metrics are matched to existing RDS records by trip_stop_hash
    # and flagged for update, if needed
    update_insert_query = (
        sa.select(
            all_new_metrics.c.trip_stop_hash,
            VehicleEventMetrics.trip_stop_hash.label("existing_trip_stop_hash"),
            all_new_metrics.c.travel_time_seconds,
            all_new_metrics.c.dwell_time_seconds,
            all_new_metrics.c.headway_branch_seconds,
            all_new_metrics.c.headway_trunk_seconds,
            sa.case(
                [
                    (
                        sa.or_(
                            all_new_metrics.c.travel_time_seconds
                            != VehicleEventMetrics.travel_time_seconds,
                            all_new_metrics.c.dwell_time_seconds
                            != VehicleEventMetrics.dwell_time_seconds,
                            all_new_metrics.c.headway_branch_seconds
                            != VehicleEventMetrics.headway_branch_seconds,
                            all_new_metrics.c.headway_trunk_seconds
                            != VehicleEventMetrics.headway_trunk_seconds,
                        ),
                        sa.literal(True),
                    )
                ],
                else_=sa.literal(False),
            ).label("to_update"),
        )
        .select_from(all_new_metrics)
        .join(
            VehicleEventMetrics,
            all_new_metrics.c.trip_stop_hash
            == VehicleEventMetrics.trip_stop_hash,
            isouter=True,
        )
    )

    # this logic is again very similar to trips dataframe insert/update operation
    # some possibility of removing duplicate code and having common insert/update
    # function
    update_insert_metrics = db_manager.select_as_dataframe(update_insert_query)

    if update_insert_metrics.shape[0] == 0:
        process_logger.add_metadata(
            metric_insert_count=0,
            metric_update_count=0,
        )
        process_logger.log_complete()
        return

    update_insert_metrics["travel_time_seconds"] = update_insert_metrics[
        "travel_time_seconds"
    ].astype("Int64")
    update_insert_metrics["dwell_time_seconds"] = update_insert_metrics[
        "dwell_time_seconds"
    ].astype("Int64")
    update_insert_metrics["headway_branch_seconds"] = update_insert_metrics[
        "headway_branch_seconds"
    ].astype("Int64")
    update_insert_metrics["headway_trunk_seconds"] = update_insert_metrics[
        "headway_trunk_seconds"
    ].astype("Int64")
    update_insert_metrics = update_insert_metrics.fillna(numpy.nan).replace(
        [numpy.nan], [None]
    )

    update_mask = (
        update_insert_metrics["existing_trip_stop_hash"].notna()
    ) & numpy.equal(update_insert_metrics["to_update"], True)
    insert_mask = update_insert_metrics["existing_trip_stop_hash"].isna()

    process_logger.add_metadata(
        metric_insert_count=insert_mask.sum(),
        metric_update_count=update_mask.sum(),
    )

    db_columns = [
        "trip_stop_hash",
        "travel_time_seconds",
        "dwell_time_seconds",
        "headway_branch_seconds",
        "headway_trunk_seconds",
    ]
    if update_mask.sum() > 0:
        db_manager.execute_with_data(
            sa.update(VehicleEventMetrics.__table__).where(
                VehicleEventMetrics.trip_stop_hash
                == sa.bindparam("b_trip_stop_hash"),
            ),
            update_insert_metrics.loc[update_mask, db_columns].rename(
                columns={"trip_stop_hash": "b_trip_stop_hash"}
            ),
        )

    if insert_mask.sum() > 0:
        db_manager.execute_with_data(
            sa.insert(VehicleEventMetrics.__table__),
            update_insert_metrics.loc[insert_mask, db_columns],
        )

    process_logger.log_complete()


def process_trips_and_metrics(db_manager: DatabaseManager) -> None:
    """
    insert and update trips and metrics tables, one day at a time
    """
    try:
        seed_data = get_seed_data(db_manager)
        process_tables(db_manager, seed_data)

    except Exception as e:
        logging.exception(e)
