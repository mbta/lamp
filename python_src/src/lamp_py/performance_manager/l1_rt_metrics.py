from typing import List

import numpy
import pandas
import sqlalchemy as sa

from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.postgres.postgres_schema import (
    VehicleEventMetrics,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger
from .l1_cte_statements import get_trips_for_metrics


# pylint: disable=R0914
# pylint too many local variables (more than 15)
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
        )
        .where(
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
            travel_times_cte.c.trip_stop_hash
            == dwell_times_cte.c.trip_stop_hash,
            full=True,
        )
        .join(
            headways_branch_cte,
            sa.func.coalesce(
                travel_times_cte.c.trip_stop_hash,
                dwell_times_cte.c.trip_stop_hash,
            )
            == headways_branch_cte.c.trip_stop_hash,
            full=True,
        )
        .join(
            headways_trunk_cte,
            sa.func.coalesce(
                travel_times_cte.c.trip_stop_hash,
                dwell_times_cte.c.trip_stop_hash,
                headways_branch_cte.c.trip_stop_hash,
            )
            == headways_trunk_cte.c.trip_stop_hash,
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


# pylint: enable=R0914


def process_metrics(
    db_manager: DatabaseManager, events: pandas.DataFrame
) -> None:
    """
    insert and update metrics table
    """
    for start_date in events["start_date"].unique():
        timestamps = [
            int(s_d)
            for s_d in events.loc[
                events["start_date"] == start_date, "fk_static_timestamp"
            ].unique()
        ]

        process_metrics_table(
            db_manager,
            seed_start_date=int(start_date),
            seed_timestamps=timestamps,
        )
