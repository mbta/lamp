from typing import List
import binascii

import numpy
import pandas
import sqlalchemy as sa

from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.postgres.postgres_schema import (
    VehicleTrips,
    TempHashCompare,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger
from .l1_cte_statements import (
    get_rt_trips_cte,
    get_static_trips_cte,
)


def insert_trips_hash_columns(
    db_manager: DatabaseManager, events: pandas.DataFrame
) -> None:
    """
    Load new 'trip_hash' records and columns into RDS

    This guarantees that all events represented in the events dataframe will have
    matching trips data in the VehicleTrips table
    """
    insert_columns = [
        "trip_hash",
        "fk_static_timestamp",
        "direction_id",
        "route_id",
        "start_date",
        "start_time",
        "vehicle_id",
    ]
    insert_events = events.loc[
        events["do_insert"], insert_columns
    ].drop_duplicates(subset="trip_hash")

    new_trip_hash_bytes = insert_events["trip_hash"]

    db_manager.truncate_table(TempHashCompare)
    db_manager.execute_with_data(
        sa.insert(TempHashCompare.__table__),
        new_trip_hash_bytes.to_frame("trip_stop_hash"),
    )

    db_trip_hashes = db_manager.select_as_dataframe(
        sa.select(
            VehicleTrips.trip_hash,
        ).join(
            TempHashCompare,
            TempHashCompare.trip_stop_hash == VehicleTrips.trip_hash,
        )
    )

    insert_events["trip_hash"] = (
        insert_events["trip_hash"].apply(binascii.hexlify).str.decode("utf-8")
    )

    if db_trip_hashes.shape[0] == 0:
        db_trip_hashes = pandas.DataFrame(columns=["trip_hash"])
    else:
        db_trip_hashes["trip_hash"] = (
            db_trip_hashes["trip_hash"]
            .apply(binascii.hexlify)
            .str.decode("utf-8")
        )

    # insert_events are records that don't have an existing trip_hash in the VehicleTrips table
    insert_events = insert_events.merge(
        db_trip_hashes, how="outer", on="trip_hash", indicator=True
    ).query('_merge=="left_only"')

    insert_events = insert_events.drop(columns=["_merge"])

    insert_events["trip_hash"] = insert_events["trip_hash"].str.decode("hex")

    if insert_events.shape[0] > 0:
        db_manager.execute_with_data(
            sa.insert(VehicleTrips.__table__),
            insert_events,
        )


# pylint: disable=R0914
# pylint too many local variables (more than 15)
def process_trips_table(
    db_manager: DatabaseManager,
    seed_start_date: int,
    seed_timestamps: List[int],
) -> None:
    """
    processs updates to trips table

    generate new trips table information
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
            sa.case(
                [
                    (
                        rt_trips_cte.c.route_id.like("Green%"),
                        sa.literal("Green"),
                    ),
                ],
                else_=rt_trips_cte.c.route_id,
            ).label("trunk_route_id"),
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
            rt_trips_summary_cte.c.trunk_route_id,
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
                < 5400,
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
            exact_trips_match.c.trunk_route_id,
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
    # - trunk_route_id
    # - stop_count
    # - static_trip_id_guess
    # - static_start_time
    # - static_stop_count
    # - first_last_station_match (exact matching)
    update_insert_query = (
        sa.select(
            all_new_trips.c.trip_hash,
            all_new_trips.c.timestamp.label("fk_static_timestamp"),
            VehicleTrips.trip_hash.label("existing_trip_hash"),
            all_new_trips.c.direction_id,
            all_new_trips.c.route_id,
            all_new_trips.c.trunk_route_id,
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
                            all_new_trips.c.trunk_route_id
                            != VehicleTrips.trunk_route_id,
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

    update_insert_trips["fk_static_timestamp"] = update_insert_trips[
        "fk_static_timestamp"
    ].astype("int64")
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

    process_logger.add_metadata(
        trip_update_count=update_mask.sum(),
    )

    if update_mask.sum() > 0:
        update_cols = [
            "trip_hash",
            "trunk_route_id",
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

    process_logger.log_complete()


def process_trips(
    db_manager: DatabaseManager, events: pandas.DataFrame
) -> pandas.DataFrame:
    """
    insert new trips records from events dataframe into RDS

    """
    insert_trips_hash_columns(db_manager, events)

    for start_date in events["start_date"].unique():
        timestamps = [
            int(s_d)
            for s_d in events.loc[
                events["start_date"] == start_date, "fk_static_timestamp"
            ].unique()
        ]

        process_trips_table(
            db_manager,
            seed_start_date=int(start_date),
            seed_timestamps=timestamps,
        )
