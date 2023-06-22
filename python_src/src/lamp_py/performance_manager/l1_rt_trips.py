from typing import List
import binascii

import pandas
import sqlalchemy as sa

from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.postgres.postgres_schema import (
    VehicleEvents,
    VehicleTrips,
    StaticTrips,
    TempHashCompare,
    StaticDirections,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger
from .l1_cte_statements import (
    get_static_trips_cte,
)


def load_temp_for_hash_compare(
    db_manager: DatabaseManager, events: pandas.DataFrame
) -> None:
    """
    Load "temp_hash_compare" table with "trip_hash" from newly inserted RT events
    """
    new_trip_hash_bytes = events.loc[
        events["do_insert"], "trip_hash"
    ].drop_duplicates()

    db_manager.truncate_table(TempHashCompare)
    db_manager.execute_with_data(
        sa.insert(TempHashCompare.__table__),
        new_trip_hash_bytes.to_frame("hash"),
    )


def update_prev_next_trip_stop(db_manager: DatabaseManager) -> None:
    """
    Update `previous_trip_stop_pk_id` and `next_trip_stop_pk_id` fields in `vehicle_events` table
    """
    prev_next_trip_stop_cte = (
        sa.select(
            VehicleEvents.pk_id,
            sa.func.lead(VehicleEvents.pk_id)
            .over(
                partition_by=VehicleEvents.trip_hash,
                order_by=VehicleEvents.stop_sequence,
            )
            .label("next_trip_stop_pk_id"),
            sa.func.lag(VehicleEvents.pk_id)
            .over(
                partition_by=VehicleEvents.trip_hash,
                order_by=VehicleEvents.stop_sequence,
            )
            .label("previous_trip_stop_pk_id"),
        ).join(
            TempHashCompare,
            TempHashCompare.hash == VehicleEvents.trip_hash,
        )
    ).cte(name="prev_next_trip_stops")

    update_query = (
        sa.update(VehicleEvents.__table__)
        .where(
            VehicleEvents.pk_id == prev_next_trip_stop_cte.c.pk_id,
        )
        .values(
            next_trip_stop_pk_id=prev_next_trip_stop_cte.c.next_trip_stop_pk_id,
            previous_trip_stop_pk_id=prev_next_trip_stop_cte.c.previous_trip_stop_pk_id,
        )
    )

    process_logger = ProcessLogger("l1_trips.update_prev_next_trip_stop")
    process_logger.log_start()
    db_manager.execute(update_query)
    process_logger.log_complete()


def load_new_trip_data(
    db_manager: DatabaseManager, events: pandas.DataFrame
) -> None:
    """
    Load new "vehicle_trip" table data columns into RDS

    This guarantees that all events represented in the events dataframe will have
    matching trips data in the "vehicle_trips" table
    """
    process_logger = ProcessLogger("l1_trips.load_new_trips")
    process_logger.log_start()
    insert_columns = [
        "trip_hash",
        "static_version_key",
        "direction_id",
        "route_id",
        "service_date",
        "start_time",
        "vehicle_id",
        "trip_id",
        "vehicle_label",
        "vehicle_consist",
    ]
    insert_events = events.loc[
        events["do_insert"], insert_columns
    ].drop_duplicates(subset="trip_hash")

    db_trip_hashes = db_manager.select_as_dataframe(
        sa.select(
            VehicleTrips.trip_hash,
        ).join(
            TempHashCompare,
            TempHashCompare.hash == VehicleTrips.trip_hash,
        )
    )

    if db_trip_hashes.shape[0] == 0:
        db_trip_hashes = pandas.DataFrame(columns=["trip_hash"])
    else:
        db_trip_hashes["trip_hash"] = (
            db_trip_hashes["trip_hash"]
            .apply(binascii.hexlify)
            .str.decode("utf-8")
        )

    insert_events["trip_hash"] = (
        insert_events["trip_hash"].apply(binascii.hexlify).str.decode("utf-8")
    )

    # insert_events are records that don't have an existing trip_hash in the "vehicle_trips" table
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
    process_logger.log_complete()


def update_trip_stop_counts(db_manager: DatabaseManager) -> None:
    """
    Update "stop_count" field for trips with new events

    this function call should also update branch_route_id and trunk_route_id columns
    for the trips table by activiating the rt_trips_update_branch_trunk TDS trigger
    """
    new_stop_counts_cte = (
        sa.select(
            VehicleEvents.trip_hash,
            sa.func.count(VehicleEvents.trip_stop_hash).label("stop_count"),
        )
        .select_from(VehicleEvents)
        .join(
            TempHashCompare,
            TempHashCompare.hash == VehicleEvents.trip_hash,
        )
        .where(
            sa.or_(
                VehicleEvents.vp_move_timestamp.is_not(None),
                VehicleEvents.vp_stop_timestamp.is_not(None),
            ),
        )
        .group_by(
            VehicleEvents.trip_hash,
        )
        .cte("new_stop_counts")
    )

    update_query = (
        sa.update(VehicleTrips.__table__)
        .where(
            VehicleTrips.trip_hash == new_stop_counts_cte.c.trip_hash,
        )
        .values(stop_count=new_stop_counts_cte.c.stop_count)
    )

    process_logger = ProcessLogger("l1_trips.update_trip_stop_counts")
    process_logger.log_start()
    db_manager.execute(update_query)
    process_logger.log_complete()


def update_static_trip_id_guess_exact(db_manager: DatabaseManager) -> None:
    """
    Update static_trip_id_guess with exact matches between rt and static trips
    """
    rt_static_match_cte = (
        sa.select(
            VehicleTrips.trip_hash,
            VehicleTrips.trip_id,
            sa.true().label("first_last_station_match"),
            # TODO: add stop_count from static pre-processing when available # pylint: disable=fixme
            # TODO: add start_time from sattic pre-processing when available # pylint: disable=fixme
        )
        .select_from(VehicleTrips)
        .join(TempHashCompare, TempHashCompare.hash == VehicleTrips.trip_hash)
        .join(
            StaticTrips,
            sa.and_(
                StaticTrips.static_version_key
                == VehicleTrips.static_version_key,
                StaticTrips.trip_id == VehicleTrips.trip_id,
                StaticTrips.direction_id == VehicleTrips.direction_id,
                StaticTrips.route_id == VehicleTrips.route_id,
            ),
        )
        .cte("exact_trip_id_matches")
    )

    update_query = (
        sa.update(VehicleTrips.__table__)
        .where(
            VehicleTrips.trip_hash == rt_static_match_cte.c.trip_hash,
        )
        .values(
            static_trip_id_guess=rt_static_match_cte.c.trip_id,
            first_last_station_match=rt_static_match_cte.c.first_last_station_match,
            # TODO: add stop_count from static pre-processing when available # pylint: disable=fixme
            # TODO: add start_time from sattic pre-processing when available # pylint: disable=fixme
        )
    )

    process_logger = ProcessLogger("l1_trips.update_exact_trip_matches")
    process_logger.log_start()
    db_manager.execute(update_query)
    process_logger.log_complete()


def update_directions(db_manager: DatabaseManager) -> None:
    """
    Update direction and direction_destination from static tables
    """
    directions_cte = (
        sa.select(
            VehicleTrips.trip_hash,
            StaticDirections.direction,
            StaticDirections.direction_destination,
        )
        .select_from(VehicleTrips)
        .join(TempHashCompare, TempHashCompare.hash == VehicleTrips.trip_hash)
        .join(
            StaticDirections,
            sa.and_(
                VehicleTrips.static_version_key
                == StaticDirections.static_version_key,
                VehicleTrips.direction_id == StaticDirections.direction_id,
                VehicleTrips.route_id == StaticDirections.route_id,
            ),
        )
        .cte("update_directions")
    )

    update_query = (
        sa.update(VehicleTrips.__table__)
        .where(
            VehicleTrips.trip_hash == directions_cte.c.trip_hash,
        )
        .values(
            direction=directions_cte.c.direction,
            direction_destination=directions_cte.c.direction_destination,
        )
    )

    process_logger = ProcessLogger("l1_trips.update_directions")
    process_logger.log_start()
    db_manager.execute(update_query)
    process_logger.log_complete()


def load_new_trips_records(
    db_manager: DatabaseManager, events: pandas.DataFrame
) -> None:
    """
    Load data into "vehicle_trips" table for any new RT events
    """
    load_temp_for_hash_compare(db_manager, events)
    update_prev_next_trip_stop(db_manager)
    load_new_trip_data(db_manager, events)
    update_trip_stop_counts(db_manager)
    update_static_trip_id_guess_exact(db_manager)
    update_directions(db_manager)


# pylint: disable=R0914
# pylint too many local variables (more than 15)
def backup_rt_static_trip_match(
    db_manager: DatabaseManager,
    seed_service_date: int,
    version_keys: List[int],
) -> None:
    """
    perform "backup" match of RT trips to Static schedule trip

    this matches an RT trip to a static trip with the same branch_route_id or trunk_route_id if branch is null
    and direction with the closest start_time
    """
    static_trips_cte = get_static_trips_cte(version_keys, seed_service_date)

    # to build a 'summary' trips table only the first and last records for each
    # static trip are needed.
    first_stop_static_cte = (
        sa.select(
            static_trips_cte.c.static_version_key,
            static_trips_cte.c.static_trip_id,
            static_trips_cte.c.static_stop_timestamp.label("static_start_time"),
        )
        .select_from(static_trips_cte)
        .where(static_trips_cte.c.static_trip_first_stop == sa.true())
        .cte(name="first_stop_static_cte")
    )

    # join first_stop_static_cte with last stop records to create trip summary records
    static_trips_summary_cte = (
        sa.select(
            static_trips_cte.c.static_version_key,
            static_trips_cte.c.static_trip_id,
            sa.func.coalesce(
                static_trips_cte.c.branch_route_id,
                static_trips_cte.c.trunk_route_id,
            ).label("route_id"),
            static_trips_cte.c.direction_id,
            first_stop_static_cte.c.static_start_time,
            static_trips_cte.c.static_trip_stop_rank.label("static_stop_count"),
        )
        .select_from(static_trips_cte)
        .join(
            first_stop_static_cte,
            sa.and_(
                static_trips_cte.c.static_version_key
                == first_stop_static_cte.c.static_version_key,
                static_trips_cte.c.static_trip_id
                == first_stop_static_cte.c.static_trip_id,
            ),
        )
        .where(static_trips_cte.c.static_trip_last_stop == sa.true())
        .cte(name="static_trips_summary_cte")
    )

    # pull RT trips records that are candidates for backup matching to static trips
    rt_trips_summary_cte = (
        sa.select(
            VehicleTrips.trip_hash,
            VehicleTrips.direction_id,
            sa.func.coalesce(
                VehicleTrips.branch_route_id, VehicleTrips.trunk_route_id
            ).label("route_id"),
            VehicleTrips.start_time,
            VehicleTrips.static_version_key,
        )
        .select_from(VehicleTrips)
        .join(TempHashCompare, TempHashCompare.hash == VehicleTrips.trip_hash)
        .where(
            VehicleTrips.first_last_station_match == sa.false(),
            VehicleTrips.service_date == int(seed_service_date),
            VehicleTrips.static_version_key.in_(version_keys),
        )
        .cte("rt_trips_for_backup_match")
    )

    # backup matching logic, should match all remaining RT trips to static trips,
    # assuming that the route_id exists in the static schedule data
    backup_trips_match = (
        sa.select(
            rt_trips_summary_cte.c.trip_hash,
            static_trips_summary_cte.c.static_trip_id,
            static_trips_summary_cte.c.static_start_time,
            static_trips_summary_cte.c.static_stop_count,
            sa.literal(False).label("first_last_station_match"),
        )
        .distinct(
            rt_trips_summary_cte.c.trip_hash,
        )
        .select_from(rt_trips_summary_cte)
        .join(
            static_trips_summary_cte,
            sa.and_(
                rt_trips_summary_cte.c.static_version_key
                == static_trips_summary_cte.c.static_version_key,
                rt_trips_summary_cte.c.direction_id
                == static_trips_summary_cte.c.direction_id,
                rt_trips_summary_cte.c.route_id
                == static_trips_summary_cte.c.route_id,
            ),
        )
        .order_by(
            rt_trips_summary_cte.c.trip_hash,
            sa.func.abs(
                rt_trips_summary_cte.c.start_time
                - static_trips_summary_cte.c.static_start_time
            ),
        )
    ).cte(name="backup_trips_match")

    update_query = (
        sa.update(VehicleTrips.__table__)
        .where(
            VehicleTrips.trip_hash == backup_trips_match.c.trip_hash,
        )
        .values(
            static_trip_id_guess=backup_trips_match.c.static_trip_id,
            static_start_time=backup_trips_match.c.static_start_time,
            static_stop_count=backup_trips_match.c.static_stop_count,
            first_last_station_match=backup_trips_match.c.first_last_station_match,
        )
    )

    db_manager.execute(update_query)


def process_trips(
    db_manager: DatabaseManager, events: pandas.DataFrame
) -> None:
    """
    insert new trips records from events dataframe into RDS

    """
    load_new_trips_records(db_manager, events)

    process_logger = ProcessLogger("l1_trips.update_backup_trip_matches")
    process_logger.log_start()
    for service_date in events["service_date"].unique():
        version_keys = [
            int(s_d)
            for s_d in events.loc[
                events["service_date"] == service_date, "static_version_key"
            ].unique()
        ]

        backup_rt_static_trip_match(
            db_manager,
            seed_service_date=int(service_date),
            version_keys=version_keys,
        )
    process_logger.log_complete()
