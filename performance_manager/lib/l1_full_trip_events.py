from dataclasses import dataclass
import logging
from typing import Dict
import pandas
import numpy

import sqlalchemy as sa

from .postgres_utils import DatabaseManager
from .postgres_schema import (
    VehiclePositionEvents,
    TripUpdateEvents,
    FullTripEvents,
    TempFullTripEvents,
)
from .gtfs_utils import add_event_hash_column


@dataclass
class EventsToMerge:
    """
    container class for processing events that will be merged to make trip events
    """

    select_query: sa.sql.selectable.Select
    fk_rename_field: str
    merge_dataframe: pandas.DataFrame = pandas.DataFrame()


def get_merge_list() -> Dict[str, EventsToMerge]:
    """
    getter function for dict of merge event objects
    """
    dupe_hash_query = (
        sa.select(VehiclePositionEvents.hash)
        .group_by(VehiclePositionEvents.hash)
        .having(sa.func.count() > 1)
    )

    moving_vp_events = EventsToMerge(
        select_query=sa.select(
            VehiclePositionEvents.pk_id,
            VehiclePositionEvents.stop_sequence,
            VehiclePositionEvents.stop_id,
            VehiclePositionEvents.direction_id,
            VehiclePositionEvents.route_id,
            VehiclePositionEvents.start_date,
            VehiclePositionEvents.start_time,
            VehiclePositionEvents.vehicle_id,
        ).where(
            (VehiclePositionEvents.is_moving == sa.true())
            & (
                VehiclePositionEvents.pk_id.notin_(
                    sa.select(FullTripEvents.fk_vp_moving_event)
                )
            )
            & (VehiclePositionEvents.hash.notin_(dupe_hash_query))
        ),
        fk_rename_field="fk_vp_moving_event",
    )

    stopped_vp_events = EventsToMerge(
        select_query=sa.select(
            VehiclePositionEvents.pk_id,
            VehiclePositionEvents.stop_sequence,
            VehiclePositionEvents.stop_id,
            VehiclePositionEvents.direction_id,
            VehiclePositionEvents.route_id,
            VehiclePositionEvents.start_date,
            VehiclePositionEvents.start_time,
            VehiclePositionEvents.vehicle_id,
        ).where(
            (VehiclePositionEvents.is_moving == sa.false())
            & (
                VehiclePositionEvents.pk_id.notin_(
                    sa.select(FullTripEvents.fk_vp_stopped_event)
                )
            )
            & (VehiclePositionEvents.hash.notin_(dupe_hash_query))
        ),
        fk_rename_field="fk_vp_stopped_event",
    )

    stopped_tu_events = EventsToMerge(
        select_query=sa.select(
            TripUpdateEvents.pk_id,
            TripUpdateEvents.stop_sequence,
            TripUpdateEvents.stop_id,
            TripUpdateEvents.direction_id,
            TripUpdateEvents.route_id,
            TripUpdateEvents.start_date,
            TripUpdateEvents.start_time,
            TripUpdateEvents.vehicle_id,
        ).where(
            (
                TripUpdateEvents.pk_id.notin_(
                    sa.select(FullTripEvents.fk_tu_stopped_event)
                )
            )
        ),
        fk_rename_field="fk_tu_stopped_event",
    )

    return {
        "moving_vp": moving_vp_events,
        "stopped_vp": stopped_vp_events,
        "stopped_tu": stopped_tu_events,
    }


def load_and_transform(
    events_list: Dict[str, EventsToMerge], db_manager: DatabaseManager
) -> None:
    """
    load and transform each set of events from RDS L0 tables
    """
    expected_hash_columns = [
        "stop_sequence",
        "stop_id",
        "direction_id",
        "route_id",
        "start_date",
        "start_time",
        "vehicle_id",
    ]

    for event_type in events_list.values():
        event_type.merge_dataframe = db_manager.select_as_dataframe(
            event_type.select_query
        )

        if event_type.merge_dataframe.shape[0] == 0:
            event_type.merge_dataframe = pandas.DataFrame(
                columns=["pk_id", "hash"]
            )
        else:
            event_type.merge_dataframe = add_event_hash_column(
                event_type.merge_dataframe,
                expected_hash_columns=expected_hash_columns,
            )[["pk_id", "hash"]]

        event_type.merge_dataframe.rename(
            columns={"pk_id": event_type.fk_rename_field}, inplace=True
        )


def merge_events(events_list: Dict[str, EventsToMerge]) -> pandas.DataFrame:
    """
    merge all unprocessed event types into single dataframe
    """
    return_dataframe = events_list["moving_vp"].merge_dataframe.merge(
        events_list["stopped_vp"].merge_dataframe, how="outer"
    )
    return_dataframe = return_dataframe.merge(
        events_list["stopped_tu"].merge_dataframe, how="left"
    )

    for column in return_dataframe.columns:
        return_dataframe[column] = return_dataframe[column].astype("Int64")

    return_dataframe = return_dataframe.fillna(numpy.nan).replace(
        [numpy.nan], [None]
    )

    return return_dataframe


def insert_and_update_database(
    insert_dataframe: pandas.DataFrame, db_manager: DatabaseManager
) -> None:
    """
    update / insert full trip event rds L1 table, handled as delete and insert
    """
    if insert_dataframe.shape[0] == 0:
        logging.warning("No new Full Trip Event records to load")
        return

    db_manager.truncate_table(TempFullTripEvents)

    db_manager.insert_dataframe(insert_dataframe, TempFullTripEvents.__table__)

    delete_query = sa.delete(FullTripEvents.__table__).where(
        FullTripEvents.hash.in_(sa.select(TempFullTripEvents.hash))
    )
    db_manager.execute(delete_query)

    insert_cols = [
        "hash",
        "fk_vp_stopped_event",
        "fk_vp_moving_event",
        "fk_tu_stopped_event",
    ]

    insert_query = (
        FullTripEvents.metadata.tables[FullTripEvents.__tablename__]
        .insert()
        .from_select(
            insert_cols,
            sa.select(
                TempFullTripEvents.hash,
                TempFullTripEvents.fk_vp_stopped_event,
                TempFullTripEvents.fk_vp_moving_event,
                TempFullTripEvents.fk_tu_stopped_event,
            ).where(
                (TempFullTripEvents.hash.notin_(sa.select(FullTripEvents.hash)))
            ),
        )
    )
    db_manager.execute(insert_query)


def process_full_trip_events(db_manager: DatabaseManager) -> None:
    """
    process new events from L0 tables and insert to L1 full trip event RDS table
    """
    try:
        events_list = get_merge_list()
        load_and_transform(events_list, db_manager)
        insert_dataframe = merge_events(events_list)
        insert_and_update_database(insert_dataframe, db_manager)
    except Exception as e:
        logging.info("Error loading new L1 full trip events")
        logging.exception(e)
