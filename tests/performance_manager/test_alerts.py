import random
import json
import datetime
import os

from typing import List, Tuple, Dict, Optional, Union

import pandas

from lamp_py.performance_manager.alerts import (
    extract_alerts,
    transform_translations,
    transform_timestamps,
    explode_active_periods,
    explode_informed_entity,
)
from lamp_py.performance_manager.gtfs_utils import BOSTON_TZ_ZONEINFO

from ..test_resources import springboard_dir


def generate_sample_translations(columns: List[str]) -> pandas.DataFrame:
    """
    generate sample data for translations tests
    """
    # create sample list of translation data
    entry_map = {
        "es": "Texto de muestra en español",
        "fr": "Texte d'exemple en français",
        "en": "Sample text in English",
    }

    # create a 200 record sample data
    sample_data: List[Dict[str, Optional[List[Dict[str, str]]]]] = []
    for _ in range(200):
        record: Dict[str, Optional[List[Dict[str, str]]]] = {}

        for col in columns:
            translations: List[Dict[str, str]] = []
            for __ in range(random.choice([0, 1, 2])):
                language = random.choice(list(entry_map.keys()))
                entry: Dict[str, str] = {
                    "language": language,
                    "text": entry_map[language],
                }
                translations.append(entry)

            record[col] = translations

            if not record[col]:
                if random.choice([True, False]):
                    record[col] = None

        sample_data.append(record)

    return pandas.DataFrame(sample_data)


def test_transform_translations() -> None:
    """
    test that translation transformations work as expected
    """
    # Define the translation columns
    columns = [
        "header_text.translation",
        "description_text.translation",
        "service_effect_text.translation",
        "timeframe_text.translation",
        "recurrence_text.translation",
    ]

    alerts_raw = generate_sample_translations(columns)
    alerts_processed = transform_translations(alerts_raw)

    for old_name in columns:
        new_name = f"{old_name}.text"

        # check that column names have been updated as expected
        assert old_name not in alerts_processed.columns
        assert new_name in alerts_processed.columns

        # check that the number of records that have english translations is
        # the same as the number of transformed translations.
        raw_en_translation_count = alerts_raw[old_name].apply(lambda x: '"en"' in json.dumps(x)).sum()
        processed_translation_count = alerts_processed[new_name].notna().sum()

        assert raw_en_translation_count == processed_translation_count

        # check that all of the translations are the english one
        unique_translations = alerts_processed[new_name].dropna().unique()
        assert len(unique_translations) == 1
        assert unique_translations[0] == "Sample text in English"


def generate_sample_timestamps(start_ts: int, end_ts: int) -> pandas.DataFrame:
    """
    generate a sample dataframe for timestamp conversion testing
    """
    sample_data = []

    # generate sample data
    for _ in range(1000):
        record: Dict[str, Optional[Union[int, list[Dict[str, int]]]]] = {}
        created_timestamp = random.randint(start_ts, end_ts)
        record["created_timestamp"] = created_timestamp
        if random.choice([True, False]):
            record["last_modified_timestamp"] = created_timestamp + random.randint(0, 3600 * 24 * 2)
        else:
            record["last_modified_timestamp"] = created_timestamp

        if random.choice([True, False]):
            record["last_push_notification_timestamp"] = created_timestamp + random.randint(0, 3600 * 24 * 2)
        else:
            record["last_push_notification_timestamp"] = None

        if random.choice([True, False]):
            record["closed_timestamp"] = created_timestamp + random.randint(0, 3600 * 24 * 2)
        else:
            record["closed_timestamp"] = None

        sample_data.append(record)

    # convert sample data to formatted dataframe
    alerts_raw = pandas.DataFrame(sample_data)
    alerts_raw["created_timestamp"] = alerts_raw["created_timestamp"].astype("Int64")
    alerts_raw["last_modified_timestamp"] = alerts_raw["last_modified_timestamp"].astype("Int64")
    alerts_raw["last_push_notification_timestamp"] = alerts_raw["last_push_notification_timestamp"].astype("Int64")
    alerts_raw["closed_timestamp"] = alerts_raw["closed_timestamp"].astype("Int64")

    return alerts_raw


def ranged_timestamp_test(start_ts: int, end_ts: int) -> None:
    """
    test timestamp conversions for a sample data generated between the start_ts and end_ts
    """
    alerts_raw = generate_sample_timestamps(start_ts, end_ts)
    # process sample data and inspect
    alerts_processed = transform_timestamps(alerts_raw)

    timestamp_columns = [
        "created",
        "last_modified",
        "last_push_notification",
        "closed",
    ]
    for base in timestamp_columns:
        old_name = f"{base}_timestamp"
        new_name = f"{base}_datetime"

        assert new_name in alerts_processed.columns
        assert old_name in alerts_processed.columns

        timestamp_count = alerts_raw[old_name].notna().sum()
        datetime_count = alerts_processed[new_name].notna().sum()
        assert timestamp_count == datetime_count

        if base in ["created", "last_modified"]:
            assert datetime_count == len(alerts_processed)
        else:
            assert datetime_count < len(alerts_processed) * 0.75, alerts_processed[[old_name, new_name]].head()

        non_null = alerts_processed[new_name].dropna()
        assert len(non_null) > 0


def test_transform_timestamps() -> None:
    """
    test that timestamp transformations work as expected around new years, the
    start of DST and the end of DST
    """
    ranged_timestamp_test(
        start_ts=int(datetime.datetime(2023, 1, 1).timestamp()),
        end_ts=int(datetime.datetime(2023, 1, 2).timestamp()),
    )

    # an hour before and after March DST transition in Boston
    ranged_timestamp_test(
        start_ts=(1678618800 - 3600),
        end_ts=(1678618800 + 3600),
    )

    # an hour before and two after November DST transition in Boston
    ranged_timestamp_test(
        start_ts=(1699182000 - 3600),
        end_ts=(1699182000 + 7200),
    )


def generate_sample_active_periods(
    start_range: Tuple[datetime.datetime, datetime.datetime],
    max_alert_duration_days: int,
) -> Tuple[pandas.DataFrame, int]:
    """
    Generate Sample Data for Active Period Explosion Test.
    @return Sample Dataframe for processing and a count of Active Periods to expect.
    """
    range_start_ts = int(start_range[0].timestamp())
    range_end_ts = int(start_range[0].timestamp())
    max_end_seconds = 3600 * 24 * max_alert_duration_days

    sample_data = []
    exploded_count = 0

    # generate sample data
    for index in range(1000):
        record: Dict[str, list[Dict[str, int | None]] | int | None] = {"id": index}
        periods: List[Dict[str, int | None]] = []
        for __ in range(random.randint(0, 15)):
            exploded_count += 1
            start: Optional[int] = random.randint(range_start_ts, range_end_ts)
            end: Optional[int] = start + random.randint(3600, max_end_seconds) if start else None

            if random.randint(1, 100) < 5:
                start = None
            elif random.randint(1, 100) < 5:
                end = None
            elif random.randint(1, 100) < 5:
                start = None
                end = None

            entry: Dict[str, int | None] = {"start": start, "end": end}
            periods.append(entry)

        record["active_period"] = periods

        if not record["active_period"] and random.choice([True, False]):
            record["active_period"] = None

        sample_data.append(record)

    return pandas.DataFrame(sample_data), exploded_count


def test_explode_active_period() -> None:
    """
    test that active periods can be exploded without losing information
    """
    start_dt = datetime.datetime(2023, 1, 1, tzinfo=BOSTON_TZ_ZONEINFO)
    end_dt = datetime.datetime(2023, 1, 2, tzinfo=BOSTON_TZ_ZONEINFO)
    max_duration_days = 2

    alerts_raw, active_period_count = generate_sample_active_periods(
        start_range=(start_dt, end_dt),
        max_alert_duration_days=max_duration_days,
    )

    alerts_processed = explode_active_periods(alerts_raw)
    assert "active_period" not in alerts_processed.columns

    nan_active_periods = alerts_raw["active_period"].apply(lambda x: 1 if x is None or len(x) == 0 else 0).sum()
    total_record_count = nan_active_periods + active_period_count

    assert len(alerts_processed) == total_record_count

    # drop the tz info, since all times should in the dataframe won't have them.
    start_dt = start_dt.replace(tzinfo=None)
    end_dt = end_dt.replace(tzinfo=None)
    max_end_dt = end_dt + datetime.timedelta(days=max_duration_days)

    for base in ["active_period.start", "active_period.end"]:
        timestamp_key = f"{base}_timestamp"
        datetime_key = f"{base}_datetime"

        assert timestamp_key in alerts_processed.columns
        assert datetime_key in alerts_processed.columns

        timestamp_count = alerts_processed[timestamp_key].notna().sum()
        datetime_count = alerts_processed[datetime_key].notna().sum()
        assert timestamp_count == datetime_count

        non_null = alerts_processed[datetime_key].dropna()
        assert len(non_null) > 0

        assert non_null[non_null < start_dt].empty
        assert non_null[non_null > max_end_dt].empty


def generate_sample_informed_entity(
    choices: Dict,
) -> Tuple[pandas.DataFrame, int]:
    """
    generate sample data for testing explode_informed_entity transformation
    @return a tuple of the data and an expected rowcount post explosion
    """
    sample_data = []

    informed_entity_count = 0
    for index in range(100):
        informed_entity = []
        for route_id in random.sample(choices["route_id"], random.randint(1, 4)):
            route_type = random.choice([0, 1, 2, 3, None])

            for direction_id in random.sample(choices["direction_id"], random.randint(1, 2)):
                for stop_id in random.sample(choices["stop_id"], random.randint(1, 3)):
                    facility_id = random.choice(choices["facility_id"])
                    activities = random.sample(choices["activities"], random.randint(0, 4))

                    record = {}
                    record["route_id"] = route_id
                    record["route_type"] = route_type
                    record["direction_id"] = direction_id
                    record["stop_id"] = stop_id
                    record["facility_id"] = facility_id
                    record["activities"] = activities
                    informed_entity.append(record)

                    informed_entity_count += 1

        sample_data.append({"id": index, "informed_entity": informed_entity})

    alerts_raw = pandas.DataFrame(sample_data)
    return alerts_raw, informed_entity_count


def test_explode_informed_entity() -> None:
    """
    test that exploding around the informed entity column works as expected
    """
    choices: Dict[str, Union[List[str | None], List[float | None], List[str]]] = {
        "route_id": [
            "1234",
            "Blue",
            "CR-Worcester",
            "Green-B",
            "Green-D",
            None,
        ],
        "route_type": [0.0, 1.0, 2.0, 3.0, None],
        "direction_id": ["0", "1", None],
        "stop_id": ["725", "09253", "523", "Winchester Center", None],
        "facility_id": [
            "door-cntsq-ibmass",
            "Beverly",
            "Alewife-02",
            "70061",
            None,
        ],
        "activities": ["BOARD", "PARK_CAR", "USING_ESCALATOR", "EXIT", "RIDE"],
    }

    alerts_raw, informed_entity_count = generate_sample_informed_entity(choices)
    alerts_processed = explode_informed_entity(alerts_raw)
    assert len(alerts_processed) == informed_entity_count

    for column, options in choices.items():
        key = f"informed_entity.{column}"
        assert key in alerts_processed.columns, alerts_processed.columns

        values = alerts_processed[key].unique()

        if column == "route_type":
            filtered_options = [o for o in options if not pandas.isna(o)]
            filtered_values = [v for v in values if not pandas.isna(v)]
            assert set(filtered_values) == set(filtered_options), f"{column} has different values"
        elif column == "activities":
            for value in values:
                if value == "":
                    continue
                for activity in value.split("|"):
                    assert activity in options
        else:
            assert set(values) == set(options), f"{column} has different values"


def test_etl() -> None:
    """
    Test that the entire ETL pipeline can be used without throwing and that it
    will be impacted by existing alerts that are passed into the extract_alerts
    function that kicks it off.
    """
    test_file = os.path.join(
        springboard_dir,
        "RT_ALERTS",
        "year=2020",
        "month=2",
        "day=9",
        "hour=1",
        "6ef6922c20064cb9a8f09a3b3b1d2783-0.parquet",
    )

    key_columns = ["id", "last_modified_timestamp"]
    existing = pandas.DataFrame(columns=key_columns)

    alerts = extract_alerts(alert_files=[test_file], existing_id_timestamp_pairs=existing)
    alerts = transform_translations(alerts)
    alerts = transform_timestamps(alerts)
    alerts = explode_active_periods(alerts)
    alerts = explode_informed_entity(alerts)

    # process it a second time with some of the id / lm timestamp pairs to filter against.
    existing = alerts[key_columns].drop_duplicates().head(5)
    alerts_2 = extract_alerts(alert_files=[test_file], existing_id_timestamp_pairs=existing)
    alerts_2 = transform_translations(alerts_2)
    alerts_2 = transform_timestamps(alerts_2)
    alerts_2 = explode_active_periods(alerts_2)
    alerts_2 = explode_informed_entity(alerts_2)

    assert len(alerts) > len(alerts_2)
