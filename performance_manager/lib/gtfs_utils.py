from typing import Optional, List

import pandas


def add_event_hash_column(
    df_to_hash: pandas.DataFrame,
    hash_column_name: str = "hash",
    expected_hash_columns: Optional[List[str]] = None,
) -> pandas.DataFrame:
    """
    provide consistent hash values for category columns of gtfs-rt events
    """
    if expected_hash_columns is None:
        expected_hash_columns = [
            "is_moving",
            "stop_sequence",
            "stop_id",
            "direction_id",
            "route_id",
            "start_date",
            "start_time",
            "vehicle_id",
        ]
    row_check = set(expected_hash_columns) - set(df_to_hash.columns)
    if len(row_check) > 0:
        raise IndexError(f"Dataframe is missing expected columns: {row_check}")

    def apply_func(record: pandas.Series) -> int:
        return hash(tuple(record[row] for row in expected_hash_columns))

    df_to_hash[hash_column_name] = df_to_hash.apply(apply_func, axis=1)

    return df_to_hash


def start_time_to_seconds(
    time: Optional[str],
) -> Optional[float]:
    """
    transform time string in HH:MM:SS format to seconds
    """
    if time is None:
        return time
    (hour, minute, second) = time.split(":")
    return int(hour) * 3600 + int(minute) * 60 + int(second)
