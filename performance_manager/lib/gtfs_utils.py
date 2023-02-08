import hashlib
from typing import Optional, Sequence

import numpy
import pandas


def add_event_hash_column(
    df_to_hash: pandas.DataFrame,
    hash_column_name: str,
    expected_hash_columns: Sequence[str],
) -> pandas.DataFrame:
    """
    provide consistent hash values for category columns of gtfs-rt events
    """

    row_check = set(expected_hash_columns) - set(df_to_hash.columns)
    if len(row_check) > 0:
        raise IndexError(f"Dataframe is missing expected columns: {row_check}")

    # function to be used for hashing each record,
    # requires string as input returns raw bytes object
    def apply_func(record: str) -> str:
        return hashlib.md5(record.encode("utf8")).hexdigest()

    # vectorize apply_func so it can be used on numpy.ndarray object
    vectorized_function = numpy.vectorize(apply_func)

    # replace all "na" types values with python None to create consistent hash
    df_to_hash = df_to_hash.fillna(numpy.nan).replace([numpy.nan], [None])

    # convert rows of dataframe to concatenated string and apply vectorized
    # hashing function
    expected_hash_columns = sorted(expected_hash_columns)
    df_to_hash[hash_column_name] = vectorized_function(
        df_to_hash[list(expected_hash_columns)].astype(str).values.sum(axis=1)
    )

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
