import os

import pyarrow.parquet as pq
import polars as pl


def get_rowgroup_statistics(parquet_path: str) -> pl.DataFrame:
    """
    Extract row group statistics from a Parquet file and return as a Polars DataFrame.

    Args:
        parquet_path: Path to the parquet file

    Returns:
        Polars DataFrame with row group statistics
    """
    parquet_file = pq.ParquetFile(parquet_path)
    file_metadata = parquet_file.metadata

    records = []
    for i in range(file_metadata.num_row_groups):
        row_group_metadata = file_metadata.row_group(i)

        for col_i in range(row_group_metadata.num_columns):
            column_chunk_metadata = row_group_metadata.column(col_i)
            stats = column_chunk_metadata.statistics

            records.append(
                {
                    "row_group": i,
                    "num_rows": row_group_metadata.num_rows,
                    "total_byte_size": row_group_metadata.total_byte_size,
                    "column_index": col_i,
                    "column_path": column_chunk_metadata.path_in_schema,
                    "compressed_size": column_chunk_metadata.total_compressed_size,
                    "uncompressed_size": column_chunk_metadata.total_uncompressed_size,
                    "min_value": str(stats.min) if stats and stats.min is not None else None,
                    "max_value": str(stats.max) if stats and stats.max is not None else None,
                    "null_count": stats.null_count if stats else None,
                    "num_values": stats.num_values if stats else None,
                }
            )

    return pl.DataFrame(records)


if __name__ == "__main__":
    # df = get_rowgroup_statistics("/tmp/2026_2_1.parquet")
    # print(df)

    parquet_file = pq.ParquetFile(file)
        file_metadata = parquet_file.metadata
    for file in os.listdir("/Users/hhuang/ingestion/lamp/RT_TRIP_UPDATES/year=2026/month=2/day=1/"):
        print(file)
        df = get_rowgroup_statistics(f"/Users/hhuang/ingestion/lamp/RT_TRIP_UPDATES/year=2026/month=2/day=1/{file}")
        breakpoint()
        ts = df.filter(pl.col("column_path") == "feed_timestamp")
        parquet_file = pq.ParquetFile(f"/Users/hhuang/ingestion/lamp/RT_TRIP_UPDATES/year=2026/month=2/day=1/{file}")
        file_metadata = parquet_file.metadata

ts.select('min_value', 'max_value').with_columns(pl.lit(f"/Users/hhuang/ingestion/lamp/RT_TRIP_UPDATES/year=2026/month=2/day=1/{file}").alias('filename'))