import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import pyarrow.parquet as pq
    return pl, pq


@app.cell
def _(pl, pq):

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
    return (get_rowgroup_statistics,)


@app.cell
def _(get_rowgroup_statistics):
    df = get_rowgroup_statistics('s3://mbta-ctd-dataplatform-staging-springboard/lamp/RT_TRIP_UPDATES/year=2026/month=5/day=1/2026-05-01T00:00:00.parquet')
    return (df,)


@app.cell
def _(df):
    df['total_byte_size'].sum()/(1024**3)
    return


@app.cell
def _(df):
    df['num_rows'].sum()
    return


@app.cell
def _():
    import duckdb
    return (duckdb,)


@app.cell
def _(duckdb):
    con = duckdb.connect()
    return (con,)


@app.cell
def _(con):
    con.execute("load aws;")
    return


@app.cell
def _(con):
    con.execute("""
        CREATE OR REPLACE SECRET secret (
        TYPE s3,
        PROVIDER credential_chain
    );
    """)
    return


@app.cell
def _():
    2000000*171
    return


@app.cell
def _(con):
    route1 = con.sql("""SELECT * from read_parquet('s3://mbta-ctd-dataplatform-staging-springboard/lamp/RT_TRIP_UPDATES/year=2026/month=5/day=16/2026-05-16T00:00:00.parquet')
    where "trip_update.trip.route_id" == '1'""").pl()
    return (route1,)


@app.cell
def _(route1):
    route1
    return


@app.cell
def _(con):
    route1_prod = con.sql("""SELECT * from read_parquet('s3://mbta-ctd-dataplatform-springboard/lamp/RT_TRIP_UPDATES/year=2026/month=5/day=16/2026-05-16T00:00:00.parquet')
    where "trip_update.trip.route_id" == '1'""").pl()
    return (route1_prod,)


@app.cell
def _(con):
    route1_prod_vp = con.sql("""SELECT * from read_parquet('s3://mbta-ctd-dataplatform-springboard/lamp/RT_VEHICLE_POSITIONS/year=2026/month=5/day=16/2026-05-16T00:00:00.parquet')
    where "vehicle.trip.route_id" == '1'""").pl()
    return (route1_prod_vp,)


@app.cell
def _(route1_prod_vp):
    route1_prod_vp.write_parquet("prod_vp.parquet")
    return


@app.cell
def _(con):
    route1_dev = con.sql("""SELECT * from read_parquet('s3://mbta-ctd-dataplatform-dev-springboard/lamp/RT_TRIP_UPDATES/year=2026/month=5/day=16/*.parquet')
    where "trip_update.trip.route_id" == '1'""").pl()
    return (route1_dev,)


@app.cell
def _(route1, route1_prod):
    route1_prod.sort('feed_timestamp').equals(route1.sort('feed_timestamp'))
    return


@app.cell
def _(route1):
    route1.describe()
    return


@app.cell
def _(route1, route1_prod):
    route1_prod.describe().equals(route1.describe())
    return


@app.cell
def _(pl, route1_dev, route1_prod):
    route1_prod.describe().equals(route1_dev.unique(subset=pl.exclude('feed_timestamp')).select(route1_prod.columns).describe())
    return


@app.cell
def _(pl, route1_dev, route1_prod):
    route1_dev.unique(subset=pl.exclude('feed_timestamp')).select(route1_prod.columns).describe()
    return


@app.cell
def _():
    return


@app.cell
def _(route1, route1_dev, route1_prod):
    route1.write_parquet('stage.parquet')
    route1_prod.write_parquet('prod.parquet')
    route1_dev.write_parquet('dev.parquet')
    return


@app.cell
def _():
    import pyarrow.dataset as ds
    dset = ds.dataset('s3://mbta-ctd-dataplatform-staging-springboard/lamp/RT_TRIP_UPDATES/year=2026/month=5/day=1/2026-05-01T00:00:00.parquet', format="parquet")

    return (dset,)


@app.cell
def _(dset, pl):
    pl.scan_pyarrow_dataset(dset).filter(pl.col('trip_update.trip.route_id') == "1").collect()
    return


@app.cell
def _(df):
    routes = df.select('trip_update.trip.route_id').unique().collect()
    return (routes,)


@app.cell
def _(df):
    df.columns
    return


@app.cell
def _(routes):
    routes.sort(by='trip_update.trip.route_id')
    return


@app.cell
def _(get_rowgroup_statistics):
    get_rowgroup_statistics("s3://mbta-ctd-dataplatform-staging-springboard/lamp/RT_TRIP_UPDATES/year=2026/month=5/day=1/2026-05-01T00:00:00.parquet")
    return


@app.cell
def _(df, pl):
    df1 = df.filter(pl.col('trip_update.trip.route_id') == "1").collect()
    return


if __name__ == "__main__":
    app.run()
