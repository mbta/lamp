# type: ignore
import os
from lamp_py.aws.s3 import file_list_from_s3_with_details
from lamp_py.runtime_utils.remote_files import S3_SPRINGBOARD
import polars as pl
import plotly.express as px
import pyarrow.dataset as pd
import pyarrow.parquet as pq
from pyarrow.fs import S3FileSystem

# Problem: Looking into data quality issues is a very adhoc process right now. Expertise/knowledge
# not centralized in code that is easily runnable (it's mostly in the app itself)

# Solution: # Prism.py (working name...) "See the Rainbow" - WIP entry point to analysis suite to
# organize tools for looking at LAMP data products inputs and outputs


def print_file_list_from_s3_with_details(bucket: str, prefix: str) -> None:
    files = file_list_from_s3_with_details(
        bucket_name="mbta-ctd-dataplatform-staging-archive", file_prefix="lamp/tableau/"
    )

    print(files)

    for f in files:
        print(f"{os.path.basename(f['s3_obj_path'])}: sz: {f['size_bytes']} last mod: {f['last_modified']}")


# detect data source from what
# returned object contains methods that are available given the input data

# ideas...
# e.g. prism(some_data_from_springboard)
#     - detect that it is Vehicle Positions file from path
#     - load it up
#     - implementations of various analysis chosen for VP

# https://docs.python.org/3/library/functools.html#functools.singledispatch


def batch_reader(files, parquet_output_path, pa_filter, pl_filter):

    ds_paths = [s.replace("s3://", "") for s in files]

    ds = pd.dataset(
        ds_paths,
        format="parquet",
        filesystem=S3FileSystem(),
    )
    # batch1 = ds.to_batches(batch_size=500_000, filter=filter)
    with pq.ParquetWriter(parquet_output_path, schema=ds.schema) as writer:
        for batch in ds.to_batches(batch_size=500_000, filter=pa_filter):
            if batch.num_rows == 0:
                continue
            if pl_filter is not None:
                batch = pl.from_arrow(batch).filter(pl_filter).to_arrow().cast(ds.schema)
            else:
                batch = pl.from_arrow(batch).to_arrow().cast(ds.schema)
            if batch.num_rows == 0:
                continue
            writer.write_table(batch)

    return


def assert_timepoint_order_timepoint_id_correspond(df: pl.DataFrame):
    """
    Check that timepoint order is not null when timepoint_id is not null, and the...converse (?).
    """
    assert df.with_columns(
        pl.when(
            (pl.col("timepoint_order").is_not_null() & pl.col("timepoint_id").is_not_null())
            | (pl.col("timepoint_order").is_null() & pl.col("timepoint_id").is_null())
        ).then(pl.lit(True).alias("verify_timepoint_order"))
    )["verify_timepoint_order"].all()

    assert (
        df.with_columns(pl.col("timepoint_order").sort().diff().over("trip_id").alias("timepoint_order_diff"))[
            "timepoint_order_diff"
        ]
        .drop_nulls()
        .eq(1)
        .all()
    )


def plot_lla(vp):
    fig3 = px.scatter_map(vp, lat="latitude", lon="longitude", zoom=8, height=300)
    fig3.update_layout(mapbox_style="open-street-map")
    fig3.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    config2 = {"scrollZoom": True}
    return fig3.show(config2)
