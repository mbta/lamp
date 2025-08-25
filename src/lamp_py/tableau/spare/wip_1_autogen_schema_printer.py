import os
import polars as pl
import polars as plspringboard_spare_admins
from pyarrow.fs import S3FileSystem
import pyarrow.parquet as pq
import pyarrow.dataset as pd
import pyarrow.compute as pc

from lamp_py.aws.s3 import file_list_from_s3
from lamp_py.runtime_utils.remote_files import S3_ARCHIVE, S3_SPRINGBOARD, S3Location
from lamp_py.runtime_utils.remote_files_spare import SPARE, SPARE_TABLEAU

### --- THIS FILE IS AUTOGEN FROM AUTOGEN_S3_REMOTE.PY --- DO NOT MODIFY --- ###
### --- THIS FILE IS AUTOGEN FROM AUTOGEN_S3_REMOTE.PY --- DO NOT MODIFY --- ###
### --- THIS FILE IS AUTOGEN FROM AUTOGEN_S3_REMOTE.PY --- DO NOT MODIFY --- ###


springboard_spare_admins = S3Location(bucket=S3_SPRINGBOARD,prefix=os.path.join(SPARE, "admins.parquet"))
tableau_spare_admins = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "admins.parquet"))
springboard_spare_appointmentTypes = S3Location(bucket=S3_SPRINGBOARD,prefix=os.path.join(SPARE, "appointmentTypes.parquet"))
tableau_spare_appointmentTypes = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "appointmentTypes.parquet"))
# springboard_spare_appointments = S3Location(bucket=S3_SPRINGBOARD,prefix=os.path.join(SPARE, "appointments.parquet"))
# tableau_spare_appointments = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "appointments.parquet"))
springboard_spare_appointments = S3Location(bucket=S3_SPRINGBOARD,prefix=os.path.join(SPARE, "appointments/"))
tableau_spare_appointments = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "appointments.parquet"))
springboard_spare_caseForms = S3Location(bucket=S3_SPRINGBOARD,prefix=os.path.join(SPARE, "caseForms/"))
tableau_spare_caseForms = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "spare_testing.parquet"))
springboard_spare_caseLetters = S3Location(bucket=S3_SPRINGBOARD,prefix=os.path.join(SPARE, "caseLetters/"))
tableau_spare_caseLetters = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "spare_testing.parquet"))

spare_resources = {}
spare_resources['admins'] = (springboard_spare_admins, tableau_spare_admins)
spare_resources['appointmentTypes'] = (springboard_spare_appointmentTypes, tableau_spare_appointmentTypes)
spare_resources['appointments'] = (springboard_spare_appointments, tableau_spare_appointments)

with open("src/lamp_py/tableau/spare/2_autogen_spare_schemas.py", "w+") as ff:

    ff.write(
        """
from polars import Schema
from polars import String, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, List, Struct, Boolean, Categorical, Float64
"""
    )

    try:
        s3_uris = file_list_from_s3(
            bucket_name=springboard_spare_admins.bucket, file_prefix=springboard_spare_admins.prefix
        )
        ds_paths = [s.replace("s3://", "") for s in s3_uris]

        ds = pd.dataset(
            ds_paths,
            format="parquet",
            filesystem=S3FileSystem(),
        )

        for batch in ds.to_batches(batch_size=500_000, filter=None):
            df = pl.from_arrow(batch)
            print("### admins ###", file=ff)
            # print(df.head(1), file=ff)
            print("spare_raw_input_schema_admins = ", df.collect_schema(), file=ff)
            break
    except:
        print(f"admins FAILED!")

    try:
        s3_uris = file_list_from_s3(
            bucket_name=springboard_spare_appointmentTypes.bucket, file_prefix=springboard_spare_appointmentTypes.prefix
        )
        ds_paths = [s.replace("s3://", "") for s in s3_uris]

        ds = pd.dataset(
            ds_paths,
            format="parquet",
            filesystem=S3FileSystem(),
        )

        for batch in ds.to_batches(batch_size=500_000, filter=None):
            df = pl.from_arrow(batch)
            print("### appointmentTypes ###", file=ff)
            # print(df.head(1), file=ff)
            print("spare_raw_input_schema_appointmentTypes = ", df.collect_schema(), file=ff)
            break
    except:
        print(f"appointmentTypes FAILED!")

    try:
        s3_uris = file_list_from_s3(
            bucket_name=springboard_spare_appointments.bucket, file_prefix=springboard_spare_appointments.prefix
        )
        ds_paths = [s.replace("s3://", "") for s in s3_uris]

        ds = pd.dataset(
            ds_paths,
            format="parquet",
            filesystem=S3FileSystem(),
        )

        for batch in ds.to_batches(batch_size=500_000, filter=None):
            df = pl.from_arrow(batch)
            print("### appointments ###", file=ff)
            # print(df.head(1), file=ff)
            print("spare_raw_input_schema_appointments = ", df.collect_schema(), file=ff)
            break
    except:
        print(f"appointments FAILED!")

    try:
        s3_uris = file_list_from_s3(
            bucket_name=springboard_spare_appointments.bucket, file_prefix=springboard_spare_appointments.prefix
        )
        ds_paths = [s.replace("s3://", "") for s in s3_uris]

        ds = pd.dataset(
            ds_paths,
            format="parquet",
            filesystem=S3FileSystem(),
        )

        for batch in ds.to_batches(batch_size=500_000, filter=None):
            df = pl.from_arrow(batch)
            print("### appointments ###", file=ff)
            # print(df.head(1), file=ff)
            print("spare_raw_input_schema_appointments = ", df.collect_schema(), file=ff)
            break
    except:
        print(f"appointments FAILED!")

    try:
        s3_uris = file_list_from_s3(
            bucket_name=springboard_spare_caseForms.bucket, file_prefix=springboard_spare_caseForms.prefix
        )
        ds_paths = [s.replace("s3://", "") for s in s3_uris]

        ds = pd.dataset(
            ds_paths,
            format="parquet",
            filesystem=S3FileSystem(),
        )

        for batch in ds.to_batches(batch_size=500_000, filter=None):
            df = pl.from_arrow(batch)
            print("### caseForms ###", file=ff)
            # print(df.head(1), file=ff)
            print("spare_raw_input_schema_caseForms = ", df.collect_schema(), file=ff)
            break
    except:
        print(f"caseForms FAILED!")

    try:
        s3_uris = file_list_from_s3(
            bucket_name=springboard_spare_caseLetters.bucket, file_prefix=springboard_spare_caseLetters.prefix
        )
        ds_paths = [s.replace("s3://", "") for s in s3_uris]

        ds = pd.dataset(
            ds_paths,
            format="parquet",
            filesystem=S3FileSystem(),
        )

        for batch in ds.to_batches(batch_size=500_000, filter=None):
            df = pl.from_arrow(batch)
            print("### caseLetters ###", file=ff)
            # print(df.head(1), file=ff)
            print("spare_raw_input_schema_caseLetters = ", df.collect_schema(), file=ff)
            break
    except:
        print(f"caseLetters FAILED!")

ff.close()
