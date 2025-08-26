# type: ignore
# pylint: skip-file

# prefix constants
import os
from pyarrow.fs import S3FileSystem
import pyarrow.parquet as pq
import pyarrow.dataset as pd
import pyarrow.compute as pc
import polars as pl
from lamp_py.aws.s3 import file_list_from_s3


SPARE = "spare"
SPARE_PROCESSED = os.path.join(SPARE, "processed")
SPARE_TABLEAU = os.path.join(SPARE, "tableau")

VERSION_KEY = "spare_version"

# src/lamp_py/utils/autogen_s3_remote.py
# src/lamp_py/tableau/spare/schema_printer.py
singles = [
    "admins",
    "appointmentTypes",
    # "appointments",
    "appointments_with_history",
    # "caseForms",
    "caseForms_with_history",
    # "caseLetters",
    "caseLetters_with_history",
    "caseStatuses",
    "caseTypes",
    # "cases",
    "cases_with_history",
    # "charges",
    "charges_with_history",
    # "constraintOverrideActions",
    "constraintOverrideActions_with_history",
    "drivers",
    # "duties",
    "duties_with_history",
    # "favoriteLocations",
    "favoriteLocations_with_history",
    "fleets",
    "forms",
    "groupConditions",
    # "groupMemberships",
    "groups",
    "letters",
    "paymentMethodTypes",
    # "paymentMethods",
    "quickReplies",
    # "requestConstraintOverrides",
    "requestConstraintOverrides_with_history",
    # "requestRecurrences",
    # "requests",
    "requests_with_history",
    # "riders",
    "services",
    "stops",
    "timeRules",
    "userBans",
    # "userFleetAgreements",
    "vehicleTypes",
    "vehicles",
    # "walletTransactions",
    "walletTransactions_with_history",
    "zones",
]

multis = [
    "appointments",
    "caseForms",
    "caseLetters",
    "cases",
    "charges",
    "constraintOverrideActions",
    "duties",
    # "fallback",
    "favoriteLocations",
    "groupMemberships",
    "paymentMethods",
    "requestConstraintOverrides",
    "requestRecurrences",
    "requests",
    "riders",
    "userFleetAgreements",
    # "vehicleLocation",
    "vehicleLocations",
    "walletTransactions",
]


# files ingested from SPARE
def template_springboard_single_input(name):
    return f'springboard_spare_{name} = S3Location(bucket=S3_SPRINGBOARD,prefix=os.path.join(SPARE, "{name}.parquet"))'


def template_springboard_prefix_input(name):
    return f'springboard_spare_{name} = S3Location(bucket=S3_SPRINGBOARD,prefix=os.path.join(SPARE, "{name}/"))'


def template_resource_map_all_resources(name):
    return f'spare_resources["{name}"] = (springboard_spare_{name}, tableau_spare_{name})'


# files output from SPARE
def template_springboard_output(name):
    return f'tableau_spare_{name} = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "{name}.parquet"))'


# def template_springboard_prefix_output(name):
#     f"tableau_spare_{name} = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, \"spare_testing.parquet\")"


def filter_gen(name):
    return f"""
def convert_spare_{name}_for_tableau() -> pyarrow.schema:
    pass
"""


with open("src/lamp_py/tableau/spare/3_autogen_spare_tableau_converters.py", "w+") as f:
    # create empty converters
    f.write("import pyarrow")
    for name in singles:
        print(filter_gen(name), file=f)
    for name in multis:
        print(filter_gen(name), file=f)


# def auto_flatten():
#     for list in types:
#     explode
#     flatten
#     return
def read_schema_template(name) -> None:

    return f"""
    try:
        s3_uris = file_list_from_s3(bucket_name=springboard_spare_{name}.bucket, file_prefix=springboard_spare_{name}.prefix)
        ds_paths = [s.replace("s3://", "") for s in s3_uris]

        ds = pd.dataset(
            ds_paths,
            format="parquet",
            filesystem=S3FileSystem(),
        )

        
        for batch in ds.to_batches(batch_size=500_000, filter=None):
            df = pl.from_arrow(batch)
            print("### {name} ###", file=ff)
            #print(df.head(1), file=ff)
            print("spare_raw_input_schema_{name} = ", df.collect_schema(), file=ff)
            break
    except:
        print(f"{name} FAILED!")
"""


# with pq.ParquetWriter(already_flattened, schema=ds.schema) as writer:
#         for batch in ds.to_batches(batch_size=500_000, filter=filter):
#             # don't write empty batch if no rows
#             if batch.num_rows == 0:
#                 continue

#             writer.write_batch(batch)
#             print('wrote something')
#             # breakpoint()

#     return f"""
# output_schema = get_default_tableau_schema_from_s3(
#         springboard_spare_vehicles,
#     )
# print(output_schema)

# print(pl.read_parquet_schema({}))
# # breakpoint()

# input = OrderedDict([('id', String), ('identifier', String), ('ownerUserId', String), ('ownerType', String), ('make', String), ('model', String), ('color', String), ('licensePlate', String), ('passengerSeats', UInt32), ('accessibilityFeatures', List(Struct({'type': String, 'count': UInt32, 'seatCost': UInt32, 'requireFirstInLastOut': Boolean}))), ('status', String), ('metadata', String), ('metadata.vin', String), ('metadata.providerId', Categorical(String)), ('metadata.owner', Categorical(String)), ('metadata.year', UInt32), ('metadata.comments', String), ('emissionsRate', UInt64), ('vehicleTypeId', String), ('capacityType', String), ('createdAt', UInt64), ('updatedAt', UInt64), ('photoUrl', String)])
# input_df = pl.Schema(schema=input).to_frame()
# output_df = pl.Schema(schema=output_schema).to_frame()
# assert vehicle.schema == input_df.schema
# print("done")
# """

with open("src/lamp_py/tableau/spare/1_autogen_schema_printer.py", "w+") as f:
    # f.write("")

    f.write(
        """
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

spare_resources = {}

"""
    )

    for name in singles:
        print(template_springboard_single_input(name), file=f)
        print(template_springboard_output(name), file=f)
        print(template_resource_map_all_resources(name), file=f)
    for name in multis:
        print(template_springboard_prefix_input(name), file=f)
        print(template_springboard_output(name), file=f)
        print(template_resource_map_all_resources(name), file=f)

    # INCEPTION

    f.write(
        """
with open('src/lamp_py/tableau/spare/2_autogen_spare_schemas.py', 'w+') as ff:

    ff.write(\"\"\"
from polars import Schema
from polars import String, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, List, Struct, Boolean, Categorical, Float64
\"\"\")
"""
    )

    # create empty converters
    for name in singles:
        print(read_schema_template(name), file=f)
    for name in multis:
        print(read_schema_template(name), file=f)
