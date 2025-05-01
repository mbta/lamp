# type: ignore
import os
from lamp_py.aws.s3 import file_list_from_s3_with_details
from lamp_py.runtime_utils.remote_files import S3_SPRINGBOARD

# Problem: Looking into data quality issues is a very adhoc process right now. Expertise/knowledge
# not centralized in code that is easily runnable (it's mostly in the app itself)

# Solution: # Prism.py (working name...) "See the Rainbow" - WIP entry point to analysis suite to
# organize tools for looking at LAMP data products inputs and outputs

files = file_list_from_s3_with_details(bucket_name="mbta-ctd-dataplatform-staging-archive", file_prefix="lamp/tableau/")

print(files)
breakpoint()

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
