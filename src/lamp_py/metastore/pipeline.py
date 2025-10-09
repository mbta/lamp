import os

from lamp_py.aws.s3 import upload_file
from lamp_py.metastore.create import create_read_date_partitioned, create_timestamp_partitioned_views, save_db_locally
from lamp_py.metastore.authenticate import authenticate_s3

LOCAL_DUCKDB_PATH = "/tmp/lamp.db"
REMOTE_S3_PATH = "s3://mbta-ctd-dataplatform-dev-archive/lamp/catalog.db"

def refresh_metastore() -> None:
    authenticate_s3()
    create_timestamp_partitioned_views()
    create_read_date_partitioned()
    local_path = save_db_locally(LOCAL_DUCKDB_PATH)
    upload_file(
        local_path,
        REMOTE_S3_PATH
    )
    os.remove(local_path)

if __name__ == "__main__":
    refresh_metastore()
