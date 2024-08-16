import os

from lamp_py.aws.s3 import upload_file
from lamp_py.runtime_utils.remote_files import S3_PUBLIC


def publish_performance_index() -> None:
    """
    Upload index.html to https://performancedata.mbta.com bucket
    """
    here = os.path.dirname(os.path.abspath(__file__))
    index_file = "index.html"

    if S3_PUBLIC == "":
        return

    local_index_path = os.path.join(here, index_file)
    upload_index_path = os.path.join(S3_PUBLIC, index_file)

    extra_args = {
        "ContentType": "text/html",
    }

    upload_file(
        file_name=local_index_path,
        object_path=upload_index_path,
        extra_args=extra_args,
    )
