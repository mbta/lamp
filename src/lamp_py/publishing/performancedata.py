import os

from lamp_py.aws.s3 import upload_file


def publish_performance_index() -> None:
    """
    Upload index.html to https://performancedata.mbta.com bucket
    """
    bucket = os.environ.get("PUBLIC_ARCHIVE_BUCKET", "")
    here = os.path.dirname(os.path.abspath(__file__))
    index_file = "index.html"

    if bucket == "":
        return

    local_index_path = os.path.join(here, index_file)
    upload_index_path = os.path.join(bucket, index_file)

    extra_args = {
        "ContentType": "text/html",
    }

    upload_file(
        file_name=local_index_path,
        object_path=upload_index_path,
        extra_args=extra_args,
    )
