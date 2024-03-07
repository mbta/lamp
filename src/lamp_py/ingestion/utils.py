import pathlib
from typing import Dict, List

DEFAULT_S3_PREFIX = "lamp"


def group_sort_file_list(filepaths: List[str]) -> Dict[str, List[str]]:
    """
    group and sort list of filepaths by filename

    expects s3 file paths that can be split on timestamp:

    full_path:
    s3://mbta-ctd-dataplatform-dev-incoming/lamp/delta/2022/10/12/2022-10-12T23:58:52Z_https_cdn.mbta.com_MBTA_GTFS.zip

    splits "2022-10-12T23:58:52Z_https_cdn.mbta.com_MBTA_GTFS.zip"
    from full_path

    into
     - 2022-10-12T23:58:52Z
     - https_cdn.mbta.com_MBTA_GTFS.zip

    groups by "https_cdn.mbta.com_MBTA_GTFS.zip"
    """

    def strip_timestamp(fileobject: str) -> str:
        """
        utility for sorting pulling timestamp string out of file path.
        assumption is that the objects will have a bunch of "directories" that
        pathlib can parse out, and the filename will start with a timestamp
        "YYY-MM-DDTHH:MM:SSZ" (20 char) format.

        This utility will be used to sort the list of objects.
        """
        filepath = pathlib.Path(fileobject)
        return filepath.name[:20]

    grouped_files: Dict[str, List[str]] = {}

    for file in filepaths:
        # skip filepaths that are directories.
        if file[-1] == "/":
            continue

        _, file_type = pathlib.Path(file).name.split("_", maxsplit=1)

        if file_type not in grouped_files:
            grouped_files[file_type] = []

        grouped_files[file_type].append(file)

    for group in grouped_files.values():
        group.sort(key=strip_timestamp)

    return grouped_files
