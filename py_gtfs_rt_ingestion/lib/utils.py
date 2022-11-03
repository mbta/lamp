import os
import logging
import pathlib

from typing import List, Dict


def load_environment() -> None:
    """
    boostrap .env file for local development

    Note: the logging doesn't matter as much in this function since its only
    used when running scripts locally, so it should never make its way to
    splunk.
    """
    try:
        if int(os.environ.get("BOOTSTRAPPED", 0)) == 1:
            return

        here = os.path.dirname(os.path.abspath(__file__))
        env_file = os.path.join(here, "..", "..", ".env")
        logging.info("bootstrapping with env file %s", env_file)

        with open(env_file, "r", encoding="utf8") as reader:
            for line in reader.readlines():
                line = line.rstrip("\n")
                line.replace('"', "")
                if line.startswith("#") or line == "":
                    continue
                key, value = line.split("=")
                logging.info("setting %s to %s", key, value)
                os.environ[key] = value

    except Exception as exception:
        logging.exception("error while trying to bootstrap")
        raise exception


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
    grouped_files: Dict[str, List[str]] = {}

    for file in filepaths:
        _, file_type = pathlib.Path(file).name.split("_", maxsplit=1)
        if file_type not in grouped_files:
            grouped_files[file_type] = []
        grouped_files[file_type].append(file)

    for group in grouped_files.values():
        group.sort(key=lambda s3_path: pathlib.Path(s3_path).name)

    return grouped_files
