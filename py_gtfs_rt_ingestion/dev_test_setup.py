#!/usr/bin/env python

import argparse
import logging
import sys
from typing import List, Optional, NamedTuple
import os
import random
from multiprocessing import Pool
from collections.abc import Iterable

import boto3

from lib import file_list_from_s3

GTFS_BUCKET = "mbta-gtfs-s3"
DEV_INCOMING_BUCKET = "mbta-ctd-dataplatform-dev-incoming"
DEV_SPRINGBOARD_BUCKET = "mbta-ctd-dataplatform-dev-springboard"
DEV_ARCHIVE_BUCKET = "mbta-ctd-dataplatform-dev-archive"
DEV_ERROR_BUCKET = "mbta-ctd-dataplatform-dev-error"
LAMP_PREFIX = "lamp/"


class SetupArgs(NamedTuple):
    """
    NamedTuple to hold Setup arguments
    """

    src_prefix: str
    objs_to_copy: int
    no_interaction: bool
    log_level: str


def parse_args(args: List[str]) -> SetupArgs:
    """
    parse input args from the command line and generate an event dict in the
    format the lambda handler is expecting
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--source-prefix",
        dest="src_prefix",
        type=str,
        default="20",
        help="prefix of objects to move into dev environment",
    )

    parser.add_argument(
        "-n",
        "--num",
        dest="objs_to_copy",
        type=int,
        default=5_000,
        help="number of objects to move into dev environment",
    )

    parser.add_argument(
        "-f",
        dest="no_interaction",
        default=False,
        action="store_true",
        help="automatically run script without prompts",
    )

    parser.add_argument(
        "--log-level",
        dest="log_level",
        type=str,
        default="INFO",
        help="logging output level to during script operation",
    )

    return SetupArgs(**vars(parser.parse_args(args)))


def del_objs(del_list: List[str], bucket: str) -> int:
    """
    Delete del_list of objects from bucket
    """
    s3_client = boto3.client("s3")
    delete_objs = {
        "Objects": [
            {"Key": uri.replace(f"s3://{bucket}/", "")} for uri in del_list
        ]
    }
    try:
        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete=delete_objs,
        )
    except Exception as e:
        logging.exception(e)
        return 0
    return len(response["Deleted"])


def make_del_jobs(files: List[str], bucket: str) -> int:
    """
    Spin up Multiprocessing jobs to delete batches of objects
    """
    chunk = 750
    with Pool(os.cpu_count()) as pool:
        chunks_list = [
            (files[i : i + chunk], bucket) for i in range(0, len(files), chunk)
        ]
        results = pool.starmap_async(del_objs, chunks_list)
        del_count = sum(r for r in results.get())
    return del_count


def get_del_obj_list(bucket: str, uri_root: str) -> List[str]:
    """
    Return list of objects in bucket, removing uri_root object from return list
    """
    logging.info("Checking for objects to delete in %s...", uri_root)
    files_to_delete = []
    for uri in file_list_from_s3(bucket, LAMP_PREFIX):
        files_to_delete.append(uri)
    # Remove uri_root from list so that root directory is not deleted.
    files_to_delete.remove(uri_root)
    return sorted(files_to_delete, reverse=True)


def clear_dev_buckets(args: SetupArgs) -> None:
    """
    Clear all development buckets of objects
    """
    bucket_list = (
        DEV_INCOMING_BUCKET,
        DEV_SPRINGBOARD_BUCKET,
        DEV_ARCHIVE_BUCKET,
        DEV_ERROR_BUCKET,
    )

    for bucket in bucket_list:
        action = None
        uri_root = f"s3://{os.path.join(bucket, LAMP_PREFIX)}"
        files_to_delete = get_del_obj_list(bucket, uri_root)
        if len(files_to_delete) == 0:
            logging.info("No objects found... skipping bucket.")
        else:
            while action not in ("n", "no"):
                # Print list of bucket objects
                if action in ("list", "ls"):
                    print(f"/{'*' * 50}/")
                    for uri in files_to_delete:
                        print(f"{uri.replace(uri_root, '')}")
                    print(f"/{'*' * 50}/")
                # Proceed with object deletion
                elif action in ("y", "yes") or args.no_interaction:
                    delete_count = make_del_jobs(files_to_delete, bucket)
                    # If not all objects deleted, retry
                    if delete_count < len(files_to_delete):
                        logging.info(
                            "Only %d of %d deleted... will retry.",
                            delete_count,
                            len(files_to_delete),
                        )
                        files_to_delete = get_del_obj_list(bucket, uri_root)
                    else:
                        logging.info("All %d deleted", len(files_to_delete))
                        break

                print(
                    f"{len(files_to_delete):,} objects found, delete? "
                    "\n"
                    "yes(y) / no(n) / list(ls)"
                )
                action = input()


def copy_obj(prefix: str, num_to_copy: int) -> int:
    """
    Copy random objects from prefix to dev Ingest bucket
    """
    count_objs_to_pull = max(num_to_copy * 10, 1_000)
    uri_copy_set = set()
    src_uri_root = f"s3://{os.path.join(GTFS_BUCKET, prefix)}"
    skip_uri = "https_mbta_busloc_s3.s3.amazonaws.com_prod_TripUpdates_enhanced"
    logging.info(
        "Pulling list of %d objects from %s for random sample...",
        count_objs_to_pull,
        src_uri_root,
    )
    try:
        for uri in file_list_from_s3(GTFS_BUCKET, prefix):
            # Skip busloc TripUpdates because of S3 permission issues
            if skip_uri not in uri:
                uri_copy_set.add(uri)
            if len(uri_copy_set) == count_objs_to_pull:
                break
    except Exception as e:
        logging.error("Unable to pull file list from %s", src_uri_root)
        logging.exception(e)
        return 0

    s3_client = boto3.client("s3")
    success_count = 0
    logging.info(
        "Starting copy of %d random objects from %s ...",
        num_to_copy,
        src_uri_root,
    )
    for uri in random.sample(tuple(uri_copy_set), num_to_copy):
        key = str(uri).replace(f"s3://{GTFS_BUCKET}/", "")
        copy_source = {
            "Bucket": GTFS_BUCKET,
            "Key": key,
        }
        try:
            s3_client.copy(
                copy_source,
                DEV_INCOMING_BUCKET,
                os.path.join(LAMP_PREFIX, key),
            )
        except Exception as e:
            logging.error("Copy failed for: %s", key)
            logging.exception(e)
        else:
            success_count += 1
    return success_count


def drill_s3_folders(
    client: boto3.client, bucket: str, prefix: str
) -> Iterable[Optional[str]]:
    """
    Enumerate folders in specified bucket prefix combination
    """
    try:
        response = client.list_objects_v2(
            Bucket=bucket, Delimiter="/", Prefix=prefix, MaxKeys=45
        )
    except Exception as e:
        logging.exception(e)
        yield None

    if "CommonPrefixes" not in response:
        yield prefix
    else:
        for new_prefix in response["CommonPrefixes"]:
            yield from drill_s3_folders(client, bucket, new_prefix["Prefix"])


def copy_gfts_to_ingest(args: SetupArgs) -> None:
    """
    Enumerate folders in GTFS_BUCKET prefix and create workers to copy
    objects to development Ingest bucket
    """
    src_uri_root = f"s3://{os.path.join(GTFS_BUCKET, args.src_prefix)}"
    dest_urc_root = f"s3://{os.path.join(DEV_INCOMING_BUCKET, LAMP_PREFIX)}"

    logging.info("Enumerating folders in (%s)...", src_uri_root)
    s3_client = boto3.client("s3")
    folders = list(drill_s3_folders(s3_client, GTFS_BUCKET, args.src_prefix))
    folders = list(filter(lambda x: x is not None, folders))
    # Get number of files to pull per folder
    per_folder = args.objs_to_copy // len(folders)
    # Number of folders that will have extra file
    remain = args.objs_to_copy % len(folders)
    logging.info(
        "%d folders found, will copy ~%d objects from each folder.",
        len(folders),
        per_folder,
    )

    # Combine list of folders and number of files to pull from each folder
    folder_pull_cnt = tuple(
        zip(
            folders,
            [
                per_folder + 1 if x < remain else per_folder
                for x in range(len(folders))
            ],
        )
    )

    action = None
    while action not in ("n", "no"):
        if action in ("y", "yes") or args.no_interaction:
            pool_size = os.cpu_count()
            if pool_size is None:
                pool_size = 4
            else:
                pool_size *= 2
            with Pool(pool_size) as pool:
                results = pool.starmap_async(copy_obj, folder_pull_cnt)
                success_count = 0
                for result in results.get():
                    success_count += result
            logging.info(
                "%d objects copied, %d errors.",
                success_count,
                args.objs_to_copy - success_count,
            )
            break

        print(
            f"Copy random selection of {args.objs_to_copy} objects "
            f"from {src_uri_root} to {dest_urc_root} ?\n"
            "yes(y) / no(n)"
        )
        action = input()


def main(args: SetupArgs) -> None:
    """
    Run functions to clear dev buckets and copy objects to ingest bucket.
    """
    # pylint: disable=W0212
    if args.log_level not in tuple(logging._nameToLevel):
        raise KeyError(f"{args.log_level} not a valid log_level")
    logging.basicConfig(level=logging.getLevelName(args.log_level))

    clear_dev_buckets(args)
    copy_gfts_to_ingest(args)


if __name__ == "__main__":
    main(parse_args(sys.argv[1:]))
