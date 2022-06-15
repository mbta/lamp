#!/usr/bin/env python

import argparse
import logging
import sys
from typing import NamedTuple
import os
import boto3
import random
from multiprocessing import Pool
from collections.abc import Iterable

from py_gtfs_rt_ingestion import file_list_from_s3

GTFS_BUCKET = "mbta-gtfs-s3"
DEV_INGEST_BUCKET = "mbta-ctd-dataplatform-dev-incoming"
DEV_EXPORT_BUCKET = "mbta-ctd-dataplatform-dev-springboard"
DEV_ARCHIVE_BUCKET = "mbta-ctd-dataplatform-dev-archive"
DEV_ERROR_BUCKET = "mbta-ctd-dataplatform-dev-error"
LAMP_PREFIX = "lamp/"

logging.basicConfig(level=logging.WARNING)

class SetupArgs(NamedTuple):
    src_prefix: str
    objs_to_copy: int
    force_delete: bool

def parseArgs(args) -> dict:
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
        default="",
        help="prefix of objects to move into dev environment")

    parser.add_argument(
        "-n",
        "--num",
        dest="objs_to_copy",
        type=int,
        default=1_000,
        help="number of objects to move into dev environment")

    parser.add_argument(
        "-f",
        dest="force_delete",
        default=False,
        action="store_true",
        help="delete objects from dev buckets without asking")

    return SetupArgs(**vars(parser.parse_args(args)))

def del_objs(del_list:list[str], bucket:str) -> int:
    s3_client = boto3.client("s3")
    delete_objs = {
        "Objects":[{"Key":uri.replace(f"s3://{bucket}/","")} for uri in del_list]
    }
    try:
        s3_client.delete_objects(
            Bucket=bucket,
            Delete=delete_objs,
        )
    except Exception as e:
        return 0
    return len(del_list)

def make_del_jobs(files:list[str], bucket:str) -> int:
    chunk = 750
    pool = Pool(os.cpu_count())
    l = [(files[i:i+chunk], bucket) for i in range(0, len(files), chunk)]
    results = pool.starmap_async(del_objs, l)
    return sum([r for r in results.get()])

def get_del_obj_list(bucket:str, uri_root:str) -> list[str]:
    print("Checking for objects to delete in %s..." % uri_root, flush=True)
    files_to_delete = list()
    for (uri,size) in file_list_from_s3(bucket, LAMP_PREFIX):
        files_to_delete.append(uri)
    # Remove uri_root from list so that root directory is not deleted.
    files_to_delete.remove(uri_root)
    return sorted(files_to_delete, reverse=True)


def clear_dev_buckets(args:SetupArgs) -> None:
    bucket_list = (
        DEV_INGEST_BUCKET,
        DEV_EXPORT_BUCKET,
        DEV_ARCHIVE_BUCKET,
        DEV_ERROR_BUCKET,
    )
    
    for bucket in bucket_list:
        action = None
        uri_root = f"s3://{os.path.join(bucket, LAMP_PREFIX)}"
        files_to_delete = get_del_obj_list(bucket, uri_root)
        if len(files_to_delete) == 0:
            print("No objects found... skipping bucket.")
        else: 
            while action not in ("n","no"):
                # Print list of bucket objects
                if action in ("list","ls"):
                    print("/%s/" % "*"*25)
                    for uri in files_to_delete:
                        print("%s" % uri.replace(uri_root,""))
                    print("/%s/" % "*"*25)
                # Proceed with object deletion 
                elif action in ("y","yes") or args.force_delete:
                    delete_count = make_del_jobs(files_to_delete, bucket)
                    # If not all objects deleted, retry
                    if delete_count < len(files_to_delete):
                        print("Only %d of %d deleted... will retry." %
                                     (delete_count,
                                     len(files_to_delete)))
                        files_to_delete = get_del_obj_list(bucket)
                    else:
                        print("All %d deleted" % len(files_to_delete))
                        break

                msg = (
                    "%d objects found, delete? \n"
                    "yes(y) / no(n) / list(ls)"
                ) % len(files_to_delete)
                print(msg, flush=True)
                action = input()

def copy_obj(prefix:str, num_to_copy:int) -> int:
    count_objs_to_pull = max(num_to_copy * 10, 1_000)
    uri_copy_set = set()
    src_uri_root = f"s3://{os.path.join(GTFS_BUCKET, prefix)}"
    skip_uri = "https_mbta_busloc_s3.s3.amazonaws.com_prod_TripUpdates_enhanced"
    print("Pulling list of %d objects from %s for random sample..." % 
                 (count_objs_to_pull,
                 src_uri_root))
    try:
        for (uri,size) in file_list_from_s3(GTFS_BUCKET, prefix):
            # Skip busloc TripUpdates because of S3 permission issues
            if size > 0 and skip_uri not in uri:
                uri_copy_set.add(uri)
            if len(uri_copy_set) == count_objs_to_pull:
                break
    except Exception as e:
        logging.error("Unable to pull file list from %s", src_uri_root)
        return 0

    s3_client = boto3.client("s3")
    success_count = 0
    print("Starting copy of %d random objects from %s ..." %
                 (num_to_copy, 
                 src_uri_root))
    for uri in random.sample(tuple(uri_copy_set), num_to_copy):
        key = str(uri).replace(f"s3://{GTFS_BUCKET}/","")
        copy_source = {
            "Bucket":GTFS_BUCKET,
            "Key": key,
        }
        try:
            s3_client.copy(
                copy_source,
                DEV_INGEST_BUCKET,
                os.path.join(LAMP_PREFIX,key),
            )
        except Exception as e:
            logging.error("Copy failed for: %s", key)
            logging.exception(e)
        else:
            success_count += 1
    return success_count

def drill_s3_folders(client:boto3.client, 
                     bucket:str, 
                     prefix:str) -> Iterable[str]:
    response = client.list_objects_v2(Bucket=bucket, 
                                      Delimiter = "/", 
                                      Prefix=prefix, 
                                      MaxKeys=45)
    if "CommonPrefixes" not in response:
        yield prefix
    else:
        for new_prefix in response["CommonPrefixes"]:
            yield from drill_s3_folders(client, bucket, new_prefix["Prefix"])

def copy_gfts_to_ingest(args:SetupArgs):
    
    src_uri_root = f"s3://{os.path.join(GTFS_BUCKET, args.src_prefix)}"
    dest_urc_root = f"s3://{os.path.join(DEV_INGEST_BUCKET, LAMP_PREFIX)}"

    print("Enumerating folders in (%s)..." % src_uri_root)
    s3_client = boto3.client("s3")
    folders = list(drill_s3_folders(s3_client, 
                                    GTFS_BUCKET, 
                                    args.src_prefix))
    # Get number of files to pull per folder
    per_folder = args.objs_to_copy // len(folders)
    # Number of folders that will have extra file
    remain = args.objs_to_copy % len(folders)
    print("%d folders found, will copy ~%d objects from each folder." %
          (len(folders), 
           per_folder))

    # Combine list of folders and number of files to pull from each folder
    folders = tuple(zip(
        folders, 
        [per_folder+1 if x < remain else per_folder for x in range(len(folders))]
    ))

    action = None
    while action not in ("n","no"):
        if action in ("y","yes"):
            pool = Pool(os.cpu_count()*2)
            results = pool.starmap_async(copy_obj, folders)
            success_count = 0
            for result in results.get():
                success_count += result
            print("%d objects copied, %d errors." %
                         (success_count, 
                         args.objs_to_copy-success_count))
            break

        msg = (
            "Copy random selection of %d objects from %s to %s ?\n"
            "yes(y) / no(n)"
        ) % (args.objs_to_copy, src_uri_root, dest_urc_root)
        print(msg, flush=True)
        action = input()

def main(args: SetupArgs) -> None:

    clear_dev_buckets(args)
    copy_gfts_to_ingest(args)

if __name__ == "__main__":
    main(parseArgs(sys.argv[1:])) 
