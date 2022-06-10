#!/usr/bin/env python

import argparse
import logging
import sys
import logging
from typing import NamedTuple
import os
import boto3
import random
from multiprocessing import Pool

from py_gtfs_rt_ingestion import file_list_from_s3

GTFS_BUCKET = 'mbta-gtfs-s3'
DEV_INGEST_BUCKET = 'mbta-ctd-dataplatform-dev-incoming'
DEV_EXPORT_BUCKET = 'mbta-ctd-dataplatform-dev-springboard'
DEV_ARCHIVE_BUCKET = 'mbta-ctd-dataplatform-dev-archive'
DEV_ERROR_BUCKET = 'mbta-ctd-dataplatform-dev-error'
LAMP_PREFIX = 'lamp/'

logging.basicConfig(level=logging.WARNING)

class SetupArgs(NamedTuple):
    src_prefix: str
    objs_to_copy: int

def parseArgs(args) -> dict:
    """
    parse input args from the command line and generate an event dict in the
    format the lambda handler is expecting
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--source-prefix',
        dest='src_prefix',
        type=str,
        default='',
        help='prefix of objects to move into dev environment')

    parser.add_argument(
        '--num',
        dest='objs_to_copy',
        type=int,
        default=1_000,
        help='number of objects to move into dev environment')

    return SetupArgs(**vars(parser.parse_args(args)))

def del_objs(delete_chunk, bucket):
    s3_client = boto3.client('s3')
    delete_objs = {
        'Objects':[{'Key':uri.replace(f"s3://{bucket}/",'')} for uri in delete_chunk]
    }
    try:
        s3_client.delete_objects(
            Bucket=bucket,
            Delete=delete_objs,
        )
    except Exception as e:
        return 0
    return len(delete_chunk)


def clear_dev_buckets(args:SetupArgs):
    bucket_list = (
        DEV_INGEST_BUCKET,
        DEV_EXPORT_BUCKET,
        DEV_ARCHIVE_BUCKET,
        DEV_ERROR_BUCKET,
    )
    
    for bucket in bucket_list:
        uri_root = f"s3://{os.path.join(bucket, LAMP_PREFIX)}"
        action = None
        print(f"Checking for objects to delete in {uri_root}...")
        files_to_delete = list()
        for (uri,size) in file_list_from_s3(bucket, LAMP_PREFIX):
            files_to_delete.append(uri)
        files_to_delete.remove(uri_root)
        if len(files_to_delete) == 0:
            print(f"No objects found... skipping bucket.")
        else: 
            while action is None or action not in ('n','no'):
                if action in ('list','ls'):
                    print(f"/{'*'*50}/")
                    for uri in files_to_delete:
                        print(str(uri).replace(f"s3://{os.path.join(bucket,LAMP_PREFIX)}",""))
                    print(f"/{'*'*50}/")
                elif action in ('y','yes'):
                    chunk_size = 750
                    pool = Pool(os.cpu_count())
                    results = pool.starmap_async(del_objs, [(files_to_delete[i:i+chunk_size], bucket) for i in range(0, len(files_to_delete), chunk_size)])
                    delete_count = sum([r for r in results.get()])
                    print(f"{delete_count} of {len(files_to_delete)}  deleted")
                    break
                print(f"{len(files_to_delete)} objects found, delete?\nyes(y) / no(n) / list(ls)")
                action = input()

def copy_obj(prefix:str, num_to_copy:int):
    count_objs_to_pull = max(num_to_copy * 10, 1_000)
    uri_copy_set = set()
    src_uri_root = f"s3://{os.path.join(GTFS_BUCKET, prefix)}"
    print(f"Pulling list of {count_objs_to_pull:,} objects from {src_uri_root} for random sample...", flush=True)
    for (uri,size) in file_list_from_s3(GTFS_BUCKET, prefix):
        if size > 0 and 'https_mbta_busloc_s3.s3.amazonaws.com_prod_TripUpdates_enhanced' not in uri:
            uri_copy_set.add(uri)
        if len(uri_copy_set) == count_objs_to_pull:
            break

    s3_client = boto3.client('s3')
    success_count = 0
    print(f"Starting copy of {num_to_copy:,} random objects from {src_uri_root} ...", flush=True)
    for uri in random.sample(tuple(uri_copy_set), num_to_copy):
        key = str(uri).replace(f"s3://{GTFS_BUCKET}/",'')
        copy_source = {
            'Bucket':GTFS_BUCKET,
            'Key': key,
        }
        try:
            s3_client.copy(
                copy_source,
                DEV_INGEST_BUCKET,
                os.path.join(LAMP_PREFIX,key),
            )
        except Exception as e:
            print(copy_source, e, flush=True)
        else:
            success_count += 1
    return success_count

def enumerate_s3_folders(client:boto3.client, bucket:str, prefix:str):
    response = client.list_objects_v2(Bucket=bucket, Delimiter = '/', Prefix=prefix, MaxKeys=45)
    if 'CommonPrefixes' not in response:
        yield prefix
    else:
        for new_prefix in response['CommonPrefixes']:
            yield from enumerate_s3_folders(client, bucket, new_prefix['Prefix'])

def copy_gfts_to_ingest(args:SetupArgs):
    
    src_uri_root = f"s3://{os.path.join(GTFS_BUCKET, args.src_prefix)}"
    dest_urc_root = f"s3://{os.path.join(DEV_INGEST_BUCKET, LAMP_PREFIX)}"

    print(f"Enumerating folders in ({src_uri_root})...")
    s3_client = boto3.client('s3')
    bucket_folders = list(enumerate_s3_folders(s3_client, GTFS_BUCKET, args.src_prefix))
    per_folder = args.objs_to_copy // len(bucket_folders)
    remain = args.objs_to_copy % len(bucket_folders)
    print(f"{len(bucket_folders)} folders found, will copy ~{per_folder} objects from each folder.")

    # Get number of files to pull per folder
    bucket_folders = tuple(zip(
        bucket_folders, 
        [per_folder + 1 if x < remain else per_folder for x in range(len(bucket_folders))]
    ))

    action = None
    while action is None or action not in ('n','no'):
        if action in ('y','yes'):
            pool = Pool(os.cpu_count())
            results = pool.starmap_async(copy_obj, bucket_folders)
            success_count = 0
            for result in results.get():
                success_count += result
            print(f"{success_count:,} objects copied, {args.objs_to_copy-success_count:,} errors.")
            break

        print(f"Copy random selection of {args.objs_to_copy:,} objects from {src_uri_root} to {dest_urc_root} ?\nyes(y) / no(n)")
        action = input()

def main(args: SetupArgs) -> None:

    clear_dev_buckets(args)
    copy_gfts_to_ingest(args)

if __name__ == '__main__':
    main(parseArgs(sys.argv[1:])) 


