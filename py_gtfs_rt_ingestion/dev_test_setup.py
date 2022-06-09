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

logging.basicConfig(level=logging.WARNING)

class SetupArgs(NamedTuple):
    dev_prefix: str
    src_prefix: str
    objs_to_copy: int

def parseArgs(args) -> dict:
    """
    parse input args from the command line and generate an event dict in the
    format the lambda handler is expecting
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--dev-prefix',
        dest='dev_prefix',
        type=str,
        default='lamp/',
        help='prefix used to save objects in dev environment (lamp) by DEFAULT')

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
        default=10_000,
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
        uri_root = f"s3://{os.path.join(bucket, args.dev_prefix)}"
        action = None
        print(f"Checking for objects to delete in {uri_root}...")
        files_to_delete = tuple(uri for (uri,size) in file_list_from_s3(bucket, args.dev_prefix) if size > 0)
        if len(files_to_delete) == 0:
            print(f"No objects found... skipping bucket.")
        else: 
            while action is None or action not in ('n','no'):
                if action in ('list','ls'):
                    for uri in files_to_delete:
                        print(str(uri).replace(f"s3://{os.path.join(bucket,args.dev_prefix)}",""))
                elif action in ('y','yes'):
                    chunk_size = 750
                    pool = Pool(os.cpu_count())
                    results = pool.starmap_async(del_objs, [(files_to_delete[i:i+chunk_size], bucket) for i in range(0, len(files_to_delete), chunk_size)])
                    delete_count = sum([r for r in results.get()])
                    print(f"{delete_count} of {len(files_to_delete)}  deleted")
                    break
                print(f"{len(files_to_delete)} objects found, delete?\nyes(y) / no(n) / list(ls)")
                action = input()

def copy_obj(obj, args):
    s3_client = boto3.client('s3')
    key = str(obj).replace(f"s3://{GTFS_BUCKET}/",'')
    copy_source = {
        'Bucket':GTFS_BUCKET,
        'Key': key,
    }
    try:
        s3_client.copy(
            copy_source,
            DEV_INGEST_BUCKET,
            os.path.join(args.dev_prefix,key),
        )
    except Exception as e:
        print(copy_source, e, flush=True)
        return 0
    return 1

def enumerate_s3_folders(client:boto3.client, bucket:str, prefix:str):
    response = client.list_objects_v2(Bucket=bucket, Delimiter = '/', Prefix=prefix, MaxKeys=45)
    if 'CommonPrefixes' not in response:
        yield prefix
    else:
        for new_prefix in response['CommonPrefixes']:
            yield from enumerate_s3_folders(client, bucket, new_prefix['Prefix'])

def copy_gfts_to_ingest(args:SetupArgs):
    count_objs_to_pull = max(args.objs_to_copy * 10, 10_000)
    src_uri_root = f"s3://{os.path.join(GTFS_BUCKET, args.src_prefix)}"
    dest_urc_root = f"s3://{os.path.join(DEV_INGEST_BUCKET, args.dev_prefix)}"

    print(f"Enumerating folders in {src_uri_root}...")
    s3_client = boto3.client('s3')
    bucket_folders = list(enumerate_s3_folders(s3_client, GTFS_BUCKET, args.src_prefix))
    print(f"{len(bucket_folders)} folders found.")

    print(f"Pulling list of {count_objs_to_pull:,} objects from {src_uri_root} for random sample...")
    list_of_objs_to_copy = []
    for (uri,size) in file_list_from_s3(GTFS_BUCKET, args.src_prefix):
        if size > 0:
            list_of_objs_to_copy.append(uri)
        if len(list_of_objs_to_copy) % 10_000 == 0:
            print(f"{len(list_of_objs_to_copy):,} found so far...")
        if len(list_of_objs_to_copy) == count_objs_to_pull:
            break

    action = None
    while action is None or action not in ('n','no'):
        if action in ('y','yes'):
            print(f"Starting copy of {len(list_of_objs_to_copy):,} objects...", flush=True)
            pool = Pool(os.cpu_count())
            results = pool.starmap_async(copy_obj, [(obj, args) for obj in random.sample(list_of_objs_to_copy, args.objs_to_copy)])
            success_count = 0
            error_count = 0
            for result in results.get():
                if result == 1:
                    success_count += 1
                else:
                    error_count += 1
            print(f"{success_count:,} objects copied, {error_count:,} errors.")
            break

        print(f"Copy random selection of {args.objs_to_copy:,} objects from {src_uri_root} to {dest_urc_root} ?\nyes(y) / no(n)")
        action = input()

def main(args: SetupArgs) -> None:

    clear_dev_buckets(args)
    copy_gfts_to_ingest(args)

if __name__ == '__main__':
    main(parseArgs(sys.argv[1:])) 


