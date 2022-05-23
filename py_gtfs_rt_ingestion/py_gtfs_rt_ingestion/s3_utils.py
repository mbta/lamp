import boto3
import logging
import os

from collections.abc import Iterable
from pathlib import Path

from .error import S3Exception

def file_list_from_s3(bucket_name: str='mbta-gtfs-s3',
                      file_prefix: str=None) -> Iterable[(str, int)]:
    """
    generate filename, filesize tuples for every file in an s3 bucket

    :param bucket_name: the name of the bucket to look inside of
    :param file_prefix: prefix for files to generate

    :yield filename, filesize tuples from inside of the bucket
    """
    logging.info("Getting files with prefix %s from %s" % (file_prefix,
                                                           bucket_name))
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    for bucket_object in bucket.objects.filter(Prefix=file_prefix):
        yield (bucket_object.key, bucket_object.size)

def download_file_from_s3(bucket_name: str,
                          filename: str,
                          destination_dir: str) -> Path:
    """
    simple wrapper to download a file from s3 to a directory.
    """
    logging.info(
        "Downloading %s from s3 bucket %s to %s" % (filename,
                                                    bucket_name,
                                                    destination_dir))
    try:
        s3 = boto3.resource('s3')

        # have to convert filenames, deliminating with . instead of /, or else the
        # move method in the download_file method errors out with "no directory".
        deliminated_filename = filename.replace("/", ".")

        destination = os.path.join(destination_dir, deliminated_filename)
        s3.Bucket(bucket_name).download_file(filename, destination)
        return Path(destination)
    except Exception as exception:
        raise S3Exception("Failed to download %s" % filename) from exception
