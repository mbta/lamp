import boto3
import logging

from collections.abc import Iterable

def file_list_from_s3(bucket_name: str,
                      file_prefix: str) -> Iterable[(str, int)]:
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
