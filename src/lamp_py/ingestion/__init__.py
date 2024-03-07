"""
Pipeline for processing ingesting GTFS static schedule files and GTFS real time
files from an s3 bucket. The realtime files are collapsed into parquet files
for long term storage.
"""
