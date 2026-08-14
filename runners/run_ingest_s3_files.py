from lamp_py.ingestion.ingest_gtfs import ingest_s3_files

# from lamp_py.postgres.postgres_utils import start_rds_writer_process

# metadata_queue, rds_process = start_rds_writer_process()

ingest_s3_files(None, bucket_filter="lamp")
