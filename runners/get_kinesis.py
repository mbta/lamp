from queue import Queue
from lamp_py.aws.kinesis import KinesisReader
from lamp_py.ingestion.glides import ingest_glides_events

r = KinesisReader(stream_name="ctd-glides-prod")

ingest_glides_events(r, Queue(), upload=False)
