from typing import List, Dict, Optional
import json

import boto3

from lamp_py.runtime_utils.process_logger import ProcessLogger


class KinesisReader:
    """
    Wrapper class for reading a Kinesis Stream, keeping alive a shard
    """

    def __init__(self, stream_name: str) -> None:
        """
        initialize an instance. shard information and sequence numbers are
        initially empty, to be filled out when needed.
        """
        self.stream_name = stream_name
        self.kinesis_client = boto3.client("kinesis")
        self.shard_id: Optional[str] = None
        self.shard_iterator: Optional[str] = None
        self.last_sequence_number: Optional[str] = None

    def update_shard_id(self) -> None:
        """
        Get the stream description and the shard id for the first shard in the
        Kinesis Stream.  Throws if the stream has more than one shard.
        """
        process_logger = ProcessLogger(
            process_name="update_shard_id", stream_name=self.stream_name
        )
        process_logger.log_start()

        # Describe the stream and pull out the shard IDs
        stream_description = self.kinesis_client.describe_stream(
            StreamName=self.stream_name
        )
        shards = stream_description["StreamDescription"]["Shards"]

        # Per conversation with Glides, their Kinesis Stream only consists of a
        # single shard. If we are consuming a stream with multiple shards, build up
        # logic to distinguish between them.
        assert len(shards) == 1

        # get the shard id and get a shard iterator that will be used to read
        self.shard_id = shards[0]["ShardId"]
        process_logger.log_complete()

    def update_shard_iterator(self) -> None:
        """
        Access the shard iterator for the first shard. If the last sequence
        number is empty, it implies this is the first read from the shard. In
        that case, get the Trim Horizon iterator which is the oldest one in the
        shard. Otherwise, get the next iterator after the last sequence number.
        """
        process_logger = ProcessLogger(
            process_name="update_shard_iterator", stream_name=self.stream_name
        )
        process_logger.log_start()

        if self.shard_id is None:
            self.update_shard_id()

        if self.last_sequence_number is None:
            # get the oldest iterator at the Trim Horizon if we don't have a sequence number
            process_logger.add_metadata(iterator_type="trim_horizon")
            shard_iterator_response = self.kinesis_client.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=self.shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )
        else:
            # get the iterator after the last sequence number if we have one
            process_logger.add_metadata(iterator_type="after_sequence_number")

            shard_iterator_response = self.kinesis_client.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=self.shard_id,
                ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                StartingSequenceNumber=self.last_sequence_number,
            )

        self.shard_iterator = shard_iterator_response["ShardIterator"]
        process_logger.log_complete()

    def get_records(self) -> List[Dict]:
        """
        Use a shard iterator to read records. The response contains both the
        records that can be processed and the next shard iterator to use for the
        next read.
        """
        process_logger = ProcessLogger(
            process_name="kinesis.get_records", stream_name=self.stream_name
        )
        process_logger.log_start()

        if self.shard_iterator is None:
            self.update_shard_iterator()

        all_records = []
        shard_count = 0

        while True:
            try:
                response = self.kinesis_client.get_records(
                    ShardIterator=self.shard_iterator
                )
                shard_count += 1
                self.shard_iterator = response["NextShardIterator"]
                records = response["Records"]

                for record in records:
                    self.last_sequence_number = record["SequenceNumber"]
                    all_records.append(json.loads(record["Data"]))

                if response["MillisBehindLatest"] == 0:
                    break

            # thrown if the shard iterator has expired. catch it and regenerate
            # a new shard iterator.
            except self.kinesis_client.exceptions.ExpiredIteratorException:
                self.update_shard_iterator()

        process_logger.add_metadata(
            record_count=len(all_records), shard_count=shard_count
        )
        process_logger.log_complete()

        return all_records
