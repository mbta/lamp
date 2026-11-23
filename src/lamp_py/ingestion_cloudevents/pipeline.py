from os import environ
from signal import signal, SIGTERM

from deltalake.table import DeltaTable
from lamp_py.aws.s3 import file_list_from_s3_with_details, move_s3_objects
from lamp_py.aws.ecs import handle_ecs_sigterm
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.ingestion_cloudevents.load import ingest, CloudEvent
from lamp_py.ingestion_cloudevents.producers import PRODUCERS
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import S3_ARCHIVE


def select_files(detailed_file_list: list[dict], max_batch_bytes: int = 2_000_000_000) -> list[str]:
    """Filter files by the lesser of maximum number or maximum aggregate file size, starting with the oldest files first."""
    sorted_list = sorted(detailed_file_list, key=lambda obj: obj["last_modified"])

    final_file_list: list[str] = []
    final_batch_size: int = 0
    for obj in sorted_list:
        if final_batch_size + obj["size_bytes"] <= max_batch_bytes:
            final_file_list.append(obj["s3_obj_path"])
            final_batch_size += obj["size_bytes"]

    return final_file_list


def main() -> None:
    """Configure environment and orchestrate event loop."""
    environ["SERVICE_NAME"] = "ingestion_cloudevents"

    process_logger = ProcessLogger("main")
    process_logger.log_start()

    signal(SIGTERM, handle_ecs_sigterm)

    validate_environment(
        required_variables=[
            "SPRINGBOARD_BUCKET",
            "ARCHIVE_BUCKET",
            "ERROR_BUCKET",
            "INCOMING_BUCKET",
        ],
    )

    counter = 0
    while True:
        process_logger.add_metadata(cycle_number=counter)
        for producer in PRODUCERS:
            process_logger.add_metadata(producer=str(producer))
            files_to_ingest = select_files(
                file_list_from_s3_with_details(producer.source.bucket, producer.source.prefix, max_list_size=2),
            )
            try:
                ingest(files_to_ingest, producer.destination)
                move_s3_objects(files_to_ingest, S3_ARCHIVE)
            except Exception as e:
                process_logger.log_failure(e)
        counter += 1
