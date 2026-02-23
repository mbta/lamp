from datetime import datetime, timedelta, timezone
from lamp_py.aws.s3 import file_list_from_s3_with_details, get_s3_client
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import LAMP, S3_SPRINGBOARD


def runner(delete: bool = True) -> None:
    """Rename springboard datasets with LRTP to TP."""
    ad_hoc_logger = ProcessLogger(process_name="ad_hoc_runner")
    ad_hoc_logger.log_start()

    lrtp_file_list = file_list_from_s3_with_details(
        S3_SPRINGBOARD,
        f"{LAMP}/DEV_GREEN_LRTP",
    ) + file_list_from_s3_with_details(
        S3_SPRINGBOARD,
        f"{LAMP}/LRTP",
    )

    ad_hoc_logger.add_metadata(lrtp_files=len(lrtp_file_list))

    s3 = get_s3_client()

    for file in lrtp_file_list:
        existing_key = file["s3_obj_path"].replace(f"s3://{S3_SPRINGBOARD}/", "")
        if datetime.now(timezone.utc) - file["last_modified"] > timedelta(
            hours=6
        ):  # skip anything modified today to avoid overwriting unfinished files
            new_key = existing_key.replace("LRTP", "TP")
            s3.copy({"Bucket": S3_SPRINGBOARD, "Key": existing_key}, S3_SPRINGBOARD, new_key)
            ad_hoc_logger.add_metadata(original_key=existing_key, new_key=new_key)
        else:
            ad_hoc_logger.add_metadata(skipped_key=existing_key, reason="File is not old enough to move")

    # move all file before deleting originals
    if delete:
        for file in lrtp_file_list:
            existing_key = file["s3_obj_path"].replace(f"s3://{S3_SPRINGBOARD}/", "")
            if datetime.now(timezone.utc) - file["last_modified"] > timedelta(hours=6):
                s3.delete_object(Bucket=S3_SPRINGBOARD, Key=existing_key)
                ad_hoc_logger.add_metadata(deleted_key=existing_key)
            else:
                ad_hoc_logger.add_metadata(skipped_deletion_key=existing_key, reason="File is not old enough to delete")
