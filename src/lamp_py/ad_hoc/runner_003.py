from lamp_py.aws.s3 import file_list_from_s3, get_s3_client
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import LAMP, S3_SPRINGBOARD


def runner(delete: bool = True) -> None:
    """Rename springboard datasets with LRTP to TP."""
    ad_hoc_logger = ProcessLogger(process_name="ad_hoc_runner")
    ad_hoc_logger.log_start()

    lrtp_file_list = file_list_from_s3(
        S3_SPRINGBOARD,
        f"{LAMP}/DEV_GREEN_LRTP",
    ) + file_list_from_s3(
        S3_SPRINGBOARD,
        f"{LAMP}/LRTP",
    )

    ad_hoc_logger.add_metadata(lrtp_files=len(lrtp_file_list))

    s3 = get_s3_client()

    for file in lrtp_file_list:
        new_key = file.replace("LRTP", "TP")
        s3.copy({"Bucket": S3_SPRINGBOARD, "Key": file}, S3_SPRINGBOARD, new_key)
        ad_hoc_logger.add_metadata(original_key=file, new_key=new_key)

    # move all file before deleting originals
    if delete:
        for file in lrtp_file_list:
            s3.delete_object(Bucket=S3_SPRINGBOARD, Key=file)
            ad_hoc_logger.add_metadata(deleted_key=file)
