from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import LAMP
from lamp_py.runtime_utils.remote_files import S3_SPRINGBOARD
from lamp_py.aws.s3 import file_list_from_s3, get_s3_client


def runner() -> None:
    """
    Rename springboard datasets with LRTP to TP.
    """
    ad_hoc_logger = ProcessLogger(process_name="ad_hoc_runner")
    ad_hoc_logger.log_start()

    lrtp_file_list = file_list_from_s3(
        S3_SPRINGBOARD,
        f"{LAMP}/*LRTP*/**",
    )

    ad_hoc_logger.add_metadata(lrtp_files = len(lrtp_file_list))

    s3 = get_s3_client()

    for file in lrtp_file_list:
        s3.copy(
            {
                "Bucket": S3_SPRINGBOARD,
                "Key": file
            },
            S3_SPRINGBOARD,
            file.replace("LRTP", "TP")
        )
