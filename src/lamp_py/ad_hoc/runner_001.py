from lamp_py.runtime_utils.process_logger import ProcessLogger


def runner() -> None:
    """Test ad-hoc runner"""
    logger = ProcessLogger("ad_hoc_runner")
    logger.log_start()
    logger.add_metadata(check_ran=True)
    logger.log_complete()
