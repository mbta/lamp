import logging
import pytest

from lamp_py.runtime_utils.process_logger import ProcessLogger

def test_unstarted_log(caplog: pytest.LogCaptureFixture) -> None:
    "It logs unraised validation errors with the correct type and message."

    process_logger = ProcessLogger("test_unstarted_log")
    process_logger.add_metadata(foo="bar")
    process_logger.log_failure(Exception("test"))
    process_logger.log_complete()

    with caplog.at_level(logging.ERROR):
        assert type(Exception).__name__ in caplog.text

def test_no_metadata(caplog: pytest.LogCaptureFixture) -> None:
    "It gracefully handles a lack of metadata."

    process_logger = ProcessLogger("test_no_metadata")
    process_logger.log_start()
    process_logger.log_failure(Exception())

    assert not process_logger.metadata
    assert "NoneType: None" not in caplog.text.splitlines()
