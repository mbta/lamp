import logging
import pytest

from lamp_py.runtime_utils.process_logger import ProcessLogger


def test_unstarted_log(caplog: pytest.LogCaptureFixture) -> None:
    "It logs unraised validation errors with the correct type and message."

    process_logger = ProcessLogger("test_unstarted_log")
    process_logger.add_metadata(foo="bar")
    process_logger.log_failure(Exception("test"))
    process_logger.log_complete()

    with caplog.at_level(logging.INFO):
        assert "status=complete" in caplog.text


def test_unraised_exception(caplog: pytest.LogCaptureFixture) -> None:
    "It doesn't output `NoneType: None` when the exception has no traceback."

    process_logger = ProcessLogger("test_not_none")
    process_logger.log_start()

    exception = Exception("foo")

    process_logger.log_failure(Exception(exception))

    assert not exception.__traceback__
    assert "NoneType: None" not in caplog.text.splitlines()

def test_start_logging_explicitly(caplog: pytest.LogCaptureFixture) -> None:
    "It doesn't start the log when it initializes."

    ProcessLogger("test_not_none", foo="bar")

    assert caplog.text == ""
