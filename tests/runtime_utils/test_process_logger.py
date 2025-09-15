import logging
import dataframely as dy
import pytest

from lamp_py.runtime_utils.process_logger import ProcessLogger

@pytest.mark.parametrize("exception_message", [("comma,separated,list")])
def test_failing_validation(exception_message: str, caplog: pytest.LogCaptureFixture) -> None:
    "It logs unraised validation errors with the correct type and message."

    process_logger = ProcessLogger("test_failing_validation")
    process_logger.log_failure(dy.exc.ValidationError(exception_message))
    process_logger.log_complete()

    with caplog.at_level(logging.ERROR):
        assert exception_message in caplog.text
        assert type(dy.exc.ValidationError).__name__ in caplog.text

def test_no_metadata(caplog: pytest.LogCaptureFixture) -> None:
    "It gracefully handles a lack of metadata."

    process_logger = ProcessLogger("test_no_metadata")
    process_logger.log_failure(Exception())

    assert not process_logger.metadata
    assert "NoneType: None" not in caplog.text.splitlines()
