import os

from ingest import parse_args


def test_argparse() -> None:
    """
    Test that the custom argparse is working as expected
    """
    filepath = "my/fake/path.json.gz"

    export_bucket = "fake_dataplatform_springboard"
    error_bucket = "fake_dataplatform_error"
    archive_bucket = "fake_dataplatform_archive"

    arguments = [
        "--input",
        filepath,
        "--export",
        export_bucket,
        "--archive",
        archive_bucket,
        "--error",
        error_bucket,
    ]

    event = parse_args(arguments)

    # check that argparse creates a single event correctly
    assert isinstance(event, dict)
    assert len(event["files"]) == 1
    assert event["files"][0] == filepath

    # check that argparse assigns env vars correctly
    assert os.environ["EXPORT_BUCKET"] == export_bucket
    assert os.environ["ARCHIVE_BUCKET"] == archive_bucket
    assert os.environ["ERROR_BUCKET"] == error_bucket
